import logging
import os
import shutil
from datetime import datetime
from json import JSONDecodeError

from google.cloud import storage, bigquery

import pandas as pd
from typing import Any, Dict, List, Callable, Optional

import requests

BUCKET_NAME = os.environ['BUCKET_NAME']
DATA_SET = os.environ.get('DATA_SET', 'test_prom')
START: int = int(datetime.fromisoformat(os.environ['START']).timestamp())
END: int = int(datetime.fromisoformat(os.environ['END']).timestamp())

HOST = os.environ.get('HOST', 'localhost')
PORT = os.environ.get('PORT', '30090')
URL = f'http://{HOST}:{PORT}/api/v1/query'

DEBUG: bool = bool(os.environ.get('DEBUG', False))

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='[%(asctime)s] %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


class PromMetric:
    table_id: str
    promql: str
    columns: Dict[str, str]
    tmp_file: str
    gcs_file: str
    add_custom_columns: Optional[Callable[[Any, Any], None]]

    def __init__(self, table_name: str, promql: str, columns: Dict[str, str], is_info=False):
        self.table_id = f'{DATA_SET}.{table_name}'
        self.promql = promql
        self.columns = columns
        self.tmp_file = f'var/{self.table_id}.csv'
        self.gcs_file = f'prom_upload/{self.table_id}.csv'
        self.gcs_path = f'gs://{BUCKET_NAME}/{self.gcs_file}'
        self.is_info = is_info
        
    def _query(self, time: int) -> Any:
        params = {
            'query': self.promql,
            'time': str(time)
        }
        response = requests.get(URL, params=params)
        try:
            decoded = response.json()
        except JSONDecodeError as e:
            logger.error(response.text())
            logger.error(e)
        return decoded

    def _convert2df(self, res_json: Any) -> Optional[pd.DataFrame]:
        try:
            result_list = res_json['data']['result']
        except KeyError as e:
            logger.error(res_json)
            raise e
        logger.debug(f"len: {len(result_list)}")
        if len(result_list) == 0:
            return None

        values = {}
        for key in result_list[0]['metric'].keys():
            column_name = self.columns.get(key, key)
            values[column_name] = [res['metric'].get(key, None) for res in result_list]
        if not self.is_info:
            values['timestamp'] = [res['value'][0] for res in result_list]
            values['value'] = [res['value'][1] for res in result_list]
        if hasattr(self, 'add_custom_columns'):
            self.add_custom_columns(result_list, values)
        return pd.DataFrame(values)

    def dump(self, time: int):
        res_json = self._query(time)
        df = self._convert2df(res_json)
        if df is None:
            logger.debug(f"{self}: No data. Skipped.")
            return
        logger.debug(self)

        if not os.path.exists(self.tmp_file):
            df.to_csv(self.tmp_file, index=False)
        elif self.is_info:
            df.to_csv(self.tmp_file, index=False)
        else:
            df.to_csv(self.tmp_file, index=False, header=False, mode='a')

    def upload(self):
        if not os.path.exists(self.tmp_file):
            logger.info(f"{self.table_id}: No records. Skipped.")
            return
        self._upload_to_gcs()
        self._bq_load()

    def _upload_to_gcs(self):
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(self.gcs_file)
        blob.upload_from_filename(self.tmp_file)

    def _bq_load(self):
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE" if self.is_info else "WRITE_APPEND"
        )
        job_config.autodetect = True
        load_job = client.load_table_from_uri(
            self.gcs_path, self.table_id, job_config=job_config
        )
        load_job.result()  # Waits for table load to complete.
        logger.info(f"{self.table_id}: Job finished.")

        destination_table = client.get_table(self.table_id)
        logger.info(f"{self.table_id}: Loaded {destination_table.num_rows} rows.")

    def __str__(self):
        return f"{self.table_id}: {self.promql}"


class PromExportRunner:
    prom_metric_list: List[PromMetric]

    def __init__(self, step: int):
        self.step = step
        self.prom_metric_list = []

    def add(self, prom_metric: PromMetric):
        self.prom_metric_list.append(prom_metric)

    def _dump(self, time: int):
        for m in self.prom_metric_list:
            m.dump(time)

    def _bq_load(self):
        for m in self.prom_metric_list:
            m.upload()

    def _rm_tmp_files(self):
        shutil.rmtree('var')
        os.mkdir('var')

    def run(self):
        self._rm_tmp_files()
        time = START
        while time < END:
            logger.info(datetime.fromtimestamp(time))
            self._dump(time)
            time = time + self.step
        self._bq_load()
