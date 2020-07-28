# prometheus-bigquery-exporter
Utility scripts to export promql metric data to bigquery


## Requirements

- python3
- Set target GCP project credential as `GOOGLE_APPLIACATION_CREDENTIALS`


## How to use

- Add promql metric export definition( with PromExportRunner/PromMetric )

```python
from prom_export_runner import PromExportRunner, PromMetric

# Create runner instance for each metric retrieval step
runner_1d = PromExportRunner(step=60 * 60 * 24)
runner_1m = PromExportRunner(step=60 * 60 * 24 * 30)

# Add promql for target metric and table name of bigquery
runner_1d.add(PromMetric(
    "some_table_name",
    "some_promql_query",
    {
         "some_label": "renamed_label"
    },
    is_info=True # Optional, should be true when target metric is info type. 
))
runner_1d.add(...)
...

runner_1m.add(...)
...

runner_1d.run()
runner_1m.run()
```

- Run the python script

```shell script
BUCKET_NAME='sample-bucket' DATA_SET='sample_prom' START='2020-03-01T00:00:00' END='2020-06-30T00:00:00' python main.py
```