# Airflow

## Trigger DAG

手动触发DAG运行。

```bash
curl -X POST http://localhost:8080/api/v1/dags/process_orders/dagRuns \
    --user "airflow:airflow" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    -d '{
  "execution_date": "2020-01-01T19:00:00Z",
  "conf": {}
}'
```

注意：`execution_date` 不可重复，并且应该大于DAG的 `start_date`。

## Modules

```python
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
```

## BashSensor

```python
check_file_existence = BashSensor(
    task_id="check_file_existence",
    bash_command="""
        ls /data/raw/file_{{ ds }}.csv
    """,
)
```

## Execute SQL with PostgresOperator

```python
create_stg_products_table = PostgresOperator(
    task_id="******unique_task_id******",
    postgres_conn_id=connection_id,
    sql="""
        ***************sql***************
        """,
)
```

## Normalize CSV with PythonOperator

Replace CSV delimiter from `,` to `\t`.

```python
def normalize_csv(ds, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:  # skip header
                    header_skipped = True
                    continue
                row.append(ds)
                writer.writerow(row)
    return target_filename

normalize_products_csv = PythonOperator(
    task_id='normalize_products_csv',
    python_callable=normalize_csv,  # normalize_csv is the method name
    op_kwargs={
        'source': "/data/raw/products_{{ ds }}.csv",
        'target': "/data/staging/products_{{ ds }}.csv"
    }
)
```

## Load data into PostgreSQL with PostgresHook

```python
load_products_to_stg_products_table = PythonOperator(
    task_id='load_products_to_stg_products_table',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'csv_filepath': "/data/staging/products_{{ ds }}.csv",
        'table_name': 'stg_products'
    },
)

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name
```
