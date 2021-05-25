# Airflow


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

## PostgresOperator

```python
create_stg_products_table = PostgresOperator(
    task_id="create_stg_products_table",
    postgres_conn_id=connection_id,
    sql="""
        CREATE TABLE IF NOT EXISTS STG_PRODUCTS (
            id VARCHAR NOT NULL UNIQUE,
            title VARCHAR,
            category VARCHAR,
            price DECIMAL,
            processed_date DATE
        );

        truncate STG_PRODUCTS;
        """,
)
```

## Normalize CSV

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
```


## PythonOperator

```python
normalize_products_csv = PythonOperator(
    task_id='normalize_products_csv',
    python_callable=normalize_csv,
    op_kwargs={
        'source': "/data/raw/products_{{ ds }}.csv",
        'target': "/data/staging/products_{{ ds }}.csv"
    }
)
```

## PostgresHook

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