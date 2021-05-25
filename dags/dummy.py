import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

default_args = {"owner": "airflow"}

with DAG(
    dag_id="dummy",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_csv_readiness = BashSensor(
        task_id="check_csv_readiness",
        bash_command="""
            ls /data/raw/csv_{{ ds }}.csv
        """,
    )

    normalize_csv = BashOperator(
        task_id='normainze_csv',
        bash_command='echo normalize_csv',
    )

    load_csv_to_staging_table = BashOperator(
        task_id='load_csv_to_staging_table',
        bash_command='echo load_csv_to_staging_table',
    )

    build_model = BashOperator(
        task_id='build_model',
        bash_command='echo build_model',
    )

    check_csv_readiness >> normalize_csv >> load_csv_to_staging_table >> build_model
