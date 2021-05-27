import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner": "airflow"}
connection_id = 'dwh'

with DAG(
    dag_id="create_dim_dates",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    create_dim_dates_table = PostgresOperator(
        task_id="create_dim_dates_table",
        postgres_conn_id=connection_id,
        sql="""
            CREATE TABLE IF NOT EXISTS dim_dates (
                id VARCHAR NOT NULL UNIQUE,
                datum date,
                day_of_month INT,
                day_of_year INT,
                month INT,
                quarter INT,
                year INT,
                first_day_of_month DATE NOT NULL,
                last_day_of_month DATE NOT NULL,
                first_day_of_next_month DATE NOT NULL
            );

            INSERT INTO dim_dates
            SELECT 
                TO_CHAR(datum, 'yyyymmdd')::INT AS id,
                datum as datum,
                EXTRACT(DAY FROM datum) AS day_of_month,
                EXTRACT(DOY FROM datum) AS day_of_year,
                EXTRACT(MONTH FROM datum) AS month,
                EXTRACT(QUARTER FROM datum) AS quarter,
                EXTRACT(YEAR FROM datum) AS year,
                datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
                (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
                (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH')::DATE AS first_day_of_next_month
            FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
                FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
                GROUP BY SEQUENCE.DAY) DQ
            ORDER BY 1
            ON CONFLICT (id) DO NOTHING;
          """,
    )

    create_dim_dates_table