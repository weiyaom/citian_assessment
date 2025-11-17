import os
from datetime import datetime
import logging
import pandas as pd
import psycopg2
# from airflow.decorators import sensor_task
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.sensors.filesystem import FileSensor
# from tenacity import retry

PATH = "/Users/miyama/Documents/citian_oa/citian_assessment/data"
POSTGRES_CONN_ID = 'postgres_default'
TABLE_NAME = "yellow_tripdata"


def ingest_table_to_postgres(filepath: str):
    logging.info(f"Starting ingestion for file: {filepath}")

    df = pd.read_parquet(filepath)
    df = df.head(100)

    engine = create_engine(
        "postgresql+psycopg2://miya:miya123@localhost:5432/testdb",
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )

    logging.info("Engine Connected")

    df.to_sql(
        "yellow_tripdata",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000  # 必须加
    )

    logging.info("Ingestion complete")


with DAG(
    dag_id="test_tlc_ppl",
    start_date = datetime(2025,11,16),
    schedule_interval='0 1 * * *', #daily
    catchup=False,
) as dag:
    # t1 = FileSensor(task_id="wait_for_file", filepath=PATH)
    create_and_ingest = PythonOperator(
        task_id='create_and_ingest',
        python_callable=lambda: [ingest_table_to_postgres(f"{PATH}/{f}") for f in os.listdir(PATH)]
    )
