import io
import requests
import os
from datetime import datetime
import logging
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

PATH = "/Users/miyama/Documents/citian_oa/citian_assessment/data"
# BASE_URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
TABLE_NAME = "yellow_trip_data_2022"

def fast_copy_parquet_to_postgres(filepath: str , table_name: str):
    # TODO: use API to download trip data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    # response = request.get()
    logging.info(f"Starting ingestion for file: {filepath}")
    df = pd.read_parquet(filepath)
    logging.info(f"Loaded DataFrame shape: {df.shape}")

    # TODO: data quality check using pandas
    df = df[df["trip_distance"] > 0]
    df = df[df["passenger_count"] > 0]
    logging.info(f"Loaded DataFrame shape: {df.shape}")

    conn = psycopg2.connect(
        dbname="testdb",
        user="miya",
        password="miya123",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    cols_with_types = ", ".join([f"{col} TEXT" for col in df.columns])
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_with_types});")
    conn.commit()

    # 3. pandas → CSV buffer → COPY FROM
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # 4. COPY to Postgres
    cur.copy_from(buffer, table_name, sep=",")
    conn.commit()

    cur.close()
    conn.close()

    logging.info(f"Ingestion complete for file: {filepath}")


# ----------------------------
# Airflow DAG
# ----------------------------
with DAG(
    dag_id="ingest_yellow_taxi_data",
    start_date=datetime(2025, 11, 16),
    schedule_interval='0 1 * * *',  # daily
    catchup=False,
) as dag:

    def ingest_all_files():
        for f in os.listdir(PATH):
            if f.endswith(".parquet"):
                fast_copy_parquet_to_postgres(PATH, TABLE_NAME)

    create_and_ingest = PythonOperator(
        task_id='create_and_ingest',
        python_callable=ingest_all_files
    )
