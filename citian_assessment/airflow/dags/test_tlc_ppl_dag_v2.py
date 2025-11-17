import io
import os
from datetime import datetime
import logging
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

PATH = "/Users/miyama/Documents/citian_oa/citian_assessment/data"
TABLE_NAME = "yellow_tripdata"

# ----------------------------
# Helper function: COPY FROM
# ----------------------------
def fast_copy_parquet_to_postgres(filepath: str, table_name: str):
    logging.info(f"Starting ingestion for file: {filepath}")

    # 1. 读 Parquet
    df = pd.read_parquet(filepath)
    # df = df.head(20000)
    logging.info(f"Loaded DataFrame shape: {df.shape}")

    # 2. 建立 Postgres 连接
    conn = psycopg2.connect(
        dbname="testdb",
        user="miya",
        password="miya123",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 建表（根据 DataFrame 列名和类型）
    cols_with_types = ", ".join([f"{col} TEXT" for col in df.columns])  # 全部用 TEXT 简单处理
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_with_types});")
    conn.commit()

    # 3. pandas → CSV buffer（内存） → COPY FROM
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # 4. COPY 到 Postgres
    cur.copy_from(buffer, table_name, sep=",")
    conn.commit()

    cur.close()
    conn.close()

    logging.info(f"Ingestion complete for file: {filepath}")


# ----------------------------
# Airflow DAG
# ----------------------------
with DAG(
    dag_id="test_tlc_ppl_v2",
    start_date=datetime(2025, 11, 16),
    schedule_interval='0 1 * * *',  # daily
    catchup=False,
) as dag:

    def ingest_all_files():
        for f in os.listdir(PATH):
            if f.endswith(".parquet"):
                fast_copy_parquet_to_postgres(os.path.join(PATH, f), TABLE_NAME)

    create_and_ingest = PythonOperator(
        task_id='create_and_ingest',
        python_callable=ingest_all_files
    )
