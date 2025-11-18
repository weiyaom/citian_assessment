import os
import io
import logging
import requests
from datetime import datetime
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------------------------------------------------
# 🔧 CONFIG
# -----------------------------------------------------------------------
DATA_DIR = "/opt/airflow/data"   # Airflow worker volume
TABLE_NAME = "yellow_tripdata_raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

os.makedirs(DATA_DIR, exist_ok=True)


# -----------------------------------------------------------------------
# 1️⃣ DOWNLOAD ONE MONTH'S PARQUET (STREAMING, SAFE FOR LARGE FILES)
# -----------------------------------------------------------------------
def download_monthly_file(execution_date, **context):
    """
    Dynamically builds the filename based on Airflow's execution_date.
    Downloads the large parquet file (500MB–1GB) using streaming.
    """
    year = execution_date.year
    month = f"{execution_date.month:02d}"
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{filename}"

    local_path = os.path.join(DATA_DIR, filename)

    logging.info(f"📥 Downloading from: {url}")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to download {url} — Status {response.status_code}")

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    logging.info(f"✅ Downloaded file saved to {local_path}")
    return local_path


# -----------------------------------------------------------------------
# 2️⃣ DATA QUALITY CHECKS USING PANDAS
# -----------------------------------------------------------------------
def validate_and_clean(filepath, **context):
    """
    Performs required validation:
      - trip_distance > 0
      - passenger_count >= 1
      - Remove null timestamps
    Returns cleaned CSV buffer for COPY ingestion.
    """
    logging.info(f"🔍 Loading Parquet: {filepath}")
    df = pd.read_parquet(filepath)
    original_rows = len(df)

    # ---- Data Cleaning Rules ----
    df = df[df["trip_distance"] > 0]
    df = df[df["passenger_count"] >= 1]

    df = df[df["fare_amount"] >= 0]
    df = df[df["total_amount"] >= 0]

    df = df[df["tpep_pickup_datetime"].notna()]
    df = df[df["tpep_dropoff_datetime"].notna()]

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    cleaned_rows = len(df)
    logging.info(f"🧹 Cleaned {original_rows} → {cleaned_rows}")

    # Convert DF → CSV buffer (COPY FROM requires CSV-like object)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Store buffer in XCom for next task
    context["ti"].xcom_push(key="clean_buffer", value=buffer.getvalue())
    context["ti"].xcom_push(key="cols", value=list(df.columns))


# -----------------------------------------------------------------------
# 3️⃣ INGEST CLEANED DATA INTO POSTGRES USING COPY (VERY FAST)
# -----------------------------------------------------------------------
def load_into_postgres(**context):
    """
    Creates table if not exists (dynamic columns).
    COPY FROM the cleaned buffer for extremely fast ingestion.
    """
    clean_csv = context["ti"].xcom_pull(key="clean_buffer")
    columns = context["ti"].xcom_pull(key="cols")

    conn = psycopg2.connect(
        dbname="testdb",
        user="miya",
        password="miya123",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    # ---- Create table dynamically (all TEXT for raw zone) ----
    col_types = ", ".join([f'"{c}" TEXT' for c in columns])
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {col_types}
        );
    """)
    conn.commit()

    # ---- COPY FROM CSV buffer ----
    logging.info(f"📤 Copying data into Postgres table: {TABLE_NAME}")
    buffer = io.StringIO(clean_csv)
    cur.copy_from(buffer, TABLE_NAME, sep=",")

    conn.commit()
    cur.close()
    conn.close()
    logging.info("✅ Ingestion completed")


# -----------------------------------------------------------------------
# AIRFLOW DAG
# -----------------------------------------------------------------------
with DAG(
    dag_id="nyc_yellow_taxi_ingestion",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@monthly",     # Incremental per month
    catchup=True,                     # Airflow will run Jan-Dec for 2022 automatically
    max_active_runs=1,                # prevent concurrency issues
) as dag:

    task_download = PythonOperator(
        task_id="download_month",
        python_callable=download_monthly_file,
    )

    task_clean = PythonOperator(
        task_id="clean_data",
        python_callable=validate_and_clean,
    )

    task_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_into_postgres,
    )

    # Task dependency
    task_download >> task_clean >> task_load
