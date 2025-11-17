from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
from sqlalchemy import create_engine

def etl_task():
    df = pd.read_parquet("/Users/miyama/Documents/citian_oa/citian_assessment/data/yellow_tripdata_2025-01.parquet")

    df = df[df["trip_distance"] > 0]
    df = df[df["passenger_count"] > 0]

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60
    )

    engine = create_engine("postgresql+psycopg2://miya:miya123@localhost:5432/testdb")
    df = df.head(100)
    df.to_sql("yellow_trips_clean", engine, if_exists="replace", index=False)

with DAG(
    dag_id="citian_yellow_taxi_etl",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl_task,
    )
