import os
import sqlite3
from datetime import datetime, timedelta
from textwrap import dedent
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


home_dir = os.path.expanduser("~")
raw_data_dir = os.path.join(home_dir, "data", "raw_data")
staging_data_dir = os.path.join(home_dir, "data", "staging")

def transform_data():
    bookings_df = pd.read_csv(
        os.path.join(raw_data_dir, "booking.csv"), low_memory=False
    )
    client_df = pd.read_csv(os.path.join(raw_data_dir, "client.csv"), low_memory=False)
    hotel_df = pd.read_csv(os.path.join(raw_data_dir, "hotel.csv"), low_memory=False)

    bookings_df.booking_date = pd.to_datetime(
        bookings_df.booking_date, infer_datetime_format=True
    )
    bookings_df.currency = "GBP"

    client_df = client_df.rename(
        columns={"age": "client_age", "name": "client_name", "type": "client_type"}
    )
    hotel_df = hotel_df.drop(columns=["address"])
    hotel_df = hotel_df.rename(
        columns={"name": "hotel_name", "address": "hotel_address"}
    )

    data = pd.merge(
        left=bookings_df, right=client_df, on="client_id", how="inner"
    ).merge(hotel_df, how="inner", on="hotel_id")

    data.to_csv(os.path.join(staging_data_dir, "hotel_bookings.csv"), index=False)


def load_data():
    table_name:str = "hotel_bookings"
    conn = sqlite3.connect("/home/airflow/databases/hotel_bookings.db")
    cursor = conn.cursor()

    data = pd.read_csv(os.path.join(staging_data_dir, "hotel_bookings.csv"), low_memory=False)

    schema = pd.io.sql.get_schema(data, name=table_name, con=conn)
    schema = schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    cursor.execute(schema)

    data.to_sql(name=table_name, con=conn, if_exists="replace", index=False)
    cursor.close()
    conn.close()


# with DAG()  as ingestion_dag:

default_args: dict = {"owner": "temmyzeus", "email": ["awoyeletemiloluwa@gmail.com"]}

ingestion_dag = DAG(
    "Booking-Ingestion",
    description="Hotel bookings data pipeline",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval="@once",
    catchup=False,
    tags=["Ingest data from source"],
)

transform_data = PythonOperator(
    task_id="Transform-Data", python_callable=transform_data, dag=ingestion_dag
)

load_data = PythonOperator(
    task_id="Load-Data", python_callable=load_data, dag=ingestion_dag
)

transform_data >> load_data
