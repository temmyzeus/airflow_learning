import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

home_dir = os.path.expanduser("~")
raw_data_dir = os.path.join(home_dir, "raw_data")

def extract_data():
    bookings_df = pd.read_csv(os.path.join(raw_data_dir, "booking.csv"), low_memory=False)
    client_df = pd.read_csv(os.path.join(raw_data_dir, "client.csv"), low_memory=False)
    hotel_df = pd.read_csv(os.path.join(raw_data_dir, "hotel.csv"), low_memory=False)

    print("Booking Data Shape:", bookings_df.shape)
    print("Client Data Shape:", client_df.shape)
    print("Hotel Data Shape:", hotel_df.shape)

    return bookings_df, client_df, hotel_df

def extract_data1():
    print("Hello World")

def extract_data2():
    print("Hello World")

# with DAG()  as ingestion_dag:

default_args: dict = {"owner": "temmyzeus", "email": ["awoyeletemiloluwa@gmail.com"]}

ingestion_dag = DAG(
    "Booking-Ingestion",
    description="Hotel bookings data pipeline",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval="@daily",
    catchup=False,
    tags=["Ingest data from source"],
)

extract_data = PythonOperator(
    task_id="Extract-Data", python_callable=extract_data, dag=ingestion_dag
)

transform_data = PythonOperator(
    task_id="Transform-Data", python_callable=extract_data1, dag=ingestion_dag
)

load_data = PythonOperator(
    task_id="Load-Data", python_callable=extract_data2, dag=ingestion_dag
)

extract_data >> transform_data >> load_data
