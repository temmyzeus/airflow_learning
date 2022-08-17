"""Checks which container runs the Airflow code, when done check logs for the docker container name output,
then dokcer ps to match against all running containers..It will be the Scheduler container.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "temmyzeus",
    "email": ["awoyeletemiloluwa@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id="Check-Docker-Code-Runner",
    default_args=default_args,
    description="Learning Airflow Hooks",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["Learning", "Redundant"]
) as dag:
    pg_to_s3 = BashOperator(
        task_id="check-where-code-runs",
        bash_command="echo Name of the container is: && cat /etc/hostname"
    ) 
