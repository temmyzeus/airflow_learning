import json
from pathlib import Path
from typing import Union

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args: dict = {
    "email": "awoyeletemiloluwa@gmail.com"
}

rockets_dag = DAG(
    dag_id="download-rocket-launches-dag",
    description="Fetch data about rocket launches and saves it",
    default_args=default_args,
    start_date=days_ago(14), # 14 days ago
    schedule_interval=None
)

download_launches = BashOperator(
    task_id="download-launches",
    bash_command="curl -o /home/airflow/tmp/launches.json -L https://ll.thespacedevs.com/2.2.0/launch/upcoming",
    dag=rockets_dag
)

string_path_type = Union[str, Path]

def _get_pictures(
    file_dir:string_path_type = "/home/airflow/tmp/launches.json",
    save_to:string_path_type = "/home/airflow/tmp/images"
    ) -> None:
    save_to = Path(save_to)
    save_to.mkdir(parents=True, exist_ok=True)
    with open(file_dir, mode="r") as f:
        launches = json.load(f)

    image_urls = [launch["image"] for launch in launches["results"]]
    for image_url in image_urls:
        try:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = save_to / image_filename
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url} to {target_file}")
        except requests_exceptions.MissingSchema:
            print(f"{image_url} appears to be an invalid URL.")
        except requests_exceptions.ConnectionError:
            print(f"Could not connect to {image_url}.")

get_images = PythonOperator(
    task_id="fetch-images",
    python_callable=_get_pictures,
    dag=rockets_dag
)

download_launches >> get_images
