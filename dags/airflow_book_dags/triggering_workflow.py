from airflow import DAG
from airflow.models.dag import DagOwnerAttributes
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="Trigerring-Workflow",
    description="Chaper 6 in the Airflow Book",
    catchup=False
) as dag:
    copy_to_raw_supermarket_1 = DummyOperator(
        task_id="copy_to_raw_supermarket_1"
    )

    copy_to_raw_supermarket_2 = DummyOperator(
        task_id="copy_to_raw_supermarket_2"
    )

    copy_to_raw_supermarket_3 = DummyOperator(
        task_id="copy_to_raw_supermarket_3"
    )

    copy_to_raw_supermarket_4 = DummyOperator(
        task_id="copy_to_raw_supermarket_4"
    )

    process_supermarket_1 = DummyOperator(
        task_id="process_supermarket_1"
    )

    process_supermarket_2 = DummyOperator(
        task_id="process_supermarket_2"
    )

    process_supermarket_3 = DummyOperator(
        task_id="process_supermarket_3"
    )

    process_supermarket_4 = DummyOperator(
        task_id="process_supermarket_4"
    )
