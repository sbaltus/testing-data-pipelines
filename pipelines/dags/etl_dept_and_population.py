from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines import DPT_DATA_SOURCES
from pipelines.tasks.http import download_and_store_to_s3

with DAG(
    "get_department_and_population",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["one_shot"],
) as dag:

    store_department_on_s3 = PythonOperator(
        task_id="departments_inventory",
        python_callable=download_and_store_to_s3,
        op_args=[DPT_DATA_SOURCES["departments"], "{{ ds_nodash }}/departments.csv"],
    )

    store_population_on_s3 = PythonOperator(
        task_id="population_by_dpt",
        python_callable=download_and_store_to_s3,
        op_args=[
            DPT_DATA_SOURCES["population_by_dpt"],
            "{{ ds_nodash }}/population.csv",
        ],
    )
