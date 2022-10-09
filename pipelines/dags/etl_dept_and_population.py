from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines import DEFAULT_S3_BUCKET, DPT_DATA_SOURCES
from pipelines.tasks.departments.departement_and_population_merger import (
    DepartmentAndPopulationMerger,
)
from pipelines.tasks.departments.department_processor import DepartmentProcessor
from pipelines.tasks.departments.population_processor import PopulationProcessor
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

    dp = DepartmentProcessor(s3_bucket=DEFAULT_S3_BUCKET)
    pop = PopulationProcessor(s3_bucket=DEFAULT_S3_BUCKET)
    merger = DepartmentAndPopulationMerger(s3_bucket=DEFAULT_S3_BUCKET)
    store_department_on_s3 = PythonOperator(
        task_id="departments_inventory",
        python_callable=download_and_store_to_s3,
        op_args=[DPT_DATA_SOURCES["departments"], "{{ ds_nodash }}/departments.csv"],
        op_kwargs={"s3_bucket": DEFAULT_S3_BUCKET},
    )

    store_population_on_s3 = PythonOperator(
        task_id="population_by_dpt",
        python_callable=download_and_store_to_s3,
        op_args=[
            DPT_DATA_SOURCES["population_by_dpt"],
            "{{ ds_nodash }}/population.csv",
        ],
    )

    enrich_departments = PythonOperator(
        task_id="enrich_departments",
        python_callable=dp.run,
        op_kwargs={
            "source": "{{ task_instance.xcom_pull(task_ids='departments_inventory' }}",
            "processing_date": "{{ ds }}",
        },
    )
    population_per_department = PythonOperator(
        task_id="population_per_department",
        python_callable=pop.run,
        op_kwargs={
            "source": "{{ task_instance.xcom_pull(task_ids='population_by_dpt' }}",
            "processing_date": "{{ ds }}",
        },
    )

    merge_population_and_department = PythonOperator(
        task_id="merge_population_and_department",
        python_callable=merger.run,
        op_kwargs={
            "department_source": "s3://{{ task_instance.xcom_pull(task_ids='enrich_departments' }}[0]/"
            "{{ task_instance.xcom_pull(task_ids='enrich_departments' }}[1]",
            "population_source": "s3://{{ task_instance.xcom_pull(task_ids='population_per_department' }}[0]/"
            "{{ task_instance.xcom_pull(task_ids='latest_population_per_department' }}[1]",
            "processing_date": "{{ ds }}",
        },
    )

    # pylint: disable=pointless-statement
    store_department_on_s3 >> enrich_departments >> merge_population_and_department
    (
        store_population_on_s3
        >> population_per_department
        >> merge_population_and_department
    )
