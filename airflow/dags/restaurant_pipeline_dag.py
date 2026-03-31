from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

sys.path.insert(0, '/opt/airflow/project')

from ingestion.extractors.google_extractor import extract_restaurants
from ingestion.loaders.snowflake_loader import load_restaurants, load_photos
from ingestion.loaders.cdc_handler import run_cdc
default_args = {
    'owner': 'mawvir',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG('restaurant_pipeline',
    default_args=default_args,
    description='RestaurantPulse SD — full pipeline',
    schedule='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

def extract_and_load(**context):
    restaurants = extract_restaurants()
    print(f"Extracted {len(restaurants)} restaurants")
    load_restaurants(restaurants)
    load_photos(restaurants)
    context['ti'].xcom_push(key='restaurants', value=restaurants)
    print("Raw data loaded into snowflake")

def cdc_task(**context):
    restaurants = context['ti'].xcom_pull(task_ids='extract_and_load', key='restaurants')

    run_cdc(restaurants)

task_extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=extract_and_load,
    dag=dag
)

task_cdc = PythonOperator(
    task_id='cdc_task',
    python_callable=cdc_task,
    dag=dag
)

task_dbt_run = BashOperator(
    task_id='run_dbt',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt',
    dag=dag
)

task_dbt_test = BashOperator(
    task_id='test_dbt',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt',
    dag=dag
)

task_extract_and_load >> task_cdc >> task_dbt_run >> task_dbt_test
