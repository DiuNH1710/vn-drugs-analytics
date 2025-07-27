# airflow/dags/geocode_dag.py

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


from geocoder.geocode_main import run_geocoding_job 


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='pharma_geocode_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['geocoding', 'pharmaceutical']
) as dag:

    geocode_task = PythonOperator(
        task_id='run_geocoding',
        python_callable=run_geocoding_job
    )

    geocode_task
