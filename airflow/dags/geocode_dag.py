# airflow/dags/geocode_dag.py

import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/dags/geocoder')

def safe_extract_callable():
    from geocoder.extract_unique_addresses import extract_unique_addresses
    return extract_unique_addresses()

def safe_geocode_callable():
    from geocoder.geocoder_address import geocode_unique_addresses
    return geocode_unique_addresses()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 14),
    'catchup': False,
}

dag = DAG(
    dag_id='geocode-dag',
    default_args=default_args,
    schedule=None,
    tags=['geocode', 'address', 'dedup'],
)
with dag: 
    extract_task  = PythonOperator(
        task_id='extract_unique_addresses_task',
        python_callable=safe_extract_callable,
    )
    geocode_task = PythonOperator(
        task_id='geocode_unique_addresses_task',
        python_callable=safe_geocode_callable,
    )

extract_task >> geocode_task