# airflow/dags/geocode_unique_addresses_dag.py

import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/dags/geocoder')

def safe_geocode_callable():
    from geocoder.geocoder_address import geocode_unique_addresses
    return geocode_unique_addresses()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 27),
    'catchup': False,
}

with DAG(
    dag_id='geocode-unique-addresses-dag',
    default_args=default_args,
    schedule=None,
    tags=['geocode', 'run'],
) as dag:
    geocode_task = PythonOperator(
        task_id='geocode_unique_addresses_task',
        python_callable=safe_geocode_callable,
    )
