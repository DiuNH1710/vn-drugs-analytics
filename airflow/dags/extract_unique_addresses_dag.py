# airflow/dags/extract_unique_addresses_dag.py

import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/dags/geocoder')

def safe_extract_callable():
    from geocoder.extract_unique_addresses import extract_unique_addresses
    return extract_unique_addresses()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 27),
    'catchup': False,
}

with DAG(
    dag_id='extract-unique-addresses-dag',
    default_args=default_args,
    schedule=None,
    tags=['geocode', 'extract'],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_unique_addresses_task',
        python_callable=safe_extract_callable,
    )
