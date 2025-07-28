from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 27),
    'catchup': False,
}

with DAG(
    dag_id='trigger_dbt_models',
    default_args=default_args,
    schedule=None,  
    tags=["dbt", "transformation"],
) as dag:

     run_dbt = DockerOperator(
        task_id="run_dbt",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",  # hoặc image dbt bạn dùng
        api_version='auto',
        auto_remove='success',
        command="run",
        docker_url="unix://var/run/docker.sock",  # Docker socket
        network_mode="vn-drugs-analytics_my_network",  # phải cùng network với db, nếu cần
        mounts=[
                  Mount(source="/mnt/e/data-projects/vn-drugs-analytics/dbt/my_project", target="/usr/app", type="bind"),
                  Mount(source="/mnt/e/data-projects/vn-drugs-analytics/dbt/profiles.yml", target="/root/.dbt/profiles.yml", type="bind"),

        ],
    )
     run_dbt
