from airflow import DAG
from datetime import datetime,timedelta
import sys
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'description': 'A dag to orchestrator data',
    'start_date' : datetime(2025,7,20),
}

dag = DAG(
    dag_id = 'dbt_data_orchestator',
    default_args = default_args,
    schedule=timedelta(minutes=10), 
    catchup=False 
)

with dag:
    run_dbt = DockerOperator(
        task_id='run_dbt',
        image='ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        command=['run', '--profiles-dir', '/root/.dbt'],
        working_dir='/usr/app', 
        docker_url='unix://var/run/docker.sock',
        network_mode='superstore_project_my-networks',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
        Mount(source='/d/superstore_project/dbt/superstore_dbt', target='/usr/app', type='bind'),
        Mount(source='/d/superstore_project/dbt/profiles.yml', target='/root/.dbt/profiles.yml', type='bind'),
        ],
        dag=dag,
        do_xcom_push=False
    )