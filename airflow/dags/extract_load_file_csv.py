from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def extract_load():
    df = pd.read_csv('data/superstore.csv',encoding='ISO-8859-1')
    engine = create_engine('postgresql://quang:q@postgres:5432/superstore')
    df.to_sql('raw_orders', engine, index=False, if_exists='replace')

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='extract_load_csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:
    
    extract_load_task = PythonOperator(
        task_id='extract_and_load_csv',
        python_callable=extract_load,
    )