import os 
from airflow import DAG
from datetime import datetime, timedelta
from zhelper_functions.web_utils import FilePathManager
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depands_on_past': False,
    'email_on_failure': True,
    'email': ['alimurad7777@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG (
    'website_docs_crawler',
    default_args=default_args,
    descrption='A simple website crawler',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 26)
)

