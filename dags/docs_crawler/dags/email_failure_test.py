from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Fixed typo here
    'email_on_failure': True,
    'email_on_success': True,
    'email': ['alimurad7777@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def always_fail():
    raise ValueError("This task is designed to fail!")

with DAG(
    'Failing_DAG',
    default_args=default_args,
    description='A DAG that intentionally fails',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 26),
    tags=['failure_test'],
    catchup=False
) as dag:

    fail_task = PythonOperator(
        task_id='always_fail_task',
        python_callable=always_fail
    )

    fail_task