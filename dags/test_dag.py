from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'abrham',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}
def greet():
    print("hello world")

with DAG(
    dag_id= 'our_first_dag_with_python_operator_v4',
    default_args=default_args,
    description='dag with python operator',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily',
)as dag:
    task1 = PythonOperator(
        task_id= 'greet',
        python_callable=greet
    )
    task1