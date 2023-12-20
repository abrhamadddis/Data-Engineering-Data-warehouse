from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'abrham',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id= 'our_first_dag_v4',
    default_args=default_args,
    description='this is our first dag that we write',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily',
)as dag:
    task1 = BashOperator(
        task_id= 'first_task',
        bash_command= 'echo hello world, this is the first task!'
    )

    task2 = BashOperator(
        task_id = 'second_id',
        bash_command='echo hey, I am task2 and will be runing after task 1'
    )

    task3 = BashOperator(
        task_id = 'tird_id',
        bash_command='echo hey, I am task3 and will be runing after task 2'
    )

    task1 >> task2 >> task3