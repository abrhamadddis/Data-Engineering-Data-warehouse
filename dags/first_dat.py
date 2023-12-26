from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'abrham',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dbt_dag',
    default_args=default_args,
    description='A DAG to run dbt commands',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --profiles-dir ../dbt_warehouse'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir ../dbt_warehouse'
    )

    dbt_run >> dbt_test