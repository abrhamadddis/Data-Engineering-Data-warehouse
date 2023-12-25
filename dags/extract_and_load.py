from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract_data import extract_data
from src.loading_data import create_conn, load_data_to_db

default_args = {
    'owner':'abrham',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}
def extract_task():
    df_track, df_trajectory = extract_data("/home/abrham/Desktop/10x/week - 2/Week-2-Data-Engineering-Data-warehouse/data/data.csv")
    return df_track, df_trajectory

def load_data_task():
    df_track, df_trajectory= extract_task()
    engine = create_conn()
    load_data_to_db(df_track, "track", engine)
    load_data_to_db(df_trajectory, "trajectory", engine)


with DAG(
    dag_id= 'extract and load the row data',
    default_args=default_args,
    description='dag with python operator',
    start_date=datetime(2023, 12, 22, 2),
    schedule_interval='@daily',
)as dag:
    extract_operator = PythonOperator(
        task_id= 'extract_task',
        python_callable=extract_task,
        dag=dag,

    )
    load_operator = PythonOperator(
        task_id='load_task',
        python_callable='load_task',
        dag=dag,
    )
    
    
extract_operator >> load_operator
