from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import pandas as pd
from sqlalchemy import create_engine

default_arguments = {
    'owner': 'Abrham',
    'email': 'abrhamaddis32@gmail.com',
    'start_date': datetime(2024, 10, 16)
}

def load_csv_files(directory_path):
    # Load the CSV file into a pandas DataFrame
    raw_data_pd = pd.read_csv(directory_path)
    return raw_data_pd

def create_conn(username, password):
    engine = None
    try:
        # Create an engine that connects to PostgreSQL server
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/data_ware_house')
        print("Connection successful")
    except Exception as error:
        print(f"Error: {error}")

    return engine

def load_data_to_db(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data loaded to {table_name} successfully.")
    except Exception as error:
        print(error)

with DAG('etl_workflow', default_args=default_arguments) as etl_dag:
    # Python operator that loads data from a CSV file to pandas DataFrame
    load_row_task = PythonOperator(
        task_id='load_row',
        python_callable=load_csv_files,
        op_kwargs={'directory_path': '../data/track_information.csv'}
    )

    create_connection_task = PythonOperator(
        task_id='create_connection_database',
        python_callable=create_conn,
        op_kwargs={
            'username': 'postgres',
            'password': 'postgre'               
        }
    )

    # Set task dependencies
    load_row_task >> create_connection_task
