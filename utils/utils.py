import pandas as pd
from sqlalchemy import create_engine

def process_data(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
    cols = lines_as_lists.pop(0)
    track_cols = cols[:4]
    trajectory_cols = ['track_id'] + cols[4:]

    track_info = []
    trajectory_info = []

    for row in lines_as_lists:
        track_id = row[0]

        # add the first 4 values to track_info
        track_info.append(row[:4]) 

        remaining_values = row[4:]
        # reshape the list into a matrix and add track_id
        trajectory_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
        # add the matrix rows to trajectory_info
        trajectory_info = trajectory_info + trajectory_matrix

    df_track = pd.DataFrame(data=track_info, columns=track_cols)
    df_track.columns = df_track.columns.str.strip()

    df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)
    df_trajectory.columns = df_trajectory.columns.str.strip()

    return df_track, df_trajectory

"""
a function that connect to the local database
"""
def create_conn():
    engine = None
    try:
        # Create an engine that connects to PostgreSQL server
        engine = create_engine('postgresql://postgres:new_password@localhost:5432/data_ware_house')
        print("Connection successful")
    except Exception as error:
        print(error)

    return engine

"""
a function that that accept engine, and table_name as an argument and return pandas data fream
"""
def fetch_data(engine, table_name):
    df = None
    try:
        # Execute a query and fetch all the rows into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", engine)
    except Exception as error:
        print(error)

    return df

def load_data_to_db(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data loaded to {table_name} successfully.")
    except Exception as error:
        print(error)