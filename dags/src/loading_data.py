from sqlalchemy import create_engine
from extract_data import extract_data

def create_conn():
    engine = None
    try:
        engine = create_engine('postgresql://postgres:new_password@localhost:5432/warehouse')
        print("Connection successful")
    except Exception as error:
        print(error)

    return engine

def load_data_to_db(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data loaded to {table_name} successfully.")
    except Exception as error:
        print(error)