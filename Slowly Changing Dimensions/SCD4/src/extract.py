import pandas as pd

def get_target_table(engine, table_name):
    return pd.read_sql_query(f'SELECT * FROM {table_name}', engine)

def get_source_table(engine, path):
    return pd.read_csv(path)

