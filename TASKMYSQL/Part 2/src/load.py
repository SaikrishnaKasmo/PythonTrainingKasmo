import pandas as pd

def load_data(df1, engine, target_table):
    if df1 is None:
        raise ValueError("DataFrame is None, check your transformation function!")
    return df1.to_sql(name=target_table, con=engine, index=False, if_exists='replace')