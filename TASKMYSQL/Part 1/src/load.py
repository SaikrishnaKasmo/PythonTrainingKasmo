import pandas as pd

def load_files(df_list, engine, target_list):
    if len(df_list) != len(target_list):
        raise ValueError("df_list and target_list must have the same length!")

    for df, target_table in zip(df_list, target_list):
        load_data(df, engine, target_table)

def load_data(df1, engine, target_table):
    if df1 is None:
        raise ValueError("DataFrame is None, check your transformation function!")
    return df1.to_sql(name=target_table, con=engine, index=False, if_exists='replace')
