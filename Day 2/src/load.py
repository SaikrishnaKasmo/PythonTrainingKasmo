import pandas as pd

def load_data(df1,engine):
    df1.to_sql(name='Unified_customer',con=engine,index=False,if_exists='replace')
    return 'Loaded Succesfully'
