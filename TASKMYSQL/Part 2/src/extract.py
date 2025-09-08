import pandas as pd


def extraction(engine):
    customers_snapshot1 =  pd.read_sql("SELECT * FROM customers_snapshot1;", engine)
    dim_customers_before1 =  pd.read_sql("SELECT * FROM dim_customers_before1;", engine)
