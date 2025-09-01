import pandas as pd


def extract(engine):
    
    sql_query_1='select * from order_data'
    df1=pd.read_sql(sql_query_1,engine)
    sql_query_2='select * from customer_data'
    df2=pd.read_sql(sql_query_2,engine)
    return df1,df2



