import pandas as pd


def load(df1,df2,df3,df4,engine):
    df1.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df1")
    df2.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df2")
    df3.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df3")
    df4.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df4")
    print("successfully_load")