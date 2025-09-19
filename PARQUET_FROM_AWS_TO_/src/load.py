import pandas as pd

def load(engine,df,file_name):
    df.to_sql(name= file_name,con= engine,index = False,if_exists = "replace")
    print(f"Successfully uploaded {file_name}")