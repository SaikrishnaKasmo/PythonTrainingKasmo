import pandas as pd

def load(data,engine):
    data = pd.DataFrame(data)
    data.to_sql(con=engine,if_exists = "replace",index = False,name = "Gmail_data")
    print(data.head(5))