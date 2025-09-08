import pandas as pd

def extraction(table):
    response = table.scan()
    items = response["Items"]
    df_loaded = pd.DataFrame(items)
    print(df_loaded)
