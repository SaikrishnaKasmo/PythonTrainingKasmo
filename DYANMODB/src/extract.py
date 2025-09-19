import pandas as pd

def extraction(table):
    response = table.scan()
    items = response["Items"]
    return items
