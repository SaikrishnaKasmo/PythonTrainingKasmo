import pandas as pd

def extract():
    product_dimension = pd.read_json(r"./data/product_dimension.json")
    sales_dimension = pd.read_json(r"./data/sales_dimensions.json")
    store_dimension = pd.read_json(r"./data/store_dimension.json")
    time_dimension = pd.read_json(r"./data/time_dimension.json")
    sales_fact = pd.read_json(r"./data/sales_fact.json")
    return [product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact]

extract()