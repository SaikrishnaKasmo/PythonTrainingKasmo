import pandas as pd
from src.extract import *
from src.transform_pyspark import *
from src.load import *
from config import config
from config.config import  *
from sqlalchemy import create_engine

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

def snowflake_setup():
    user = SNOWFLAKE['user']
    password = SNOWFLAKE['password']
    account = SNOWFLAKE["account"]
    warehouse = SNOWFLAKE["warehouse"]
    database  = SNOWFLAKE["database"]
    schema = SNOWFLAKE["schema"]

    engine = create_engine(
        f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    )
    return engine


def main():
    engine = snowflake_setup()
    product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact = extract()
    product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact= transform(product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact)
    load(engine,product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact)
    

if __name__ == "__main__":
    main()