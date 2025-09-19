from sqlalchemy import create_engine
import pandas as pd
import boto3
import os
import sys
from extract import *
from transform import *
from load import *
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.config import *

session = boto3.Session(
    aws_access_key_id=AWS_DETAILS["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=AWS_DETAILS["AWS_SECRET_ACCESS_KEY"],
    region_name=AWS_DETAILS["AWS_REGION"]
)
s3 = session.client("s3")

#SQL SETUP
db_user = DATABASE["username"]
db_pass = DATABASE["password"]
db_database = DATABASE["database"]
db_port = DATABASE["port"]
db_server = DATABASE["server"]
connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
engine = create_engine(connection_string)

def main():
    house_df = extract(s3,"parquet-kasmo","house-price.parquet")
    weather_df = extract(s3,"parquet-kasmo","weather.parquet")
    house_df,weather_df = transform(house_df,weather_df)
    load(engine,house_df,"HousePrice")
    load(engine,weather_df,"Weather")
    
if __name__ == "__main__":
    main()
