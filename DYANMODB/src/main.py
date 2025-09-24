import boto3
import pandas as pd
from extract import *
from transform import *
from load import *
import sys
import os
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.config import *
session = boto3.Session(
aws_access_key_id=aws_details["aws_access_key_id"],
    aws_secret_access_key=aws_details["aws_secret_access_key"],
    region_name=aws_details["region_name"]
)

connection_string = (
    f"mysql+pymysql://{MYSQLDB['username']}:{MYSQLDB['password']}@{MYSQLDB['server']}:{MYSQLDB['port']}/{MYSQLDB['database']}"
)
engine = create_engine(connection_string)

dynamodb = session.resource("dynamodb")
table = dynamodb.Table("ast-dynamodb")

def main():
    data = extraction(table)
    df1,df2,df3,df4 = transformation(data)
    load(df1,df2,df3,df4,engine)

if __name__ == "__main__":
    main()
