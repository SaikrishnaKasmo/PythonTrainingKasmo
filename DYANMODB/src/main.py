import boto3
import pandas as pd
from extract import *
from transform import *
from load import *
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.config import *
# Initialize DynamoDB client
session = boto3.Session(
aws_access_key_id=aws_details["aws_access_key_id"],
    aws_secret_access_key=aws_details["aws_secret_access_key"],
    region_name=aws_details["region_name"]
)

dynamodb = session.resource("dynamodb")
table = dynamodb.Table("projecttable2")

def main():
    extraction(table)

if __name__ == "__main__":
    main()
