import boto3
import pandas as pd
from extract import *
from transform import *
from load import *

# Explicit credentials (not recommended for production)
session = boto3.Session(
    aws_access_key_id="AKIA4OLIY6GTVLIT2CRN",
    aws_secret_access_key="mbTcb4JtXqK8Xj2wEL+/uHD83reFjcO5h8nPFAne",
    region_name="us-east-2"   # change to your region
)

# DynamoDB resource using session
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("projecttable2")

def main():
    extraction(table)

if __name__ == "__main__":
    main()