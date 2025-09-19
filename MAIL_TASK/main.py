import boto3
from config import config
from config.config import *
from src.extract import extract
from src.load import load
from src.transform import transform
import sys
import os
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

session = boto3.Session(
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION
)

# MYSQL SESSION
db_user = DATABASE["username"]
db_pass = DATABASE["password"]
db_server = DATABASE["server"]
db_database = DATABASE["database"]
db_port = DATABASE["port"]
connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
engine = create_engine(connection_string)

def main():
    service,response = extract()
    data = transform(session,response,service)
    load(data,engine)
    # s3.upload_fileobj("./data/Resume-Latest.pdf","gmail-kasmo","Lateset")

if __name__ == "__main__":
    main()
