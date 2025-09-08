import pandas as pd
from pymongo import MongoClient
from load import *
from extract import *
from transform import *
from config import *
import os
import configparser
from sqlalchemy import create_engine

# Connect
client = MongoClient("mongodb://localhost:27017/")
db = client["pythontraining"]
# collection = db["names"]

# Pull documents into a cursor
# cursor = collection.find({})   # {} = all documents

def mysql_setup():
    # config_path = os.path.join(os.path.dirname(__file__),"..","config","config.ini")
    # config = configparser.ConfigParser()
    # config.read(config_path)
    db_user = DATABASE["username"]
    db_pass = DATABASE["password"]
    db_server = DATABASE["server"]
    db_database = DATABASE["database"]
    db_port = DATABASE["port"]

    connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
    engine = create_engine(connection_string)
    return engine


def ETL():
# Convert cursor to DataFrame
    # df = pd.DataFrame(list(cursor))
    # print(df.head())
    df1 = extract_file(db,"doc")
    df1,df2,df3,df4=transformation(df1)
    print(load(df1,df2,df3,df4,mysql_setup()))
    # projects_df,project_tech_df = transform_project(df1)
    # engine = mysql_setup()
    # # load_to_mysql(engine,"projects_df",projects_df)
    # # print(load_to_mysql(engine,"project_tech_df",project_tech_df))
    # load(projects_df,project_tech_df,engine)

if __name__ == "__main__":
    ETL()
