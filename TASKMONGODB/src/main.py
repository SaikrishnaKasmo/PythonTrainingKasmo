import pandas as pd
from pymongo import MongoClient
from load import *
from extract import *
from transform import *
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
    config_path = os.path.join(os.path.dirname(__file__),"..","config","config.ini")
    config = configparser.ConfigParser()
    config.read(config_path)
    db_user = config["database"]["username"]
    db_pass = config["database"]["password"]
    db_server = config["database"]["server"]
    db_database = config["database"]["database"]
    db_port = config["database"]["port"]

    connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
    engine = create_engine(connection_string)
    return engine


def ETL():
# Convert cursor to DataFrame
    # df = pd.DataFrame(list(cursor))
    # print(df.head())
    df1 = extract_file(db,"project")
    projects_df,project_tech_df = transform_project(df1)
    engine = mysql_setup()
    # load_to_mysql(engine,"projects_df",projects_df)
    # print(load_to_mysql(engine,"project_tech_df",project_tech_df))
    load(projects_df,project_tech_df,engine)

if __name__ == "__main__":
    ETL()
