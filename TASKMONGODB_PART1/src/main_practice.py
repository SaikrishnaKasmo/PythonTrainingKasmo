from pymongo import MongoClient
import pandas as pd

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

session = MongoClient("mongodb://localhost:27017/")
db = session['pythontraining']



def ext(db,collectionname):
    cursor = db[collectionname].find({})
    return cursor



def load(a,b,c,d,engine):
    connection = engine.connect()
    tranasaction = connection.begin()
    try:
        a.to_sql(con = engine,index= False, name= "a",if_exists='replace')
        tranasaction.commit()
        return 'Loaded'
    except Exception as e:
        tranasaction.rollback()
        return 'Failed'
    finally:
        connection.close()

def tran(a):
    df1 = pd.json_normalize(a,meta=[
        ["cilent","name"],
        ["client","industry"],
        ["client","location","city"],
        ["client","location","country"],
        ["team","project_managers"]
    ])
    df1.rename(
        columns={
                   'client.name':'client_name',
        'client.industry':'client_industry',
        'client.location.city':'client_city',
        'client.location.country':'client_country',
        'team.project_manager':'project_manager'},inplace=True
    )

    project = df1[["project_id","project_name","client_name","client_industry","client_city","client_country","project_manager"]]

    df_technologies = df1[["project_id","technologies"]].explode("technologies")
    df_team_members = df1[["project_id","team.members"]].explode("team.members")
    df_team_members_1 = pd.json_normalize(df_team_members["team.members"])

    df_team_members = df_team_members.reset_index(drop=True)
    df_team_members_1 = df_team_members_1.reset_index(drop=True)
    df_team_members = pd.concat([df_team_members.drop(["team.members"],axis=1),df_team_members_1],axis=1)

    #Milestones
    df_milestones = df1[["project_id","milestones"]].explode("milestones")
    df_milestones_1 = pd.json_normalize(df_milestones["milestones"])
    df_milestones = df_milestones.reset_index(drop=True)
    df_milestones_1 = df_milestones_1.reset_index(drop=True)
    df_milestones = pd.concat([df_milestones.drop(["milestones"],axis=1),df_milestones_1],axis=1)
    
    #status standardisation
    df1["status"] = df1["status"].map({"In Progress":"Active","Planned":"Pending","Completed":"Done"})
    
    ##milestones_date
    df_milestones["due_date"] = pd.to_datetime(df_milestones["due_date"])
    return project,df_technologies,df_team_members,df_milestones
def main():
    a = ext(db,"doc")
    a,b,c,d = tran(a)
    load(a,b,c,d,mysql_setup())

main()