#DyanmoDB Pract File
import boto3
from sqlalchemy import create_engine
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.config import *

session = boto3.Session(
    aws_access_key_id=aws_details["aws_access_key_id"],
    aws_secret_access_key=aws_details["aws_secret_access_key"],
    region_name=aws_details["region_name"])
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("last-dynamodb")

##MYSQL ENGINE
connection_string = (f"mysql+pymysql://{MYSQLDB['username']}:{MYSQLDB['password']}@{MYSQLDB['server']}:{MYSQLDB['port']}/{MYSQLDB['database']}")
engine = create_engine(connection_string)

def extract(table):
    response = table.scan()
    items = response["Items"]
    return items
def transform(df1):
    print(df1)
    df1 = pd.json_normalize(df1
                            ,meta= [
        ['client','name'],
        ['client','industry'],
        ['client','location','city'],
        ['client','location','country'],
        ['team','members']])
    df1.columns=['project_name','technologies','status','milestones','project_id','client_name','client_industry','client_city','client_country'
                       ,'project_manager','team_members']
    df_project=df1[['project_id','project_name','status','client_name','client_industry','client_city','client_country','project_manager']]

    df_technologies = df1[['project_id','technologies']].explode("technologies")

    df_team_members = df1[['project_id','team_members']].explode('team_members')
    df_team_members_id = pd.json_normalize(df_team_members['team_members'])
    df_team_members_id = df_team_members_id.reset_index(drop=True)
    df_team_members = df_team_members.reset_index(drop=True)
    df_team_members = pd.concat([df_team_members.drop(['team_members'],axis=1),df_team_members_id],axis=1)

    df_milestones = df1[["project_id","milestones"]].explode("milestones")
    df_milestones_1 = pd.json_normalize(df_milestones["milestones"])

    df_milestones = df_milestones.reset_index(drop=True)
    df_milestones_1 = df_milestones_1.reset_index(drop=True)
    df_milestones = pd.concat([(df_milestones.drop(['milestones'],axis=1)),df_milestones_1],axis=1)

    df1['status'] = df1['status'].map({"In Progress": "Active", "Planned" : "Pending", "Completed" : "Done"})

    df_milestones['due_date'] = pd.to_datetime(df_milestones['due_date'])
    print(df_milestones)

    return df_project,df_technologies,df_milestones,df_team_members

def load(df1,df2,df3,df4,engine):
    df1.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df1")
    df2.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df2")
    df3.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df3")
    df4.to_sql(con=engine,index= False,if_exists= 'replace',name="dynamodb_df4")
    print("successfully_load")

def main(table):
    items = extract(table)
    df1,df2,df3,df4 = transform(items)
    load(df1,df2,df3,df4,engine)


main(table)