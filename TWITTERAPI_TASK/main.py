import requests
import pandas as pd
import re
import sqlite3
from config.config import *
from datetime import datetime
from src.extract import *
from src.transform import *
from src.load import *
import json
from sqlalchemy import create_engine

# Twitter API credentials
BEARER_TOKEN = keys["Bearer_key"]
query = "#BrandCampaign"
url = "https://api.twitter.com/2/tweets/search/recent"
headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
params = {
    "query": query,
    "tweet.fields": "id,text,author_id,created_at,public_metrics",
    "max_results": 50
}


#MYSQL
connection_string = (f"mysql+pymysql://{MYSQLDB['username']}:{MYSQLDB['password']}@{MYSQLDB['server']}:{MYSQLDB['port']}/{MYSQLDB['database']}") 
engine = create_engine(connection_string)

def main():
    # extraction(requests,url,headers,params)
    with open("./data/tweets_raw.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)

# Extract only the tweets list
    tweets = raw_data["data"]

    df1=  transformation(tweets)
    print(load_data(df1,engine,"tweets"))
if __name__ == "__main__":
    main()