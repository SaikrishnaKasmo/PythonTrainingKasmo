##MAIL task 

from __future__ import annotations
import boto3
from config import config
from config.config import *
import sys
import os
from sqlalchemy import create_engine
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import base64
import io
import numpy as np

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def extract():
    flow = InstalledAppFlow.from_client_secrets_file("./data/client.json",SCOPES)
    creds = flow.run_local_server(port = 0)
    service = build('gmail','v1',credentials = creds)
    response = service.users().messages().list(
        userId = 'me',
        # q='is:unread',
        maxResults = 5
    ).execute()
    return service,response

def get_header(name,headers):
    return next((h['value'] for h in headers if h['name'].lower() == name.lower()), None)

def transform(session,response,service):
    s3 = session.client('s3')
    mails= []
    for msg in response.get('messages',[]):
        msg_id = msg['id']
        full_msg = service.users().messages().get(userId = 'me', id = msg_id, format = 'full').execute()
        payload = full_msg.get('payload', {})
        headers = payload.get('headers', [])
        sender = get_header('From',headers)
        receiver = get_header('To',headers)
        cc = get_header('Cc',headers)
        subject = get_header('Subject',headers)
        date = get_header('Date',headers)

        attachments = []
        for subpart in payload.get('parts',[]):
            filename = subpart.get('filename')
            if filename :
                att_id = subpart['body'].get('attachmentId')
                if att_id:
                    att = service.users().messages().attachments().get(userId='me',messageId = msg_id, id = att_id).execute()
                    file_data = base64.urlsafe_b64decode(att['data'])
                    file_obj = io.BytesIO(file_data)
                    s3.upload_fileobj(file_obj, 'gmail-kasmo', filename)
                    url = f"https://gmail-kasmo.s3.us-east-1.amazonaws.com/{filename}"
                    attachments.append(url)

        data = {"msg_id":msg_id,"sender":sender,"receiver":receiver,"cc":cc,"subject":subject,"date":date}
        if len(attachments) > 0:
            data['attachments_1'] = attachments[0]
        if len(attachments) > 1:
            data['attachments_2'] = attachments[1]
        mails.append(data)
    return mails 
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

def load(data,engine):
    data = pd.DataFrame(data)
    data.to_sql(con=engine,if_exists = "replace",index = False,name = "Gmail_data")
    print(data.head(10))

def main():
    service,response = extract()
    data = transform(session,response,service)
    load(data,engine)
    # s3.upload_fileobj("./data/Resume-Latest.pdf","gmail-kasmo","Lateset")

if __name__ == "__main__":
    main()

