#AWS Resume

import boto3
import pandas as pd
import os
import sys
from sqlalchemy import create_engine
import pdfplumber
import re

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.config import *
#AWS s3
session = boto3.Session(
    aws_access_key_id= aws_details["aws_access_key_id"],
    aws_secret_access_key= aws_details["aws_secret_access_key"],
    region_name=aws_details["region_name"]
)
s3 = session.client("s3")

S3_BUCKET = "resume-kasmo"
S3_PREFIX = "TODO/"
S3_ARCHIVE_PREFIX = "ARCHIEVE/"
LOCAL_DOWNLOAD_DIR = "./data/"

#Mysql Configuration
connection_string = (f"mysql+pymysql://{DATABASE['username']}:{DATABASE['password']}@{DATABASE['server']}:{DATABASE['port']}/{DATABASE['database']}")
engine = create_engine(connection_string)


def transform(all_text,engine):
    mail = capture_mail(all_text)
    number = capture_phonenumber(all_text)
    name = capture_name(all_text)
    summary = capture_summary(all_text)
    df1 = pd.DataFrame({
        "name": [name],
        "email": [mail],
        "experience": [summary],
        "phone_number": [number]
    })
    df1.to_sql(name="resumes_data_table",con=engine,if_exists='append',index = False)
def capture_mail(all_text):
    pattern  = re.compile(r'\w*@.\w*.\w*')
    matches = pattern.finditer(all_text)
    for match in matches:
        mail = match.group(0)
    return mail
def capture_phonenumber(all_text):
    pattern = re.compile(r'\(?(\d){3}\)?(\.|-)(\d){3}(\.|-)(\d){4}')
    matches = pattern.finditer(all_text)
    for match in matches:
        number  = re.sub(r'\D',"",match.group(0))
    return number

def capture_name(all_text):
    pattern = re.compile(r'^([a-zA-Z ])+\n')
    matches = pattern.finditer(all_text)
    for match in matches:
        name = match.group(0)
    return name

def capture_summary(all_text):
    pattern = re.compile(r'summary(.*?)(\.|,|\n)+(Technical|Project|SKILLS|Education|$)',re.DOTALL | re.IGNORECASE)
    match = pattern.search(all_text)
    return match.group(1)

def main():
    text = extract(s3)
    for t in text:
        transform(t["text"],engine)

def extract(s3):
    extracted_txt = []
    os.makedirs(LOCAL_DOWNLOAD_DIR,exist_ok=True)
    response = s3.list_objects_v2(Bucket="resume-kasmo",Prefix=S3_PREFIX)
    for obj in response.get("Contents",[]):
        key =  obj["Key"]
        if key.endswith(".pdf") and not key.startswith(S3_ARCHIVE_PREFIX):
            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR,filename)

            s3.download_file(S3_BUCKET,key,local_path)
            with pdfplumber.open(local_path) as pdf:
                text = "/n".join(page.extract_text() for page in pdf.pages if page.extract_text())  
                extracted_txt.append({"filename":filename,"text":text})

            s3.copy_object(
                Bucket = S3_BUCKET,
                CopySource = {'Bucket':S3_BUCKET,'Key':f"TODO/{filename}"},
                Key = f"archive/{filename}"
            )
            s3.delete_object(
                Bucket  = S3_BUCKET,
                Key = f"archive/{filename}" # Make this TODO file to work.
            )
    return extracted_txt
main()




