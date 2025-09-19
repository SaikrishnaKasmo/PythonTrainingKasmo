import pdfplumber
from src.extract import *
from src.transform import *
from sqlalchemy import create_engine
from config.config import *

# def pdf_to_text(pdf_file, txt_file):
#     with pdfplumber.open(pdf_file) as pdf, open(txt_file, "w", encoding="utf-8") as out:
#         for page in pdf.pages:
#             text = page.extract_text()
#             if text:
#                 out.write(text + "\n")

# pdf_to_text("sample.pdf", "output.txt")
import boto3
import os

# S3 Configuration
S3_BUCKET = "resume-kasmo"
S3_PREFIX = "TODO/"        # folder where PDFs are stored
S3_ARCHIVE_PREFIX = "ARCHIEVE/"
LOCAL_DOWNLOAD_DIR = "./data/"

# Create local directory if not exists
# os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

session = boto3.Session(
aws_access_key_id=aws_details["aws_access_key_id"],
    aws_secret_access_key=aws_details["aws_secret_access_key"],
    region_name=aws_details["region_name"]
)
# Initialize S3 client
s3 = session.client("s3")

# SQL SESSION
db_user = DATABASE["username"]
db_pass = DATABASE["password"]
db_server = DATABASE["server"]
db_database = DATABASE["database"]
db_port = DATABASE["port"]
connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
engine = create_engine(connection_string)

def download_and_convert_pdfs():
    extracted_texts = []
    # Make sure local dir exists
    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    # List PDF files in S3
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".pdf") and not key.startswith("archive/"):
            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
            
            # Download PDF
            s3.download_file(S3_BUCKET, key, local_path)
            
            # Convert PDF to text
            with pdfplumber.open(local_path) as pdf:
                text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
                extracted_texts.append({"filename": filename, "text": text})

            # ✅ Move file to archive/ folder
            s3.copy_object(
                Bucket='resume-kasmo',
                CopySource={'Bucket': 'resume-kasmo', 'Key': f"TODO/{filename}"},
                Key=f"archive/{filename}"
            )
            s3.delete_object(Bucket='resume-kasmo', Key=f"TODO/{filename}")

    return extracted_texts


# def download_and_convert_pdfs():
#     extracted_texts = []

#     # List PDF files in S3
#     response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
#     for obj in response.get("Contents", []):
#         key = obj["Key"]
#         if key.endswith(".pdf"):
#             local_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(key))
            
#             # Download PDF
#             s3.download_file(S3_BUCKET, key, local_path)
            
#             # Convert PDF to text
#             with pdfplumber.open(local_path) as pdf:
#                 text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
#                 extracted_texts.append({"filename": os.path.basename(key), "text": text})
#             s3.copy_object(Bucket='resume-kasmo',CopySource={'Bucket': 'resume-kasmo', 'Key': 'TODO/'+key},Key='archive/'+key)
#             s3.delete_object(Bucket='resume-kasmo',Key='TODO/'+ key)
#     return extracted_texts


if __name__ == "__main__":
    pdf_text_list = download_and_convert_pdfs() 

    for pdf_text in pdf_text_list:
        print(f"Processing {pdf_text['filename']}...")
        txt_content = pdf_text["text"]
        
        # Pass as string to your transform function
        transformed_data = transform_text(txt_content,engine)

# def download_pdfs_from_s3():
#     files_downloaded = []
#     response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
#     for obj in response.get("Contents", []):
#         key = obj["Key"]
#         if key.endswith(".pdf"):

#             # local_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(key))
#             # s3.download_file(S3_BUCKET, key, local_path)
#             # files_downloaded.append(local_path)
#             return files_downloaded
#     # return files_downloaded


# if __name__ == "__main__":
#     ff = download_pdfs_from_s3()
#     print(ff)


#     # extracted_text = extract_text_from_pdf("./data/Sai_Krishna_Resume.pdf")
#     # extracted_text = ff
#     # with open('./data/Sai_Krishna_Resume.txt', "w") as f:
#     #     for text in extracted_text:
#     #         f.write(text + "\n\n")
#     # txt_file = "./data/Sai_Krishna_Resume.txt"
#     # content = transformation(txt_file)

