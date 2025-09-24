##Parquet file extraction

import boto3
import pandas as pd
from io import BytesIO

def extract(s3,bucket_name,file_name):
    obj1 = s3.get_object(Bucket = bucket_name,Key = file_name)
    df1 = pd.read_parquet(BytesIO(obj1["Body"].read()))

    return df1