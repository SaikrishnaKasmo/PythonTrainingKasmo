from src.extract import extract
from src.transform import transform_data
from src.load import load_data
from sqlalchemy import create_engine
import pandas as pd
import pyodbc

# SQL Server connection info
server = "localhost,1433"          # Note the comma before port
database = "pythontraining"
username = "sa"
password = "NewStrongPassword123!"  # Must match what you used in Docker

# SQLAlchemy connection string
connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create engine
engine = create_engine(connection_string)

# Test query
try:
    df = pd.read_sql("SELECT name FROM sys.databases;", engine)
    print("Connection successful! Databases:")
    print(df)
except Exception as e:
    print("Connection failed:", e)

def main():
    
    df1,df2=extract(engine)

    df1=transform_data(df1,df2)

    print(load_data(df1,engine))


if __name__=="__main__":
    main()