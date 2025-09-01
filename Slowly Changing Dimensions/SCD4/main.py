from src.extract import *
from src.transform import *
from src.load import *
from config import *
from sqlalchemy import create_engine
import pandas as pd
import pyodbc



# SQLAlchemy connection string
connection_string = (
    f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
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
    # Existing data in target table
    curr = get_target_table(engine,'current_data')
    hist = get_target_table(engine,'history_data')

    # New data from Master CSV or Updates to be applied
    df = get_source_table(engine,'./data/Customer_Updates.csv')

    curr,hist= scd4(current_data=curr,history_data=hist,incoming_data=df)

    print(load_data(curr,engine,'current_data'))
    print(load_data(hist,engine,'history_data'))


if __name__=="__main__":
    main() 