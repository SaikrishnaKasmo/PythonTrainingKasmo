from sqlalchemy import create_engine
import pandas as pd
from src.extract import *
from src.load import *
from src.transform import *
from config import *

# MySQL connection info


# SQLAlchemy connection string for MySQL (using pymysql)
connection_string = (
    f"mysql+pymysql://{MYSQLDB['username']}:{MYSQLDB['password']}@{MYSQLDB['server']}:{MYSQLDB['port']}/{MYSQLDB['database']}"
)

# Create engine
engine = create_engine(connection_string)


def main():
    # Test connection
    try:
        with engine.connect() as connection:
            ETL()
            
    except Exception as e:
        print("Error connecting to the database:", e)

def ETL():
        customers_snapshot1,dim_customers_before1 = extraction(engine)
        dim_customers_scd2 = transform(customers_snapshot1,dim_customers_before1 )
        print(load_data(dim_customers_scd2,dim_customers_scd2))
if __name__ == "__main__":
    
    #main()
# 
    df = pd.read_sql("show tables;", engine)
    print(df.head())
