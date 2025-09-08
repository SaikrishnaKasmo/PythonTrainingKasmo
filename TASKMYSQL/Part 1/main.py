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
    customer,orders,order_items,products= extraction(engine)
    sales, order_summary, region_sales_ranked, category_sales= transform(customer,orders,order_items,products)
    print(load_files([sales,order_summary,region_sales_ranked,category_sales],engine,["sales","order_summary","region_sales_ranked","category_sales"]))

if __name__ == "__main__":
    main()

# Data ingestion

# df1  = pd.read_csv("./data/customers 1.csv")
# df2 = pd.read_csv("./data/order_items 1.csv")
# df3 = pd.read_csv("./data/orders 1.csv")
# df4 = pd.read_csv("./data/products 1.csv")
# df = pd.read_sql("show tables;", engine)
# df1.to_sql(name="customer", con=engine, index=False, if_exists='replace')
# df2.to_sql(name="order_items", con=engine, index=False, if_exists='replace')
# df3.to_sql(name="orders", con=engine, index=False, if_exists='replace')
# df4.to_sql(name="products", con=engine, index=False, if_exists='replace')

# print(df.head())
# # 
# df = pd.read_sql("show tables;", engine)
# print(df.head())
