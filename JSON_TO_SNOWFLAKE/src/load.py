import pandas as pd
from sqlalchemy import create_engine,text

from sqlalchemy import create_engine, text

def load(engine, product_dimension, sales_dimension, store_dimension, time_dimension, sales_fact):
    # Step 1: Create schema objects
    with engine.connect() as conn:
        conn.execute(text("USE DATABASE pythontraining"))
        conn.execute(text("USE SCHEMA public"))

        ddl_statements = [

            # Sales Dimension
            """
            CREATE OR REPLACE TABLE sales_dimension (
                supplier_id INT PRIMARY KEY,
                supplier_name STRING,
                contact_email STRING,
                supplier_country STRING,
                reliability_score FLOAT,
                region_id INT,
                region_name STRING,
                region_country STRING,
                regional_manager STRING,
                promotion_id INT,
                promotion_name STRING,
                discount_percentage INT,
                start_date DATE,
                end_date DATE
            );
            """,

            # Product Dimension
            """
            CREATE OR REPLACE TABLE product_dimension (
                product_id INT PRIMARY KEY,
                product_name STRING,
                category STRING,
                product_line STRING,
                brand STRING,
                price FLOAT,
                sku STRING,
                description STRING,
                weight_kg FLOAT,
                supplier_id INT,
                is_active BOOLEAN,
                stock_level INT,
                CONSTRAINT fk_product FOREIGN KEY (supplier_id) REFERENCES sales_dimension(supplier_id)
            );
            """,

            # Store Dimension
            """
            CREATE OR REPLACE TABLE store_dimension (
                store_id INT PRIMARY KEY,
                store_name STRING,
                state STRING,
                city STRING,
                country STRING,
                store_type STRING,
                manager_name STRING,
                opening_date DATE,
                region_id INT,
                square_footage INT,
                employee_count INT
            );
            """,

            # Time Dimension
            """
            CREATE OR REPLACE TABLE time_dimension (
                date_id INT PRIMARY KEY,
                date DATE,
                day_of_week STRING,
                month STRING,
                quarter STRING,
                year INT,
                fiscal_year INT,
                fiscal_quarter STRING,
                is_holiday BOOLEAN,
                holiday_name STRING,
                week_number INT,
                is_weekend BOOLEAN,
                is_business_day BOOLEAN
            );
            """,

            # Sales Fact
            """
            CREATE OR REPLACE TABLE sales_fact (
                sale_id INT PRIMARY KEY,
                product_id INT,
                date_id INT,
                store_id INT,
                promotion_id INT,
                quantity_sold INT,
                discount_applied DECIMAL(10,2),
                tax_amount DECIMAL(10,2),
                net_amount DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                customer_id STRING,
                payment_method STRING,
                transaction_type STRING,
                CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES product_dimension(product_id),
                CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store_dimension(store_id),
                CONSTRAINT fk_time FOREIGN KEY (date_id) REFERENCES time_dimension(date_id)
            );
            """
        ]

        # Execute all DDLs
        for ddl in ddl_statements:
            conn.execute(text(ddl))

    # Step 2: Load data from Pandas to Snowflake (append to preserve schema/constraints)
    sales_dimension.to_sql("sales_dimension", engine, index=False, if_exists="append")
    product_dimension.to_sql("product_dimension", engine, index=False, if_exists="append")
    store_dimension.to_sql("store_dimension", engine, index=False, if_exists="append")
    time_dimension.to_sql("time_dimension", engine, index=False, if_exists="append")
    sales_fact.to_sql("sales_fact", engine, index=False, if_exists="append")

    print("✅ Loaded Successfully with Constraints Preserved")


# def load(engine,product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact):

#     with engine.connect() as conn:
#         conn.execute(text("USE DATABASE pythontraining"))
#         conn.execute(text("USE SCHEMA public"))
#     ddl_statements = [
#     """
#     CREATE OR REPLACE TABLE sales_dimension (
#         supplier_id INT PRIMARY KEY,
#     supplier_name STRING,
#     contact_email STRING,
#     supplier_country STRING,
#     reliability_score FLOAT,
#     region_id INT,
#     region_name STRING,
#     region_country STRING,
#     regional_manager STRING,
#     promotion_id INT,
#     promotion_name STRING,
#     discount_percentage INT,
#     start_date DATE,
#     end_date DATE
#     );
# """,
# """
#     CREATE OR REPLACE TABLE product_dimension (
#         product_id INT PRIMARY KEY,
#         product_name STRING,
#         category STRING,
#         product_line STRING,
#         brand STRING,
#         PRICE FLOAT,
#         sku STRING,
#         description STRING,
#         weight_kg FLOAT,
#         supplier_id INT,
#         is_active BOOLEAN,
#         stock_level INT,
#         CONSTRAINT fk_product FOREIGN KEY (supplier_id) REFERENCES sales_dimensions(supplier_id)
#     );
#     """,
#     """
#     CREATE OR REPLACE TABLE store_dimension (
#         store_id INT PRIMARY KEY,
#         store_name STRING
#         ,state STRING
#         ,city STRING
#         ,country STRING
#         ,store_type STRING
#         ,manager_name STRING
#         ,opening_date DATE
#         ,region_id INT
#         ,square_footage INT
#         ,employee_count INT

#     );
#     """,
#     """
#     CREATE OR REPLACE TABLE time_dimension (
#     date_id INT PRIMARY KEY
#     ,date DATE
#     ,day_of_week STRING
#     ,month STRING
#     ,quarter STRING
#     ,year INT
#     ,fiscal_year INT
#     ,fiscal_quarter STRING
#     ,is_holiday BOOLEAN
#     ,holiday_name STRING
#     ,week_number INT
#     ,is_weekend BOOLEAN
#     ,is_business_day BOOLEAN
#     );
#     """,
#     """
#     CREATE OR REPLACE TABLE sales_fact (
#         sale_id INT PRIMARY KEY,
#         product_id INT,
#         date_id INT,
#         store_id INT
#         ,promotion_id INT
#         ,quantity_sold INT
#         ,discount_applied DECIMAL(10,2)
#         ,tax_amount DECIMAL(10,2)
#         ,net_amount DECIMAL(10,2)
#         ,total_amount DECIMAL(10,2)
#         ,customer_id STRING
#         ,payment_method STRING
#         ,transaction_type STRING,
#         CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES product_dimension(product_id),
#         CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store_dimension(store_id),
#         CONSTRAINT fk_time FOREIGN KEY (date_id) REFERENCES time_dimension(date_id)
#     );
#     """
# ]

#     with engine.connect() as conn:
#         for ddl in ddl_statements:
#             conn.execute(text(ddl))


#     product_dimension.to_sql("product_dimension",engine,index=False,if_exists="replace")
#     sales_dimension.to_sql("sales_dimension",engine,index = False, if_exists = "replace")
#     store_dimension.to_sql("store_dimension",engine,index = False, if_exists = "replace")
#     sales_fact.to_sql("sales_fact",engine,index = False, if_exists = "replace")
#     time_dimension.to_sql("time_dimension",engine,index=False, if_exists = "replace")

#     print("Loaded Successfully")