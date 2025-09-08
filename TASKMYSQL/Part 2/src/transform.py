import pandas as pd
import numpy as np
from datetime import datetime

def transform(customers_snapshot1,dim_customers_before1):

    today = datetime.today().date()

    # Transformation 1:
    merged = dim_customers_before1.merge(customers_snapshot1, on="customer_id", how="left", suffixes=("_new", "_old"))

    #Transfomation 2:
    change_mask = (
    (merged['region_new'] != merged['region_old']) |
    (merged['loyalty_tier_new'] != merged['loyalty_tier_old']) |
    (merged['email_new'] != merged['email_old'])
)

    changed_customers = merged[change_mask & merged['name_old'].notna()]
    new_customers = merged[merged['name_old'].isna()]

    #Transformation 3:
    
    customers_snapshot1.loc[
        customers_snapshot1['customer_id'].isin(changed_customers['customer_id']),
        ['valid_to', 'is_current']] = [pd.to_datetime(today) - pd.Timedelta(days=1), "N"]
    
    # Transformation 4:
    records_to_insert = pd.concat([changed_customers, new_customers], ignore_index=True)

    scd2_new_records = records_to_insert[[
        "customer_id", "name_new", "region_new", "loyalty_tier_new", "email_new"
    ]].rename(columns={
        "name_new": "name",
        "region_new": "region",
        "loyalty_tier_new": "loyalty_tier",
        "email_new": "email"
    })
    scd2_new_records["valid_from"] = today
    scd2_new_records["valid_to"] = datetime(9999, 12, 31)
    scd2_new_records["is_current"] = "Y"
    #Transformation 5: 
    dim_customers_scd2 = pd.concat([customers_snapshot1, scd2_new_records], ignore_index=True)


    return dim_customers_scd2

