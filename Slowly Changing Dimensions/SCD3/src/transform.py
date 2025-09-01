import pandas as pd
from datetime import datetime, timedelta

def scd3(df1, df2):

    df1 = df1.copy()
    df2 = df2.copy()

    current_df1 = df1[df1["CurrentFlag"] == 1]


    # Find existing vs new customers

    existing_ids = set(current_df1["CustomerID"])
    incoming_ids = set(df2["CustomerID"])

    new_ids = incoming_ids - existing_ids
    existing_ids_to_update = incoming_ids & existing_ids

    # Handle updates for existing customers

    merged = current_df1.merge(df2, on="CustomerID", suffixes=("_old", "_new"), how="inner")

    new_records = []

    for _, row in merged.iterrows():
        cust_id = row["CustomerID"]

        # SCD3 for Loyalty ---
        if (row["LoyaltyTier_old"] != row["LoyaltyTier_new"]) &  (cust_id in existing_ids):
            # Close old record
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "PrevLoyaltyTier"] = row["LoyaltyTier_old"]
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "LoyaltyTier"] = row["LoyaltyTier_new"]
    if new_ids:
        new_customers = df2[df2["CustomerID"].isin(new_ids)].copy()
        new_customers["Version"] = 1
        new_customers["CurrentFlag"] = 1
        new_records.extend(new_customers.to_dict(orient="records"))
    if new_records:
        df1 = pd.concat([df1, pd.DataFrame(new_records)], ignore_index=True)


    return df1

