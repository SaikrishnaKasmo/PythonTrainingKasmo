import pandas as pd
from datetime import datetime, timedelta
def scd2(df1, df2):

    # ----------------------------
    # Step 1: Prepare current df1 only
    # ----------------------------
    df1 = df1.copy()
    df2 = df2.copy()

    current_df1 = df1[df1["CurrentFlag"] == 1]

    # ----------------------------
    # Step 2: Find existing vs new customers
    # ----------------------------
    existing_ids = set(current_df1["CustomerID"])
    incoming_ids = set(df2["CustomerID"])

    new_ids = incoming_ids - existing_ids
    existing_ids_to_update = incoming_ids & existing_ids

    # ----------------------------
    # Step 3: Handle updates for existing customers
    # ----------------------------
    merged = current_df1.merge(df2, on="CustomerID", suffixes=("_old", "_new"), how="inner")

    new_records = []

    for _, row in merged.iterrows():
        cust_id = row["CustomerID"]

        # --- SCD2 for Address ---
        if row["Address_old"] != row["Address_new"]:
            # Close old record
            #df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "SubscriptionEnd"] = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "CurrentFlag"] = 0

            # Insert new record with incremented version
            new_version = row["Version"] + 1
            new_records.append({
                "CustomerID": cust_id,
                "FirstName": row["FirstName_new"],  # keep first name same
                "LastName": row["LastName_new"],    # keep last name same
                "LoyaltyTier": row["LoyaltyTier_old"],  # keep loyalty tier same
                "SubscriptionStart": datetime.today().strftime("%Y-%m-%d"), # new subscription start date
                "Phone": row["Phone_new"],  # keep phone updated (SCD1 applied here)
                "Address": row["Address_new"],
                "Email": row["Email_new"],  # keep email updated (SCD1 applied here)
                "Version": new_version,
                "CurrentFlag": 1
            })

        # --- SCD1 for Email ---
        elif row["Email_old"] != row["Email_new"]:
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "Email"] = row["Email_new"]

    # ----------------------------
    # Step 4: Handle completely new customers
    # ----------------------------
    if new_ids:
        new_customers = df2[df2["CustomerID"].isin(new_ids)].copy()
        new_customers["Version"] = 1
        new_customers["CurrentFlag"] = 1
        new_records.extend(new_customers.to_dict(orient="records"))
    # ----------------------------
    # Step 5: Append new records
    # ----------------------------
    if new_records:
        df1 = pd.concat([df1, pd.DataFrame(new_records)], ignore_index=True)

    # ----------------------------
    # Final Result
    # ----------------------------
    df1 = df1.sort_values(["CustomerID", "Version"]).reset_index(drop=True)
    return df1

