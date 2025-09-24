#Etl Transformation File
import pandas as pd
from datetime import datetime, timedelta

def scd5(df1, df2):
    df1 = df1.copy()
    df2 = df2.copy()

    current_df1 = df1[df1["CurrentFlag"] == 1]

    # Find existing vs new customers
    existing_ids = set(current_df1["CustomerID"])
    incoming_ids = set(df2["CustomerID"])

    new_ids = incoming_ids - existing_ids
    existing_ids_to_update = incoming_ids & existing_ids

    # --- Exclude SCD1 columns from merge to avoid _old/_new suffix ---
    scd1_columns = ["Email", "Phone"]  # SCD1 only
    merge_columns = [c for c in df2.columns if c not in scd1_columns]

    merged = current_df1.merge(df2[merge_columns], on="CustomerID", suffixes=("_old", "_new"), how="inner")

    new_records = []

    for _, row in merged.iterrows():
        cust_id = row["CustomerID"]

        # --- SCD1: overwrite Email and Phone directly from df2 ---
        new_email = df2.loc[df2["CustomerID"] == cust_id, "Email"].values[0]
        new_phone = df2.loc[df2["CustomerID"] == cust_id, "Phone"].values[0]

        df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), ["Email", "Phone"]] = [new_email, new_phone]

        # --- SCD2 for Address ---
        if row["Address_old"] != row["Address_new"]:
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "CurrentFlag"] = 0

            new_version = row["Version"] + 1
            new_records.append({
                "CustomerID": cust_id,
                "FirstName": row["FirstName_old"],
                "LastName": row["LastName_old"],
                "LoyaltyTier": row["LoyaltyTier_old"],
                "SubscriptionStart": datetime.today().strftime("%Y-%m-%d"),
                "Phone": new_phone,          # keep updated (SCD1)
                "Address": row["Address_new"],
                "Email": new_email,          # keep updated (SCD1)
                "Version": new_version,
                "CurrentFlag": 1
            })

        # --- SCD3 for LoyaltyTier ---
        if row["LoyaltyTier_old"] != row["LoyaltyTier_new"]:
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "PrevLoyaltyTier"] = row["LoyaltyTier_old"]
            df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "LoyaltyTier"] = row["LoyaltyTier_new"]

    # Handle completely new customers
    if new_ids:
        new_customers = df2[df2["CustomerID"].isin(new_ids)].copy()
        new_customers["Version"] = 1
        new_customers["CurrentFlag"] = 1
        new_records.extend(new_customers.to_dict(orient="records"))

    # Append new records
    if new_records:
        df1 = pd.concat([df1, pd.DataFrame(new_records)], ignore_index=True)

    # Final result
    df1 = df1.sort_values(["CustomerID", "Version"]).reset_index(drop=True)
    return df1







# import pandas as pd
# from datetime import datetime, timedelta
# def scd2(df1, df2):
#     df1 = df1.copy()
#     df2 = df2.copy()

#     current_df1 = df1[df1["CurrentFlag"] == 1]

#     # Find existing vs new customers
#     existing_ids = set(current_df1["CustomerID"])
#     incoming_ids = set(df2["CustomerID"])

#     new_ids = incoming_ids - existing_ids
#     existing_ids_to_update = incoming_ids & existing_ids


#     # Handle updates for existing customers
#     merged = current_df1.merge(df2, on="CustomerID", suffixes=("_old", "_new"), how="inner")

#     new_records = []

#     for _, row in merged.iterrows():
#         cust_id = row["CustomerID"]
#         # --- SCD1 for Phone & Email ---
#         if row["Email_old"] != row["Email_new"] or row["Phone_old"] != row["Phone_new"]:
#             df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), ["Email_old","Phone_old"]] = [row["Email_new"], row["Phone_new"]]

#         # --- SCD2 for Address ---
#         if row["Address_old"] != row["Address_new"]:
#             # Close old record
#             #df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "SubscriptionEnd"] = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
#             df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "CurrentFlag"] = 0

#             # Insert new record with incremented version
#             new_version = row["Version"] + 1
#             new_records.append({
#                 "CustomerID": cust_id,
#                 "FirstName": row["FirstName_old"],  # keep first name same
#                 "LastName": row["LastName_old"],    # keep last name same
#                 "LoyaltyTier": row["LoyaltyTier_old"],  # keep loyalty tier same
#                 "SubscriptionStart": datetime.today().strftime("%Y-%m-%d"), # new subscription start date
#                 "Phone": row["Phone_old"],  # keep phone updated (SCD1 applied here)
#                 "Address": row["Address_new"],
#                 "Email": row["Email_old"],  # keep email updated (SCD1 applied here)
#                 "Version": new_version,
#                 "CurrentFlag": 1
#             })
#         if row["LoyaltyTier_old"] != row["LoyaltyTier_new"]:
#             # Just update loyalty tier in place (SCD3)
#             df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "PrevLoyaltyTier"] = row["LoyaltyTier_old"]
#             df1.loc[(df1.CustomerID == cust_id) & (df1.CurrentFlag == 1), "LoyaltyTier"] = row["LoyaltyTier_new"]


#     # Handle completely new customers
#     if new_ids:
#         new_customers = df2[df2["CustomerID"].isin(new_ids)].copy()
#         new_customers["Version"] = 1
#         new_customers["CurrentFlag"] = 1
#         new_records.extend(new_customers.to_dict(orient="records"))

#     # Append new records
#     if new_records:
#         df1 = pd.concat([df1, pd.DataFrame(new_records)], ignore_index=True)

#     # ----------------------------
#     # Final Result
#     # ----------------------------
#     df1 = df1.sort_values(["CustomerID", "Version"]).reset_index(drop=True)
#     return df1

