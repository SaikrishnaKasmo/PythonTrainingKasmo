import pandas as pd
from datetime import datetime, timedelta

def scd4(current_data, history_data, incoming_data):
    current_data = current_data[["CustomerID", "Address", "Email", "FirstName", "LastName"]]
    history_data = history_data[["CustomerID", "Address", "Email", "FirstName", "LastName", "ValidFrom", "ValidTo"]]
    incoming_data = incoming_data[["CustomerID", "Address", "Email", "FirstName", "LastName"]]
    # # Existing "current" table
    # current_data = pd.DataFrame({
    #     "CustomerID": [1, 2],
    #     "Address": ["B Street", "X Street"],
    #     "Email": ["a@gmail.com", "x@gmail.com"],
    #     "FirstName": ["John", "Alice"],
    #     "LastName": ["Doe", "Smith"]
    # })

    # # Existing "history" table
    # history_data = pd.DataFrame({
    #     "CustomerID": [1],
    #     "Address": ["A Street"],
    #     "Email": ["a@gmail.com"],
    #     "FirstName": ["John"],
    #     "LastName": ["Doe"],
    #     "ValidFrom": ["2020-01-01"],
    #     "ValidTo": ["2023-12-31"]
    # })

    # # Incoming updates
    # incoming_data = pd.DataFrame({
    #     "CustomerID": [1, 2, 3],   # 3 is new
    #     "Address": ["C Street", "X Street", "Y Street"],
    #     "Email": ["new_a@gmail.com", "new_x@gmail.com", "y@gmail.com"],
    #     "FirstName": ["John", "Alice", "Bob"],
    #     "LastName": ["Doe", "Smith", "Brown"]
    # })

    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Track history changes
    history_records = []

    for _, new_row in incoming_data.iterrows():
        cust_id = new_row["CustomerID"]

        if cust_id in current_data["CustomerID"].values:
            # Fetch current record
            old_row = current_data[current_data["CustomerID"] == cust_id].iloc[0]

            # If Address changed -> archive old row into history
            if old_row["Address"] != new_row["Address"]:
                history_records.append({
                    "CustomerID": old_row["CustomerID"],
                    "Address": old_row["Address"],
                    "Email": old_row["Email"],
                    "FirstName": old_row["FirstName"],
                    "LastName": old_row["LastName"],
                    "ValidFrom": "2020-01-01",   # here you’d keep the old start date
                    "ValidTo": yesterday
                })

                # Update current table with new address & email
                current_data.loc[current_data.CustomerID == cust_id, ["Address"]] = [
                    new_row["Address"]
                ]

        else:
            # New customer → just insert into current table
            current_data = pd.concat([current_data, pd.DataFrame([new_row])], ignore_index=True)

    # Append new history rows if any
    if history_records:
        history_data = pd.concat([history_data, pd.DataFrame(history_records)], ignore_index=True)

    return current_data, history_data
    # print("✅ Current Table (latest state)")
    # print(current_data)

    # print("\n📜 History Table (all old versions)")
    # print(history_data)
