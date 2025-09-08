import pandas as pd


def load(table):
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Alice", "Bob"],
        "region": ["East", "West"],
        "loyalty_tier": ["Gold", "Silver"],
        "email": ["alice@example.com", "bob@example.com"]
    })

    for _, row in df.iterrows():
        table.put_item(
            Item={
                "customer_id": int(row["customer_id"]),
                "name": row["name"],
                "region": row["region"],
                "loyalty_tier": row["loyalty_tier"],
                "email": row["email"]
            }
        )
