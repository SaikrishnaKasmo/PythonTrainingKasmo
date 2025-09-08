import pandas as pd

def scd1(df1, df2):
    #df2 is incoming data.
    # Ensure both have CustomerID as index
    df1 = df1.set_index("CustomerID")
    df2 = df2.set_index("CustomerID")

    # Align df2 to df1 columns (add missing columns as NaN)
    df2 = df2.reindex(columns=df1.columns)

    # Update df1 with df2 (NaN values in df2 will overwrite df1)
    df1.update(df2)
    df1['Version'] = df1['Version'] + 1

    # --- Step 2: Find new rows in df2 not present in df1 ---
    new_rows = df2.loc[~df2.index.isin(df1.index)]
    new_rows['Version'] = 1  # Initialize version for new rows

    # Append new rows
    df_final = pd.concat([df1, new_rows])

    # Reset index to bring CustomerID back as a column
    return df_final.reset_index()
