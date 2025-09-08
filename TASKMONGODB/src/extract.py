import pandas as pd

def extract_file(db,collectionname):
    cursor = db[collectionname].find({})
    return pd.DataFrame(list(cursor))
