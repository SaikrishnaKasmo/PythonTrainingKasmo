import pandas as pd

def first_name(name):
    dstr=name.split()
    if dstr[0] in ['Mr.','Mrs.','Miss', 'Dr.']:
        return dstr[1]
    else:
        return dstr[0]

def last_name(name):
    dstr=name.split()
    if dstr[len(dstr)-1] in ['Jr.', 'Sr.', 'II', 'III','MD']:
        return dstr[len(dstr)-2]
    else:
        return dstr[len(dstr)-1]
    
def transform_data(df1,df2):
    
    df2['first_name']=df2['name'].apply(first_name)
    df2['last_name']=df2['name'].apply(last_name)

    tier_map = {
    'Gold': 2,
    'Silver': 1,
    'Bronze': 0
    }

    df2['Customer_tier']=df2['loyalty_status'].map(tier_map)

    df1=df1.merge(df2,left_on='customer_id',right_on='customer_id',how='left')

    df1.drop(columns='name')

    return df1



