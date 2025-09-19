import pandas as pd

def transform_project(df):
    # Transformations
    #Step1:
    df1 = df[["project_id", "project_name", "client", "domain",
        "location", "project_manager", "start_date",
        "end_date", "status"]].copy()
    
    technologies = pd.DataFrame({
    "project_id":df["project_id"],
    "technologies":df["technologies"]
}).explode("technologies")
    project_tech_df = technologies.rename(columns={"technologies":"Technology"}).reset_index(drop=True)

    #Step 3:
    status_map = {
    "In Progress": "Active",
    "Planned": "Pending",
    "Completed": "Done",
    }
    df1["status"] = df1["status"].map(status_map).fillna(df1["status"])

    #Step 4:
    #Dates
    # df1["start_date"] = pd.to_datetime(df1["start_date"])
    df1.loc[:, "start_date"] = pd.to_datetime(df1["start_date"])
    df1.loc[:,"end_date"] = pd.to_datetime(df1["end_date"])

    df1 = df1.drop_duplicates()

    Null_count = df1.isnull().sum().sum()
    print("Count of Nulls:",Null_count)

    df1['location']= df1['location'].str.title()
    df1['domain'] = df1['domain'].str.title()

    return df1,project_tech_df