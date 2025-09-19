import pandas as pd

def transformation(df1):
    df1 = pd.json_normalize(df1,meta= [
        ['client','name'],
        ['client','industry'],
        ['client','location','city'],
        ['client','location','country'],
        ['team','members']])
    df1.rename(columns={
        'client.name':'client_name',
        'client.industry':'client_industry',
        'client.location.city':'client_city',
        'client.location.country':'client_country',
        'team.project_manager':'project_manager'},inplace=True)
    print(df1)
    df_project=df1[['project_id','project_name','status','client_name','client_industry','client_city','client_country','project_manager']]

    df_technologies = df1[['project_id','technologies']].explode("technologies")

    df_team_members = df1[['project_id','team.members']].explode('team.members')
    df_team_members_id = pd.json_normalize(df_team_members['team.members'])
    df_team_members_id = df_team_members_id.reset_index(drop=True)
    df_team_members = df_team_members.reset_index(drop=True)
    df_team_members = pd.concat([df_team_members.drop(['team.members'],axis=1),df_team_members_id],axis=1)

    df_milestones = df1[["project_id","milestones"]].explode("milestones")
    df_milestones_1 = pd.json_normalize(df_milestones["milestones"])

    df_milestones = df_milestones.reset_index(drop=True)
    df_milestones_1 = df_milestones_1.reset_index(drop=True)
    df_milestones = pd.concat([(df_milestones.drop(['milestones'],axis=1)),df_milestones_1],axis=1)

    df1['status'] = df1['status'].map({"In Progress": "Active", "Planned" : "Pending", "Completed" : "Done"})

    df_milestones['due_date'] = pd.to_datetime(df_milestones['due_date'])
    print(df_milestones)

    return df_project,df_technologies,df_milestones,df_team_members
