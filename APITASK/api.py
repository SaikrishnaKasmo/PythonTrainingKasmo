#API P1
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from config import *

app=FastAPI()

server_conn={
    'server':r'SSS-DESKTOP\SQLEXPRESS',\
    'database':'master',\
        }

db_user = DATABASE["username"]
db_pass = DATABASE["password"]
db_server = DATABASE["server"]
db_database = DATABASE["database"]
db_port = DATABASE["port"]
connection_string = (f"mysql+pymysql://{db_user}:{db_pass}@{db_server}:{db_port}/{db_database}")
engine = create_engine(connection_string)

class TaskInput(BaseModel):
    title:str
    status:str


@app.post("/etl1")
def create_task(task_input:TaskInput):
    try:
        dicti=task_input.model_dump()
        df=pd.DataFrame([dicti])
        df['created_at']=date.today()
        df.to_sql(name='api_call',con=engine,if_exists='append',index=False)
        return {"message": "Loaded Successfully"}
    except Exception as e:
        return {"error":str(e)}



if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)

