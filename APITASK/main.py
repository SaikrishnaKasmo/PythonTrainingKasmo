from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import json

# In-memory "database"
tasks = []

class Task(BaseModel):
    title: str
    status: str

STATUS_MAP = {
    "pending": "Pending",
    "in progress": "In Progress",
    "done": "Done"
}

def transform_task(task: Task):
    return {
        "title": task.title.title(),
        "status": STATUS_MAP.get(task.status.lower(), "Pending"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        with open("tasks_raw.json", "r") as f:
            data = json.load(f)
            for t in data:
                task = Task(**t)
                tasks.append(transform_task(task))
        print(f"✅ Loaded {len(tasks)} tasks from tasks_raw.json")
    except FileNotFoundError:
        print("⚠️ No initial dataset found.")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/tasks")
def add_task(task: Task):
    transformed = transform_task(task)
    tasks.append(transformed)
    return {"message": "Task added successfully!", "task": transformed}

@app.get("/tasks")
def get_tasks():
    return tasks
