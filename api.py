from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from task_queue import push_task, get_task_status, r
import json

app = FastAPI(title="Task Queue API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class TaskRequest(BaseModel):
    task_type: str
    payload: dict
    priority: int = 1

@app.post("/tasks")
def create_task(request: TaskRequest):
    """Submit a new task"""
    task_id = push_task(request.task_type, request.payload, request.priority)
    return {"message": "Task added!", "task_id": task_id}

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    """Get status of a specific task"""
    task = get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.get("/stats")
def get_stats():
    """See queue stats + metrics"""
    # Get all task durations
    durations = r.lrange("task_durations", 0, -1)
    durations = [float(d) for d in durations]

    avg_duration = round(sum(durations) / len(durations), 2) if durations else 0
    total_processed = r.get("total_tasks_processed") or 0

    return {
        "high_priority_queue": r.llen("high_priority_queue"),
        "normal_queue": r.llen("normal_queue"),
        "dead_letter_queue": r.llen("dead_letter_queue"),
        "total_tasks_processed": int(total_processed),
        "avg_processing_time_seconds": avg_duration,
    }

@app.get("/dead-letter")
def get_dead_letter_tasks():
    """See all failed tasks"""
    tasks = r.lrange("dead_letter_queue", 0, -1)
    return {"failed_tasks": [json.loads(t) for t in tasks]}

@app.get("/")
def root():
    return {"message": "Task Queue API is running! Go to /docs to test it."}