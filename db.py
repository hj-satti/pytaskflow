import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any

DB_PATH = Path("tasks.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    function TEXT NOT NULL,
    args_json TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    error TEXT
);
"""

def get_conn():
    return sqlite3.connect(DB_PATH)

def init():
    conn = get_conn()
    try:
        conn.execute(SCHEMA)
        conn.commit()
    finally:
        conn.close()

def insert_task(task: Dict[str, Any]):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO tasks (id, function, args_json, status, result_json, error) VALUES (?,?,?,?,?,?)",
            (
                task["id"],
                task["function"],
                task["args_json"],
                task["status"],
                task.get("result_json"),
                task.get("error"),
            ),
        )
        conn.commit()
    finally:
        conn.close()

def fetch_next_pending() -> Optional[dict]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, function, args_json, status, result_json, error FROM tasks WHERE status = 'PENDING' ORDER BY rowid LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "function": row[1],
            "args_json": row[2],
            "status": row[3],
            "result_json": row[4],
            "error": row[5],
        }
    finally:
        conn.close()

def update_result(task_id: str, status: str, result_json: Optional[str], error: Optional[str]):
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE tasks SET status = ?, result_json = ?, error = ? WHERE id = ?",
            (status, result_json, error, task_id),
        )
        conn.commit()
    finally:
        conn.close()

def update_status(task_id: str, status: str):
    conn = get_conn()
    try:
        conn.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()
    finally:
        conn.close()

def list_tasks():
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, function, status, result_json, error FROM tasks ORDER BY rowid"
        ).fetchall()
        return [
            {
                "id": r[0],
                "function": r[1],
                "status": r[2],
                "result_json": r[3],
                "error": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()