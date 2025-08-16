import time
import uuid
import threading
import json

from db import (
    init as db_init,
    insert_task,
    fetch_next_pending,
    update_result,
    list_tasks,
    update_status,
)

# Registry of allowed functions
FUNCTIONS: dict[str, callable] = {}

def task(fn):
    FUNCTIONS[fn.__name__] = fn
    return fn

@task
def add_numbers(a, b):
    return a + b

@task
def say_hello(name):
    return f"Hello, {name}!"

def enqueue(function_name: str, args: dict) -> str:
    if function_name not in FUNCTIONS:
        raise ValueError("Function not registered: " + function_name)
    task_id = uuid.uuid4().hex
    insert_task({
        "id": task_id,
        "function": function_name,
        "args_json": json.dumps(args),
        "status": "PENDING",
        "result_json": None,
        "error": None,
    })
    print(f"[ENQUEUE] id={task_id} func={function_name} args={args}")
    return task_id

def worker_loop(stop_flag: dict):
    while not stop_flag["stop"]:
        task_row = fetch_next_pending()
        if not task_row:
            time.sleep(0.25)
            continue
        update_status(task_row["id"], "RUNNING")
        fn = FUNCTIONS[task_row["function"]]
        try:
            args = json.loads(task_row["args_json"])
            result = fn(**args)
            update_result(task_row["id"], "SUCCESS", json.dumps(result), None)
            print(f"[SUCCESS] {task_row['id']} result={result}")
        except Exception as e:
            update_result(task_row["id"], "FAILED", None, str(e))
            print(f"[FAILED] {task_row['id']} error={e}")

def start_worker():
    stop_flag: dict[str, bool] = {"stop": False}
    thread = threading.Thread(target=worker_loop, args=(stop_flag,), daemon=True)
    thread.start()
    return stop_flag, thread

if __name__ == "__main__":
    db_init()

    stop_flag, thread = start_worker()

    enqueue("add_numbers", {"a": 2, "b": 5})
    enqueue("say_hello", {"name": "Satti"})
    enqueue("add_numbers", {"a": 10, "b": 90})

    time.sleep(2)

    print("\nFINAL TASK STATES (from DB):")
    for t in list_tasks():
        print(t)

    stop_flag["stop"] = True
    thread.join(timeout=1)
    print("Done.")