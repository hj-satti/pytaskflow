import time
import uuid
import threading

# Simple in-memory task queue
TASK_QUEUE = []
# Function registry
FUNCTIONS = {}

def task(fn):
    FUNCTIONS[fn.__name__] = fn
    return fn

@task
def add_numbers(a, b):
    return a + b

@task
def say_hello(name):
    return f"Hello, {name}!"

def enqueue(function_name, args: dict):
    if function_name not in FUNCTIONS:
        raise ValueError("Function not registered: " + function_name)
    task_id = uuid.uuid4().hex
    TASK_QUEUE.append({
        "id": task_id,
        "function": function_name,
        "args": args,
        "status": "PENDING",
        "result": None,
        "error": None,
    })
    print(f"[ENQUEUE] id={task_id} func={function_name} args={args}")
    return task_id

def worker_loop(stop_flag):
    while not stop_flag["stop"]:
        task = None
        for t in TASK_QUEUE:
            if t["status"] == "PENDING":
                task = t
                break
        if not task:
            time.sleep(0.2)
            continue
        task["status"] = "RUNNING"
        fn = FUNCTIONS[task["function"]]
        try:
            result = fn(**task["args"])
            task["result"] = result
            task["status"] = "SUCCESS"
            print(f"[SUCCESS] {task['id']} result={result}")
        except Exception as e:
            task["error"] = str(e)
            task["status"] = "FAILED"
            print(f"[FAILED] {task['id']} error={e}")

def start_worker():
    stop_flag = {"stop": False}
    thread = threading.Thread(target=worker_loop, args=(stop_flag,), daemon=True)
    thread.start()
    return stop_flag, thread

if __name__ == "__main__":
    stop_flag, thread = start_worker()

    enqueue("add_numbers", {"a": 2, "b": 5})
    enqueue("say_hello", {"name": "Satti"})
    enqueue("add_numbers", {"a": 10, "b": 90})

    time.sleep(2)

    print("\nFINAL TASK STATES:")
    for t in TASK_QUEUE:
        print(t)

    stop_flag["stop"] = True
    thread.join(timeout=1)
    print("Done.")