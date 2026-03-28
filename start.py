"""
One-command launcher for the Read-Aloud audiobook server.

Starts Redis, Celery worker, and FastAPI server.
Ctrl+C shuts everything down cleanly.

Usage:
    python start.py
"""

import subprocess
import sys
import signal
import time
import shutil
import atexit

procs = []


def cleanup():
    for name, proc in reversed(procs):
        if proc.poll() is None:
            print(f"Stopping {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def find_redis():
    """Find redis-server executable."""
    # Check PATH first
    path = shutil.which("redis-server")
    if path:
        return path
    # Common Windows install location
    win_path = r"C:\Program Files\Redis\redis-server.exe"
    import os
    if os.path.exists(win_path):
        return win_path
    return None


def main():
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

    # 1. Start Redis
    redis_path = find_redis()
    if not redis_path:
        print("ERROR: redis-server not found. Install with: winget install Redis.Redis")
        sys.exit(1)

    print(f"Starting Redis ({redis_path})...")
    redis_proc = subprocess.Popen(
        [redis_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    procs.append(("Redis", redis_proc))
    time.sleep(1)

    if redis_proc.poll() is not None:
        print("Redis failed to start! It may already be running (which is fine).")
        procs.pop()
    else:
        print("Redis is running on port 6379.")

    # 2. Start Celery worker
    print("Starting Celery worker...")
    celery_proc = subprocess.Popen(
        [
            sys.executable, "-m", "celery",
            "-A", "app.pipeline.tasks",
            "worker",
            "--loglevel=info",
            "--pool=solo",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    procs.append(("Celery", celery_proc))
    time.sleep(2)

    if celery_proc.poll() is not None:
        print("Celery worker failed to start!")
        sys.exit(1)

    # 3. Start FastAPI server
    print("Starting FastAPI server...")
    uvicorn_proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "app.main:app",
            "--host", "0.0.0.0",
            "--port", "8800",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    procs.append(("Uvicorn", uvicorn_proc))

    print("\n=== Read-Aloud server is running ===")
    print("  App:    http://localhost:8800")
    print("  API:    http://localhost:8800/docs")
    print("  Press Ctrl+C to stop everything.\n")

    # Wait for either process to exit
    while True:
        for name, proc in procs:
            if proc.poll() is not None:
                print(f"{name} exited with code {proc.returncode}")
                sys.exit(proc.returncode or 1)
        time.sleep(1)


if __name__ == "__main__":
    main()
