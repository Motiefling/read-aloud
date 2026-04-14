"""
One-command launcher for the Read-Aloud audiobook server.

Starts Redis, Celery worker, and FastAPI server.
Both Uvicorn and the Celery worker auto-reload when Python files change.
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
import threading
import os
from pathlib import Path

procs = []

# Directory containing Python source files to watch
APP_DIR = Path(__file__).parent / "app"


def _kill_tree(proc):
    """Kill a process and all its children (needed on Windows where
    terminate() only kills the parent, leaving child processes orphaned)."""
    import psutil
    try:
        parent = psutil.Process(proc.pid)
        children = parent.children(recursive=True)
        for child in children:
            child.kill()
        parent.kill()
        psutil.wait_procs(children + [parent], timeout=5)
    except psutil.NoSuchProcess:
        pass


def cleanup():
    for name, proc in reversed(procs):
        if proc.poll() is None:
            print(f"Stopping {name}...")
            _kill_tree(proc)


def find_redis():
    """Find redis-server executable."""
    # Check PATH first
    path = shutil.which("redis-server")
    if path:
        return path
    # Common Windows install location
    win_path = r"C:\Program Files\Redis\redis-server.exe"
    if os.path.exists(win_path):
        return win_path
    return None


def start_celery():
    """Start the Celery worker process and return it."""
    proc = subprocess.Popen(
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
    return proc


def _get_py_mtimes():
    """Snapshot modification times for all .py files under app/."""
    mtimes = {}
    for path in APP_DIR.rglob("*.py"):
        try:
            mtimes[path] = path.stat().st_mtime
        except OSError:
            pass
    return mtimes


def celery_reloader(get_proc, set_proc):
    """Background thread: watches .py files and restarts Celery on changes."""
    last_mtimes = _get_py_mtimes()

    while True:
        time.sleep(2)
        current_mtimes = _get_py_mtimes()
        if current_mtimes != last_mtimes:
            changed = (
                set(current_mtimes.items()) ^ set(last_mtimes.items())
            )
            names = {str(p.relative_to(APP_DIR.parent)) for p, _ in changed}
            print(f"\n--- Celery reloading (changed: {', '.join(sorted(names))}) ---")

            old_proc = get_proc()
            if old_proc and old_proc.poll() is None:
                _kill_tree(old_proc)

            new_proc = start_celery()
            set_proc(new_proc)
            last_mtimes = _get_py_mtimes()  # re-snapshot after restart
        else:
            last_mtimes = current_mtimes


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

    # Purge stale tasks from the queue so old jobs don't auto-run on startup
    print("Purging stale task queue...")
    subprocess.run(
        [
            sys.executable, "-m", "celery",
            "-A", "app.pipeline.tasks",
            "purge", "-f",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # 2. Start Celery worker (with auto-reload watcher)
    print("Starting Celery worker (auto-reload enabled)...")
    celery_proc = start_celery()
    procs.append(("Celery", celery_proc))
    time.sleep(2)

    if celery_proc.poll() is not None:
        print("Celery worker failed to start!")
        sys.exit(1)

    # The procs list entry for Celery needs to stay current when the
    # reloader replaces the process, so we use a mutable reference.
    celery_idx = len(procs) - 1

    def get_celery():
        return procs[celery_idx][1]

    def set_celery(new_proc):
        procs[celery_idx] = ("Celery", new_proc)

    watcher = threading.Thread(
        target=celery_reloader, args=(get_celery, set_celery), daemon=True,
    )
    watcher.start()

    # 3. Start FastAPI server (with auto-reload)
    print("Starting FastAPI server (auto-reload enabled)...")
    uvicorn_proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "app.main:app",
            "--host", "0.0.0.0",
            "--port", "8800",
            "--reload",
            "--reload-dir", "app",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    procs.append(("Uvicorn", uvicorn_proc))

    print("\n=== Read-Aloud server is running (auto-reload ON) ===")
    print("  App:    http://localhost:8800")
    print("  API:    http://localhost:8800/docs")
    print("  Python file changes will auto-restart the backend.")
    print("  Frontend changes just need a browser refresh.")
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
