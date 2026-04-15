"""
Launches a Celery worker that connects to the remote Redis broker.

Uses config.worker.yaml (via READ_ALOUD_CONFIG env var) so the worker
picks up tasks from the server's queue while running GPU jobs locally.

Usage:
    python start_worker.py
"""

import subprocess
import sys
import signal
import os
import atexit

os.environ["READ_ALOUD_CONFIG"] = "config.worker.yaml"

proc = None


def _kill_tree(p):
    """Kill a process and all its children."""
    import psutil
    try:
        parent = psutil.Process(p.pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
        psutil.wait_procs(parent.children(recursive=True) + [parent], timeout=5)
    except psutil.NoSuchProcess:
        pass


def cleanup():
    global proc
    if proc and proc.poll() is None:
        print("Stopping Celery worker...")
        _kill_tree(proc)


def main():
    global proc

    atexit.register(cleanup)
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

    # Load config to display broker URL dynamically
    import yaml
    with open("config.worker.yaml") as f:
        worker_cfg = yaml.safe_load(f)
    broker_url = worker_cfg.get("celery", {}).get("broker_url", "unknown")

    print("Starting Celery worker (config: config.worker.yaml)...")
    print(f"  Broker: {broker_url}")

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

    print("Celery worker is running. Press Ctrl+C to stop.\n")

    try:
        proc.wait()
    except SystemExit:
        pass

    sys.exit(proc.returncode or 0)


if __name__ == "__main__":
    main()
