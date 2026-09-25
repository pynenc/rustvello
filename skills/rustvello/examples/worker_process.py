"""Run a worker as its own process (the production shape), submit from another, stop it."""

import os
import signal
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
os.environ["ORDERS_DB"] = os.path.join(tempfile.mkdtemp(), "orders.db")
sys.path.insert(0, HERE)

from tasks import process_order  # noqa: E402  (the same module the worker imports)

if __name__ == "__main__":
    # `python -m rustvello.worker module:attribute` imports the app and runs it until
    # SIGTERM/SIGINT. Add `--processes N` for CPU-bound Python tasks.
    worker = subprocess.Popen(
        [sys.executable, "-m", "rustvello.worker", "tasks:app"],
        cwd=HERE,
        env={**os.environ, "PYTHONPATH": HERE},
    )
    try:
        invocations = [process_order(f"order-{i}") for i in range(3)]
        results = [inv.result(timeout=60) for inv in invocations]
        assert results == ["processed order-0", "processed order-1", "processed order-2"]
        print("ok", results)
    finally:
        worker.send_signal(signal.SIGTERM)  # graceful: running tasks finish first
        worker.wait(timeout=60)
