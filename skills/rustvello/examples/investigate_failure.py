"""Investigate a failed invocation through the monitoring API (/api/capabilities first)."""

import json
import os
import subprocess
import sys
import tempfile
import urllib.request

from rustvello import App

DB = os.path.join(tempfile.mkdtemp(), "shop.db")
app = App(app_id="shop", backend="sqlite", db_path=DB)


@app.task(max_retries=1)
def charge(order_id: str) -> str:
    raise PermissionError(f"card declined for {order_id}")


def get_json(base: str, path: str) -> dict:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        return json.loads(response.read())


if __name__ == "__main__":
    app.run(block=False)
    try:
        invocation = charge("order-7")
        try:
            invocation.result(timeout=60)
        except RuntimeError as error:
            print("failed as expected:", error)  # Task failed: PermissionError: card declined ...
    finally:
        app.stop()

    # 1. Serve the monitoring API for the same backend (port 0 = any free port).
    server = app.start_monitor(host="127.0.0.1", port=0, log_level="warn")
    base = f"http://{server.address}"
    try:
        # 2. Discover the contract: routes, schema_version and the backend's guarantees.
        capabilities = get_json(base, "/api/capabilities")
        assert capabilities["schema_version"] == 1
        route = capabilities["investigation"]["invocation"].replace("{invocation_id}", str(invocation.id))

        # 3. One report joins status, stored error, history, runners and integrity flags.
        report = get_json(base, route)
        statuses = [row["status"] for row in report["history"]]
        print("app:", capabilities["app_id"], "| backend:", capabilities["guarantees"]["active"]["profile"])
        print("status:", report["invocation"]["status"], "| history:", statuses)
        print("error:", report["error"]["error_type"], "-", report["error"]["message"])
        assert report["invocation"]["status"] == "Failed"
        assert report["error"]["error_type"] == "PermissionError"
        assert "Retry" in statuses  # max_retries=1: one retry before the final failure
    finally:
        server.stop()

    # 4. Same report without a running monitor, from the SQLite file (read-only use).
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "investigate.py")
    output = subprocess.run(
        [sys.executable, script, str(invocation.id), "--db-path", DB, "--app-id", "shop"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert json.loads(output)["investigation"]["error"]["error_type"] == "PermissionError"
    print("ok")
