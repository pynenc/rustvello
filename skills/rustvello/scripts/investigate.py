"""Print the investigation report of one invocation as JSON (read-only).

Usage::

    python investigate.py INVOCATION_ID --db-path ./tasks.db --app-id my-app
    python investigate.py INVOCATION_ID --url http://127.0.0.1:8000   # a running monitor

Needs only the ``rustvello`` wheel: with ``--db-path`` it serves the monitoring
API for that SQLite file on a free local port for the duration of the call
(nothing is written), then queries ``/api/capabilities`` and
``/invocations/<id>/investigation``. The Rust CLI does the same without a
server: ``rustvello investigate <id> --app-id <app> --db-path <db> --format json``.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request


def get_json(base: str, path: str) -> dict:
    with urllib.request.urlopen(base.rstrip("/") + path, timeout=30) as response:
        return json.loads(response.read())


def investigate(invocation_id: str, base: str) -> dict:
    capabilities = get_json(base, "/api/capabilities")
    route = capabilities["investigation"]["invocation"].replace("{invocation_id}", invocation_id)
    return {
        "app_id": capabilities["app_id"],
        "backend_guarantees": capabilities["guarantees"]["active"],
        "investigation": get_json(base, route),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Investigate one Rustvello invocation (read-only).")
    parser.add_argument("invocation_id")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--db-path", help="SQLite database of the app")
    source.add_argument("--url", help="base URL of a running monitor")
    parser.add_argument("--app-id", default="rustvello", help="app_id used by the app (default: rustvello)")
    args = parser.parse_args()

    server = None
    base = args.url
    if args.db_path:
        from rustvello import App

        app = App(app_id=args.app_id, backend="sqlite", db_path=args.db_path)
        server = app.start_monitor(host="127.0.0.1", port=0, log_level="warn")
        base = f"http://{server.address}"
    try:
        report = investigate(args.invocation_id, base)
    except urllib.error.HTTPError as error:
        print(f"HTTP {error.code} for invocation {args.invocation_id}: check the id and --app-id", file=sys.stderr)
        return 1
    finally:
        if server is not None:
            server.stop()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
