"""Print the backend guarantee matrix, or the backends that guarantee what you need.

Usage::

    python guarantees.py                                   # the whole matrix
    python guarantees.py --need delayed_retry durability   # backends that guarantee both
    python guarantees.py --url http://127.0.0.1:8000       # from a running monitor

The matrix is the one served at ``/api/capabilities`` (``guarantees.matrix``) by
the installed version; without ``--url`` it starts a throwaway in-memory monitor.
"""

import argparse
import json
import sys
import urllib.request


def fetch_matrix(base: str) -> list:
    with urllib.request.urlopen(base.rstrip("/") + "/api/capabilities", timeout=30) as response:
        return json.loads(response.read())["guarantees"]["matrix"]


def level(row: dict, guarantee: str) -> str:
    cell = row[guarantee]
    return cell["level"] if isinstance(cell, dict) else str(cell)


def main() -> int:
    parser = argparse.ArgumentParser(description="Show which backend guarantees what.")
    parser.add_argument("--need", nargs="*", default=[], help="guarantees that must be 'guaranteed'")
    parser.add_argument("--url", help="base URL of a running monitor")
    args = parser.parse_args()

    server = None
    base = args.url
    if base is None:
        from rustvello import App

        server = App(app_id="guarantees").start_monitor(host="127.0.0.1", port=0, log_level="warn")
        base = f"http://{server.address}"
    try:
        matrix = fetch_matrix(base)
    finally:
        if server is not None:
            server.stop()

    columns = [key for key in matrix[0] if key != "backend"]
    unknown = [n for n in args.need if n not in columns]
    if unknown:
        print(f"unknown guarantee(s) {unknown}; choose from {columns}", file=sys.stderr)
        return 2
    rows = [row for row in matrix if all(level(row, name) == "guaranteed" for name in args.need)]
    print(("backend    " + "  ".join(f"{c:<20}" for c in columns)).rstrip())
    for row in rows:
        print((f"{row['backend']:<10} " + "  ".join(f"{level(row, c):<20}" for c in columns)).rstrip())
    if args.need and not rows:
        print("no backend guarantees all of", args.need)
    return 0


if __name__ == "__main__":
    sys.exit(main())
