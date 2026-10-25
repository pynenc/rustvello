"""``python -m rustvello.doctor package.module:app``: probe the app's backends and exit non-zero on problems.

Prints the same report as the monitor's ``/health`` (:meth:`rustvello.App.health`):
every backend's status and implementation, active runners, queue depths and the
list of problems. Exit codes:

* ``0``: every backend answered and at least one runner is alive;
* ``1``: a backend refused a read (wrong credentials, missing permission, unreachable);
* ``2``: backends are fine but no runner has sent a heartbeat recently
  (``--allow-no-runners`` turns this into ``0``, for a producer-only process).

Use it as a Kubernetes readiness or liveness probe for a worker image, or run it
first when "the monitor shows 0 runners".
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

from rustvello.worker import load_app

EXIT_OK = 0
EXIT_BACKEND_ERROR = 1
EXIT_NO_RUNNERS = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m rustvello.doctor",
        description="Probe a rustvello app's backends and runners; exit non-zero on problems",
    )
    parser.add_argument("app", help="import path of the App, e.g. package.module:app")
    parser.add_argument("--json", action="store_true", help="print the report as JSON (one object)")
    parser.add_argument(
        "--allow-no-runners",
        action="store_true",
        help="exit 0 when the backends answer but no runner is alive (producer-only processes)",
    )
    return parser


def exit_code(report: dict[str, Any], *, allow_no_runners: bool = False) -> int:
    """Exit code for a report from :meth:`rustvello.App.health`."""
    if report.get("status") != "ok":
        return EXIT_BACKEND_ERROR
    if not allow_no_runners and int(report.get("runners", {}).get("active", 0)) == 0:
        return EXIT_NO_RUNNERS
    return EXIT_OK


def format_text(report: dict[str, Any]) -> str:
    """Human-readable report, one line per component."""
    lines = [f"app {report.get('app_id')}: {report.get('status')}"]
    for name, component in sorted(report.get("components", {}).items()):
        line = f"  {name:<18} {component.get('status'):<6} {component.get('implementation')}"
        if component.get("error"):
            line += f"  {component['error']}"
        lines.append(line)
    runners = report.get("runners", {})
    lines.append(
        f"  runners            {runners.get('active', 0)} active (dead after {runners.get('dead_after_seconds')}s)"
    )
    queues = report.get("queues", {})
    if queues:
        depths = ", ".join(f"{queue}={depth}" for queue, depth in sorted(queues.items()))
        lines.append(f"  queues             {depths}")
    for problem in report.get("problems", []):
        lines.append(f"  problem: {problem}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        app = load_app(args.app)
        report = app.health()
    except Exception as exc:  # noqa: BLE001 - every failure is a probe result, not a crash
        report = {
            "status": "error",
            "app_id": args.app,
            "components": {},
            "runners": {"active": 0},
            "queues": {},
            "problems": [f"{type(exc).__name__}: {exc}"],
        }
    print(json.dumps(report) if args.json else format_text(report))
    return exit_code(report, allow_no_runners=args.allow_no_runners)


if __name__ == "__main__":
    sys.exit(main())
