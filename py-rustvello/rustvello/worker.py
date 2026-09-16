"""Worker command line and worker-process protocol for :meth:`rustvello.App.run`.

Two entry points share this module:

* ``python -m rustvello.worker package.module:app [--processes N] [--queues q1 q2]``
  imports the application and starts a persistent runner. This replaces
  ``pynenc runner`` / ``celery worker`` style launchers.
* ``python -m rustvello.worker --child --app package.module:app`` is what the Rust
  process pool starts for every worker slot. The child speaks a JSON-lines protocol on
  stdin/stdout (see ``crates/rustvello/src/runner/executor/subprocess.rs``): it prints a
  ready line after importing the app, then answers one request per line. Anything the
  task prints goes to stderr so the protocol stream stays clean.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sys
import traceback
from collections.abc import Sequence
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rustvello.app import App

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = 1


def load_app(import_path: str) -> App:
    """Import ``module:attribute`` (or ``module.attribute``) and return the :class:`App`."""
    from rustvello.app import App

    module_name, _, attribute = import_path.partition(":")
    if not attribute:
        module_name, _, attribute = import_path.rpartition(".")
        if not module_name:
            raise ValueError(f"import path {import_path!r} must look like 'package.module:app'")
    module = importlib.import_module(module_name)
    app = getattr(module, attribute, None)
    if not isinstance(app, App):
        raise TypeError(f"{import_path!r} is {type(app).__name__}, expected rustvello.App")
    if app._import_path is None:
        app._import_path = f"{module_name}:{attribute}"
    return app


def _error_response(exc: BaseException) -> dict[str, Any]:
    return {
        "ok": False,
        "error_type": type(exc).__name__,
        "message": str(exc),
        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    }


def execute_request(app: App, request: dict[str, Any]) -> dict[str, Any]:
    """Run one task request from the Rust executor and build the response line."""
    from rustvello.app import _run_python_task
    from rustvello.rustvello import clear_current_invocation_context, set_current_invocation_context

    if request.get("protocol") != PROTOCOL_VERSION:
        return _error_response(ValueError(f"unsupported worker protocol {request.get('protocol')!r}"))
    try:
        handle = app.get_task(f"{request.get('language', 'python')}::{request['module']}.{request['name']}")
        set_current_invocation_context(
            request["invocation_id"],
            request["module"],
            request["name"],
            num_retries=int(request.get("num_retries") or 0),
            language=request.get("language") or "python",
            parent_invocation_id=request.get("parent_invocation_id"),
            traceparent=request.get("traceparent"),
            tracestate=request.get("tracestate"),
        )
        try:
            result = _run_python_task(handle._func, json.dumps(request.get("args") or {}))
        finally:
            clear_current_invocation_context()
    except BaseException as exc:  # noqa: BLE001 - every failure must reach the executor as a response
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _error_response(exc)
    return {"ok": True, "result": result}


def serve(app: App, requests: IO[str], responses: IO[str]) -> None:
    """Answer requests line by line until stdin closes."""
    responses.write(json.dumps({"ready": True, "pid": os.getpid(), "protocol": PROTOCOL_VERSION}) + "\n")
    responses.flush()
    for line in requests:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError as exc:
            response = _error_response(exc)
        else:
            response = execute_request(app, request)
        responses.write(json.dumps(response) + "\n")
        responses.flush()


def run_child(import_path: str) -> int:
    """Body of a pool worker: import the app, then serve requests from stdin."""
    # Keep the protocol stream private: task code printing to stdout must not corrupt it.
    protocol_out = os.fdopen(os.dup(sys.stdout.fileno()), "w", buffering=1)
    sys.stdout = sys.stderr
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        app = load_app(import_path)
    except Exception:  # noqa: BLE001 - the executor reports a missing ready line as a configuration error
        traceback.print_exc(file=sys.stderr)
        return 1
    serve(app, sys.stdin, protocol_out)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m rustvello.worker", description="Run a rustvello worker")
    parser.add_argument("app", nargs="?", help="import path of the App, e.g. package.module:app")
    parser.add_argument("--processes", type=int, default=None, help="worker processes (one interpreter each)")
    parser.add_argument("--workers", type=int, default=4, help="in-process worker slots when --processes is not set")
    parser.add_argument("--queues", nargs="*", default=None, help="broker queues this runner consumes")
    parser.add_argument("--idle-sleep-ms", type=int, default=50, help="sleep when no work is available")
    parser.add_argument("--no-triggers", action="store_true", help="do not evaluate trigger conditions here")
    parser.add_argument(
        "--loglevel",
        choices=["debug", "info", "warning", "error", "critical"],
        default=None,
        help="Python and runner log level",
    )
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--app", dest="child_app", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.child:
        return run_child(args.child_app or args.app)
    if not args.app:
        build_parser().error("the app import path is required")
    if args.loglevel:
        logging.basicConfig(level=args.loglevel.upper())
    app = load_app(args.app)
    if args.loglevel:
        app._config.logging_level = args.loglevel
    logger.info("starting rustvello runner for %s (%s)", args.app, "processes" if args.processes else "threads")
    app.run(
        num_workers=args.workers,
        num_processes=args.processes,
        queues=args.queues,
        idle_sleep_ms=args.idle_sleep_ms,
        evaluate_triggers=not args.no_triggers,
        import_path=args.app,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
