"""``python -m rustvello.monitor package.module:app``: serve the monitoring dashboard until SIGTERM.

The equivalent of ``python -m rustvello.worker`` for the monitor: imports the app,
checks its backends (a refused credential exits non-zero with the backend's
error), serves the dashboard and stops on SIGTERM or Ctrl-C, so a container or
a Kubernetes pod running it shuts down cleanly.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from rustvello.worker import load_app

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m rustvello.monitor", description="Serve the rustvello monitoring dashboard"
    )
    parser.add_argument("app", help="import path of the App, e.g. package.module:app")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (0.0.0.0 inside a container)")
    parser.add_argument("--port", type=int, default=8000, help="bind port; 0 picks a free port")
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="refuse purge, rerun and cancel (also RUSTVELLO_MONITORING_READ_ONLY=1)",
    )
    parser.add_argument(
        "--no-check", action="store_true", help="serve even when a backend refuses the monitor's credentials"
    )
    parser.add_argument(
        "--loglevel",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="Python log level",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.loglevel.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        app = load_app(args.app)
        server = app.start_monitor(
            host=args.host, port=args.port, read_only=args.read_only, check_backends=not args.no_check
        )
    except Exception as exc:  # noqa: BLE001 - a start-up failure is the exit status, with its message
        logger.error("monitor did not start: %s", exc)
        return 1
    logger.info(
        "monitoring %s at %s (read_only=%s); SIGTERM or Ctrl-C stops it", app._app_id, server.url, server.read_only
    )
    from rustvello.app import _serve_until_signal

    _serve_until_signal(server)
    logger.info("monitor stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
