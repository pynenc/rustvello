"""The benchmark's tasks, defined once per system. ``BENCH_SYSTEM`` selects the system.

Worker processes import this module:

* Rustvello: ``python -m rustvello.worker bench_tasks:app --workers 4``
* Celery:    ``celery -A bench_tasks worker --concurrency 4 --pool prefork``

Both sides get the same durability: every accepted task and every result is
written to storage that fsyncs (see README.md, "Equal durability").
"""

from __future__ import annotations

import os
import pathlib
import time

SYSTEM = os.environ.get("BENCH_SYSTEM", "rustvello-sqlite")
# Seconds without a heartbeat (Rustvello) or acknowledgement (Celery on Redis)
# before a running task is handed to another worker.
RECOVER_AFTER = int(os.environ.get("BENCH_RECOVER_AFTER", "5"))


def _stamp(sent: float) -> float:
    return time.time()


def _slow(seconds: float, marker: str) -> float:
    with pathlib.Path(marker).open("a") as f:  # one line per execution: at-least-once is visible
        f.write(f"{os.getpid()} {time.time()}\n")
        f.flush()
        os.fsync(f.fileno())
    time.sleep(seconds)
    return time.time()


if SYSTEM.startswith("rustvello"):
    from rustvello import App, AppConfig

    config = AppConfig(
        app_id="bench",
        heartbeat_interval_seconds=1,
        runner_dead_after_seconds=RECOVER_AFTER,
        recovery_check_interval_seconds=1,
    )
    if SYSTEM == "rustvello-sqlite":
        app = App(
            app_id="bench",
            backend="sqlite",
            db_path=os.environ["BENCH_SQLITE_PATH"],
            sqlite_synchronous="FULL",
            config=config,
        )
    elif SYSTEM == "rustvello-postgres":
        app = App(app_id="bench", backend="postgres", postgres_url=os.environ["BENCH_POSTGRES_URL"], config=config)
    else:
        raise ValueError(f"unknown system {SYSTEM}")

    stamp = app.task(_stamp)
    slow = app.task(_slow)

elif SYSTEM.startswith("celery"):
    from celery import Celery

    if SYSTEM == "celery-redis":
        broker = backend = os.environ["BENCH_REDIS_URL"]
        transport = {"visibility_timeout": RECOVER_AFTER}
    elif SYSTEM == "celery-rabbitmq":
        broker = os.environ["BENCH_AMQP_URL"]
        backend = "db+" + os.environ["BENCH_CELERY_DB_URL"]
        transport = {"confirm_publish": True}
    else:
        raise ValueError(f"unknown system {SYSTEM}")

    app = Celery("bench_tasks", broker=broker, backend=backend)
    app.conf.update(
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        broker_transport_options=transport,
        result_backend_transport_options={"visibility_timeout": RECOVER_AFTER},
        broker_connection_retry_on_startup=True,
        worker_hijack_root_logger=False,
        task_serializer="json",
        result_serializer="json",
    )
    stamp = app.task(_stamp, name="bench_tasks.stamp")
    slow = app.task(_slow, name="bench_tasks.slow")

else:
    raise ValueError(f"unknown system {SYSTEM}")
