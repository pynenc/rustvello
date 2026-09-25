"""BEFORE: a small Celery application (see docs/migrating-from-celery.md).

Durable production settings: RabbitMQ or Redis broker, a result backend,
late acknowledgement. ``python celery_app.py`` runs it end to end with an
in-process worker on Celery's in-memory transport, so no broker is needed.

Production worker and scheduler:
    celery -A celery_app worker -Q default,emails --concurrency 4
    celery -A celery_app beat
"""

from __future__ import annotations

import os

from celery import Celery, chain, chord, group
from celery.schedules import crontab

app = Celery(
    "shop",
    broker=os.environ.get("CELERY_BROKER_URL", "memory://"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "cache+memory://"),
)
app.conf.update(
    task_acks_late=True,  # re-deliver when a worker dies mid-task
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="default",
    task_routes={"celery_app.send_receipt": {"queue": "emails"}},  # routing
    beat_schedule={  # periodic work
        "nightly-report": {"task": "celery_app.nightly_report", "schedule": crontab(minute=0, hour=7)},
    },
)

ATTEMPTS: dict[str, int] = {}


@app.task
def add(x: int, y: int) -> int:
    return x + y


@app.task(
    bind=True,
    autoretry_for=(ConnectionError,),
    max_retries=3,
    retry_backoff=True,  # 1 s, 2 s, 4 s ... capped at retry_backoff_max (600 s), full jitter
)
def fetch_price(self, sku: str) -> int:
    ATTEMPTS[sku] = ATTEMPTS.get(sku, 0) + 1
    if ATTEMPTS[sku] == 1:
        raise ConnectionError("pricing service unavailable")  # retried
    return 100 + len(sku)


@app.task(time_limit=30)
def send_receipt(amount: int, order_id: str) -> str:
    return f"receipt for {order_id}: {amount}"


@app.task
def total(prices: list[int]) -> int:
    return sum(prices)


@app.task
def nightly_report() -> str:
    return "report"


def checkout(order_id: str, skus: list[str]) -> str:
    """Canvas: price every SKU in parallel (chord), then send the receipt (chain)."""
    workflow = chain(
        chord(group(fetch_price.s(sku) for sku in skus), total.s()),
        send_receipt.s(order_id).set(queue="emails"),
    )
    # in a chain, send_receipt receives the total as its first argument
    return workflow.apply_async().get(timeout=30)


if __name__ == "__main__":
    from celery.contrib.testing.worker import start_worker

    app.conf.update(task_retry_backoff_max=1)  # keep the demo fast
    fetch_price.retry_backoff_max = 1
    with start_worker(app, pool="solo", perform_ping_check=False, queues=["default", "emails"]):
        assert add.delay(1, 2).get(timeout=30) == 3
        receipt = checkout("order-1", ["apple", "kiwi"])
        print(receipt)
        assert receipt.startswith("receipt for")
    print("celery example ok")
