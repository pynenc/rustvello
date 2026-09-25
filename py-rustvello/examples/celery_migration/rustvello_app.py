"""AFTER: the Celery application of celery_app.py on Rustvello.

SQLite (or PostgreSQL) holds queue, state and results in one durable store, so
there is no separate broker or result backend to run. ``python rustvello_app.py``
runs it end to end with a worker thread and a temporary database.

Production worker (it also evaluates the cron trigger, so there is no beat process):
    python -m rustvello.worker rustvello_app:app --queues default emails --workers 4
"""

from __future__ import annotations

import os
import tempfile

from rustvello import App, AppConfig

APP_ID = "shop"
app = App(
    app_id=APP_ID,
    backend="sqlite",
    db_path=os.environ.get("SHOP_DB", os.path.join(tempfile.mkdtemp(), "shop.db")),
    config=AppConfig(app_id=APP_ID, broker_queues=["default", "emails"]),  # routing: declared queues
)

ATTEMPTS: dict[str, int] = {}


@app.task
def add(x: int, y: int) -> int:
    return x + y


@app.task(
    max_retries=3,
    retry_for=(ConnectionError,),  # Celery autoretry_for
    retry_delay=1.0,  # Celery retry_backoff=True: 1 s, 2 s, 4 s ...
    retry_backoff=2.0,
    retry_max_delay=600.0,  # Celery retry_backoff_max
    retry_jitter="full",  # Celery retry_jitter=True
)
def fetch_price(sku: str) -> int:
    ATTEMPTS[sku] = ATTEMPTS.get(sku, 0) + 1
    if ATTEMPTS[sku] == 1:
        raise ConnectionError("pricing service unavailable")  # retried
    return 100 + len(sku)


@app.task(queue="emails", timeout=30)  # Celery task_routes + time_limit
def send_receipt(amount: int, order_id: str) -> str:
    return f"receipt for {order_id}: {amount}"


@app.task
def nightly_report() -> str:
    return "report"


# Celery beat crontab(minute=0, hour=7); Rustvello cron has a leading seconds field.
app.trigger(nightly_report).on_cron("0 0 7 * * *").register()


@app.workflow
def checkout(order_id: str, skus: list[str]) -> str:
    """Celery canvas chain(chord(group(...), total), send_receipt) as plain code."""
    prices = app.wait_results([fetch_price(sku) for sku in skus], timeout=60)  # group
    amount = sum(prices)  # chord callback
    return send_receipt(amount, order_id).result(timeout=30)  # chain


if __name__ == "__main__":
    app.run(num_workers=4, queues=["default", "emails"], block=False)
    try:
        assert add(1, 2).result(timeout=30) == 3
        receipt = checkout("order-1", ["apple", "kiwi"]).result(timeout=60)
        print(receipt)
        assert receipt == "receipt for order-1: 209"
    finally:
        app.stop()
    print("rustvello example ok")
