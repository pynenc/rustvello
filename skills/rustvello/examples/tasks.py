"""An application module: the worker process and the producers import the same file."""

import os

from rustvello import App

# Every process must open the same SQLite file and use the same app_id.
app = App(
    app_id="orders",
    backend="sqlite",
    db_path=os.environ.get("ORDERS_DB", "./orders.db"),
)


@app.task(max_retries=3, retry_for=(ConnectionError,), retry_delay=0.5)
def process_order(order_id: str) -> str:
    return f"processed {order_id}"
