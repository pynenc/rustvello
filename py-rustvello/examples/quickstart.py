from rustvello import App, workflow_root

app = App(backend="sqlite", db_path="./tasks.db")


@app.task(max_retries=2)
def add(x: int, y: int) -> int:
    return x + y


@app.workflow
def process_order(order_id: str) -> dict[str, str]:
    root = workflow_root()  # deterministic helpers, recorded for replay
    return {"order_id": order_id, "run_id": root.uuid()}


if __name__ == "__main__":
    # A worker executes what you submit. In production it is its own process:
    #   python -m rustvello.worker my_module:app
    # Here it runs in a background thread of this script.
    app.run(block=False)
    try:
        print(add(1, 2).result(timeout=30))  # 3
        print(process_order("order-1").result(timeout=30))
    finally:
        app.stop()
