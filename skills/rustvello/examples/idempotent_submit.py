"""Submit at most once per request with an idempotency key, and key a side effect."""

import os
import tempfile

from rustvello import App, InvocationId

app = App(app_id="idempotent", backend="sqlite", db_path=os.path.join(tempfile.mkdtemp(), "orders.db"))
charged: dict[str, int] = {}  # stands in for a payment API that deduplicates by key


@app.task(max_retries=2)
def place_order(order: str, amount: int) -> str:
    # The body runs at least once: key the external effect by the invocation id,
    # which stays the same across retries and recovery.
    effect_key = f"{app.current_invocation().invocation_id}:charge"
    charged.setdefault(effect_key, amount)
    return f"{order} charged {amount}"


if __name__ == "__main__":
    request_id = "request-42"  # e.g. the client's request id or the order id
    first = place_order.submit_with_key(request_id, order="A-1", amount=10)
    again = place_order.submit_with_key(request_id, order="A-1", amount=10)  # a client retry
    assert str(again.id) == str(first.id)  # same key and arguments: the same invocation
    assert str(first.id) == str(InvocationId.from_key("python::__main__.place_order", request_id))

    app.run(block=False)
    try:
        assert first.result(timeout=30) == "A-1 charged 10"
        assert len(charged) == 1
        print("ok", first.id)
    finally:
        app.stop()
