```python
import asyncio
import os
import tempfile
import time

from rustvello import App, InvocationCancelledError

app = App(app_id="cancel", backend="sqlite", db_path=os.path.join(tempfile.mkdtemp(), "c.db"))


@app.task
async def nap() -> str:
    await asyncio.sleep(60)
    return "done"


if __name__ == "__main__":
    app.run(block=False)
    try:
        inv = nap()
        while str(inv.status) != "RUNNING":
            time.sleep(0.05)
        inv.cancel()
        try:
            inv.result(timeout=30)
        except InvocationCancelledError:
            print("cancelled")
    finally:
        app.stop()
```
