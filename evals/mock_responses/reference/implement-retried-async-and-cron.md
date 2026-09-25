```python
import asyncio

from rustvello import App

app = App(app_id="shop", backend="sqlite", db_path="shop.db")


@app.task
def cleanup() -> int:
    return 0


@app.task(max_retries=3, retry_for=(ConnectionError,), retry_delay=1, retry_backoff=2, timeout=10)
async def fetch_status(url: str) -> int:
    await asyncio.sleep(0.1)
    return 200


app.trigger(cleanup).on_cron("*/5 * * * *").register()
```
