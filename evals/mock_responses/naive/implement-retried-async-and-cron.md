```python
from rustvello import App

app = App(app_id="shop", backend="sqlite", db_path="shop.db")


@app.task
def cleanup() -> int:
    return 0


@app.task(retries=3, autoretry_for=(ConnectionError,), retry_backoff=True, time_limit=10)
async def fetch_status(url: str) -> int:
    return 200


app.schedule(cleanup, cron="*/5 * * * *")
```
