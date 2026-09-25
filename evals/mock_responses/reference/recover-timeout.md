The attempt ran past its per-attempt deadline: `timeout=2` fails any attempt that
takes longer than 2 s with `TaskTimeoutError` (history: Running -> Failed after
2 s, no retry because `max_retries` is 0). `load_rows` takes 3-6 s.

Increase the timeout above the real duration, and allow a retry:

```python
@app.task(timeout=15, max_retries=2, retry_delay=5)
def build_report(day: str) -> str: ...
```

A sync body is abandoned, not stopped, on timeout (its thread keeps running);
use `async def` or `--processes N` if it must really stop.
