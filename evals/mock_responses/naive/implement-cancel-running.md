```python
from rustvello import App

app = App()


@app.task
def nap():
    import time
    time.sleep(60)


result = nap.apply_async()
result.revoke(terminate=True)
print("cancelled")
```
