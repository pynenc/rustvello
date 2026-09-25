```bash
pip install rustvello
```

```python
from rustvello import Rustvello, task

app = Rustvello("sqlite:///tasks.db")


@app.task(retries=3)
def add(x, y):
    return x + y


print(add.delay(2, 3).get(timeout=10))
```
