```bash
python -m venv .venv
. .venv/bin/activate
pip install "rustvello>=0.8,<0.9"
```

```python
import os
import tempfile

from rustvello import App

app = App(app_id="demo", backend="sqlite", db_path=os.path.join(tempfile.mkdtemp(), "tasks.db"))


@app.task
def add(x: int, y: int) -> int:
    return x + y


if __name__ == "__main__":
    app.run(block=False)
    try:
        print(add(2, 3).result(timeout=30))
    finally:
        app.stop()
```
