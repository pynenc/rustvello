from rustvello import App

# Tasks run inline in the caller: no worker needed. Handy for tests and
# local tries; RUSTVELLO__DEV_MODE_FORCE_SYNC=true does the same without code.
app = App(dev_mode_force_sync=True)


@app.task
def add(x: int, y: int) -> int:
    return x + y


print(add(1, 2).result())  # 3
