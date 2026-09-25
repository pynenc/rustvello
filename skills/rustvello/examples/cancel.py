"""Cancel a queued invocation and a running one."""

import asyncio
import os
import tempfile
import time

from rustvello import App, InvocationCancelledError

app = App(app_id="cancel", backend="sqlite", db_path=os.path.join(tempfile.mkdtemp(), "cancel.db"))


@app.task
async def long_job(seconds: float) -> str:
    await asyncio.sleep(seconds)  # a cancelled async body stops at its next await
    return "finished"


if __name__ == "__main__":
    queued = long_job(60)
    assert queued.cancel() is True  # no worker yet: it never runs
    assert str(queued.status) == "CANCELLED"

    app.run(block=False)
    try:
        running = long_job(60)
        while str(running.status) != "RUNNING":
            time.sleep(0.05)
        assert app.cancel(running) is True  # the worker abandons the attempt within ~1 s
        try:
            running.result(timeout=30)
            raise AssertionError("expected InvocationCancelledError")
        except InvocationCancelledError as error:
            print("cancelled:", error)

        done = long_job(0)
        assert done.result(timeout=30) == "finished"
        assert done.cancel() is False  # already final: nothing changes
        print("ok")
    finally:
        app.stop()
