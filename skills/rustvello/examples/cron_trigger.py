"""Schedule a task with a cron trigger; a running worker fires it."""

import os
import tempfile
import time

from rustvello import App

WORKDIR = tempfile.mkdtemp()
app = App(app_id="cron", backend="sqlite", db_path=os.path.join(WORKDIR, "cron.db"))


@app.task
def write_report(kind: str) -> str:
    path = os.path.join(WORKDIR, f"{kind}-{time.time_ns()}.txt")
    with open(path, "w") as report:
        report.write(kind)
    return path


# register() stores the trigger in the backend: every worker sharing it sees the
# trigger, and each slot fires once. Registering it again is a no-op.
# 6 fields = seconds first ("*/2" = every 2 s); 5 fields = standard minute cron,
# e.g. app.trigger(write_report).on_cron("*/5 * * * *") for every 5 minutes.
app.trigger(write_report).on_cron("*/2 * * * * *").with_args(kind="daily").register()

if __name__ == "__main__":
    app.run(block=False)  # the worker evaluates triggers every few seconds
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            reports = [name for name in os.listdir(WORKDIR) if name.startswith("daily-")]
            if len(reports) >= 2:
                break
            time.sleep(0.5)
        assert len(reports) >= 2, "the cron trigger did not fire twice within 60 s"
        print("ok", sorted(reports))
    finally:
        app.stop()
