Nothing executes the task: calling `send(...)` only submits it, and no worker is
running for app `mail`, so it stays PENDING until `result(timeout=30)` gives up.

Put the app in a module and run a worker process against the same database:

```bash
python -m rustvello.worker mail_app:app
```

(or call `app.run()` in a long-running process). For tests only,
`App(dev_mode_force_sync=True)` runs tasks inline.
