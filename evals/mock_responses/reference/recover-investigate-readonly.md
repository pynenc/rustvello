```bash
rustvello investigate 3f1c6a2e-8d4b-4c47-9a53-2f0e7b1d9c10 --app-id shop --db-path ./shop.db --format json
rustvello status 3f1c6a2e-8d4b-4c47-9a53-2f0e7b1d9c10 --db-path ./shop.db
```

Without the CLI: `python scripts/investigate.py 3f1c6a2e-8d4b-4c47-9a53-2f0e7b1d9c10 --db-path ./shop.db --app-id shop`
(it serves `/api/capabilities` and `/invocations/<id>/investigation` locally and
only reads). Read `error.error_type`, then the history for retries and runners.
