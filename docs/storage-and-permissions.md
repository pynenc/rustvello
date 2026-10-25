# Storage layout and permissions

What each backend creates for an application, the name it uses, and the
minimal rights the credentials need. Read this before provisioning a
database or a message broker for Rustvello, and when a runner starts and
exits with "refused the runner context write".

## What fails when a permission is missing

Every process checks its backends at start-up:

- A runner (`App.run`, `python -m rustvello.worker`, `rustvello run`) writes
  its runner context, reads it back, writes a heartbeat, reads the active
  runners and reads the queue depths. The first refused action stops the
  process with a non-zero exit code and a message that names the backend,
  the action and the backend's own error (for MongoDB, the database name is
  in that error: `not authorized on rustvello_shop`).
- The monitor (`App.start_monitor`, `python -m rustvello.monitor`) reads
  every component before serving. A refused read raises `RuntimeError`
  (`check_backends=False` serves anyway). While serving, `GET /health`
  answers 503 with the failing component as soon as a read fails.
- Background work that keeps failing (heartbeats) is logged at `ERROR`, once
  at the first failure and then at most once per minute with a count, and a
  recovery is logged at `INFO`.

`python -m rustvello.doctor module:app` runs the same checks on demand and
exits non-zero on a refused read or when no runner is alive
([CLI](cli/index.md)).

## MongoDB (`backend="mongo"` and `backend="mongo3"`)

| Setting            | Value                                                                                                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Database name      | `<mongo_db>_<app_id>` (default `mongo_db="rustvello"`)                                                                                                                                                                               |
| Exact name instead | `App(mongo_db="ops_tasks", mongo_db_verbatim=True)`                                                                                                                                                                                  |
| Collections        | `broker_queue`, `orch_*` (status, retries, auto purge, atomic service timeline), `state_*` (invocations, history, runner contexts, workflow runs), `trg_*` (conditions, definitions, events, trigger runs) and the client data store |
| Minimal role       | `readWrite` on that database                                                                                                                                                                                                         |
| Indexes            | Created on first connect; needs `readWrite` (a failure is logged and the app runs without them)                                                                                                                                      |
| `purge()`          | `delete_many` on every collection: `readWrite` is enough; `dropDatabase` is never needed                                                                                                                                             |

`mongo_db` is a prefix, not the final name, so that several applications can
share one cluster. An operator who creates and grants databases ahead of time
can pass `mongo_db_verbatim=True` to use the name as given; every process of
the application (producers, workers, monitor, cron images) must then pass the
same `mongo_db`.

```python
app = App(app_id="shop", backend="mongo3",
          mongo_url="mongodb://tasks:secret@mongo:27017/?authSource=admin",
          mongo_db="rustvello")        # data in rustvello_shop
```

Grant, as a database administrator:

```javascript
db.getSiblingDB("admin").createUser({
  user: "tasks",
  pwd: "secret",
  roles: [{ role: "readWrite", db: "rustvello_shop" }],
});
```

The Python pool objects report the final name:
`RustMongo3Pool(url, "rustvello", "shop").database_name == "rustvello_shop"`.

## PostgreSQL (`backend="postgres"`)

| Setting      | Value                                                                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database     | The one in `postgres_url`                                                                                                                                      |
| Schema       | One schema per application, named after the `app_id` (`CREATE SCHEMA IF NOT EXISTS "<app_id>"`); every connection sets `search_path` to it                     |
| Tables       | `broker_queue`, `invocations`, `calls`, `results`, `errors`, `history` and the orchestrator, trigger and runner tables, created at first start                 |
| Minimal role | `CREATE` on the database at first start (the schema and its tables); afterwards `USAGE` on the schema and `SELECT`, `INSERT`, `UPDATE`, `DELETE` on its tables |
| `purge()`    | `DELETE` on the app's rows; no `DROP` needed                                                                                                                   |

## Redis (`backend="redis"`)

| Setting        | Value                                                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Keys           | Prefixed with `rustvello:<app_id>:`                                                                                      |
| Minimal rights | Read and write on those keys (an ACL rule such as `~rustvello:<app_id>:*` with `+@all`, or a dedicated logical database) |
| `purge()`      | Deletes the app's keys                                                                                                   |

## SQLite (`backend="sqlite"`)

One file (`db_path`) holds everything; the process needs read and write
access to the file and its directory (SQLite creates `-wal` and `-shm` files
next to it). Every process of the app must use the same path.

## RabbitMQ (`broker="rabbitmq"`)

RabbitMQ carries only the queue; state and results stay in the backend.

| Setting        | Value                                                                                                                                                                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Vhost          | The one in `rabbitmq_url` (default `/`)                                                                                                                                                                                        |
| Queues         | `<rabbitmq_prefix>_rustvello_broker_<language>_<queue>` per declared queue and task language, for example `shop_rustvello_broker_python_default` (`rabbitmq_prefix` defaults to the `app_id`)                                  |
| Minimal rights | On that vhost: `configure` (to declare the queues), `write` (publish) and `read` (consume), for example `rabbitmqctl set_permissions -p / tasks "^shop_rustvello_broker_" "^shop_rustvello_broker_" "^shop_rustvello_broker_"` |
| `purge()`      | Purges the app's queues; no delete right needed                                                                                                                                                                                |

## In-memory (`backend="memory"`)

Nothing is created outside the process. Only for tests and single-process
development.
