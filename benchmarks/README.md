# Rustvello vs Celery benchmark

A small, reproducible comparison of Rustvello and Celery with **the same
durability**: every accepted task and every result is written to storage that
fsyncs before it is acknowledged, and a worker killed mid-task has its task
run again. Published results and their limits: [docs/benchmarks.md](../docs/benchmarks.md).

## What is compared

| System               | Queue                                     | Results                    | Durability settings                                                                                                          |
| -------------------- | ----------------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `rustvello-sqlite`   | SQLite file (host disk)                   | same SQLite file           | WAL, `synchronous=FULL`; queue, state and result change in one transaction                                                   |
| `rustvello-postgres` | PostgreSQL 16 (container)                 | same database              | PostgreSQL defaults: `fsync=on`, `synchronous_commit=on`                                                                     |
| `celery-redis`       | Redis 7.4 (container)                     | same Redis                 | `appendonly yes`, `appendfsync always`; `task_acks_late`, `task_reject_on_worker_lost`, `visibility_timeout`                 |
| `celery-rabbitmq`    | RabbitMQ 3.13 (container), durable queues | PostgreSQL 16 (SQLAlchemy) | persistent messages (Celery default), publisher confirms (`confirm_publish`), `task_acks_late`, `task_reject_on_worker_lost` |

Common settings: 4 worker slots (Rustvello: 4 threads in one worker process;
Celery: prefork pool with 4 children), `worker_prefetch_multiplier=1` for
Celery, JSON serialization, one client process on the same host. The
dead-worker window is the same for the systems that have one: Rustvello
`runner_dead_after_seconds=5` (heartbeat 1 s, recovery check 1 s) and Celery on
Redis `visibility_timeout=5`. RabbitMQ re-queues an unacknowledged message as
soon as the consumer's connection closes.

## Phases

1. **Latency**: 200 tasks, started on a 20 per second schedule, one in flight
   at a time. The task returns its finish time. _End-to-end_ = submit call to
   result returned by the client's blocking wait (Rustvello `result()`,
   Celery `get()`; both poll every 5 ms where the API polls, and Celery's Redis
   backend pushes results through pub/sub). _Executed_ = submit call to the end of
   the task body.
2. **Throughput**: 300 tasks submitted at once. _Burst tasks/s_ = 300 divided by
   the time from the first submit to the last task body finishing; every result
   is then read back. CPU seconds of the client process, the worker process
   tree and the storage containers (cgroup `usage_usec`) are measured over the
   phase, and the worker's peak RSS is sampled every 200 ms.
3. **Idle**: CPU used by the worker and storage in 10 s without work (workers
   that poll pay for it).
4. **Recovery**: a task that sleeps 2 s starts; 0.3 s later the worker's
   process group is SIGKILLed and a replacement worker starts at once.
   _Recovery_ = kill to result available to the client. The task records every
   execution, so the table shows that the body ran twice (at-least-once).

## Run it

Prerequisites: Docker with Compose v2, [uv](https://docs.astral.sh/uv/), a Rust
toolchain, and Python 3.12.

```bash
make install        # once: dev environment and the Rustvello extension
make bench-up       # Redis, RabbitMQ, PostgreSQL on 127.0.0.1:56379/55673/55433
make bench          # all four systems; results in benchmarks/results/
make bench BENCH_ARGS="--systems rustvello-sqlite rustvello-postgres --idle-sleep-ms 5"
make bench-down     # remove the containers and their volumes
```

Combine repeated runs into medians and ranges:

```bash
python3 benchmarks/summarize.py benchmarks/results/*.json
```

`make bench` runs
`uv run --no-sync --with-requirements benchmarks/requirements.txt python benchmarks/run.py`.
Options: `--systems`, `--rate`, `--latency-tasks`, `--burst-tasks`,
`--recovery-trials`, `--recover-after`, `--poll-ms`, `--idle-sleep-ms`
(`python benchmarks/run.py --help`). Ports can be moved with
`RVBENCH_REDIS_PORT`, `RVBENCH_AMQP_PORT` and `RVBENCH_POSTGRES_PORT`. Each
system starts from empty storage and runs in its own interpreter. Every worker
the harness starts is killed when the system finishes, also on failure.

Versions are pinned in [`requirements.txt`](requirements.txt) (Celery side) and
[`docker-compose.yml`](docker-compose.yml) (images); Rustvello is the checkout
built by `make develop`. Each result file records the versions, the machine and
the load average at start and end.

## Setup steps compared

| System               | Services to run and operate           | Worker command                                                |
| -------------------- | ------------------------------------- | ------------------------------------------------------------- |
| `rustvello-sqlite`   | none (a file)                         | `python -m rustvello.worker bench_tasks:app --workers 4`      |
| `rustvello-postgres` | PostgreSQL                            | same                                                          |
| `celery-redis`       | Redis with AOF `appendfsync always`   | `celery -A bench_tasks worker --concurrency 4 --pool prefork` |
| `celery-rabbitmq`    | RabbitMQ, plus PostgreSQL for results | same                                                          |

Cron schedules add a `celery beat` process on the Celery side; Rustvello
workers evaluate triggers themselves.

## Known caveats

- **One machine, shared and loaded.** Client, workers and storage share the
  CPU. Absolute numbers depend heavily on the host; compare systems within one
  run, and rerun before drawing conclusions.
- **fsync is not equal everywhere.** SQLite runs on the host file system; the
  containers run inside Docker Desktop's VM on macOS, whose virtual disk decides
  when an fsync reaches the physical drive. On macOS, SQLite's `fsync()` does not
  use `F_FULLFSYNC` unless `PRAGMA fullfsync` is set. On Linux with local disks
  both sides fsync to the device.
- **Tiny tasks.** The tasks do almost nothing, so the numbers measure the
  runtime's overhead: queueing, storage round trips and polling. Real tasks add
  their own time.
- **Polling.** Rustvello workers poll the database. `idle_sleep_ms` bounds
  how long an idle worker waits before polling again (the Python worker's
  default is 50 ms; SQLite and PostgreSQL never wait more than 100 ms), which
  trades dispatch delay for idle CPU. Celery on Redis blocks on `BRPOP` and
  receives results through pub/sub; on RabbitMQ messages are pushed. Runs with
  the default and with 5 ms are shown.
- **Recovery depends on configuration.** Defaults are much slower: Rustvello
  recovers after `runner_dead_after_seconds=300`; Celery on Redis after the
  visibility timeout (1 hour by default), and kombu 5.6 only restores
  unacknowledged messages at worker start (for messages already older than the
  timeout) and on every tenth 10-second check, so recovery can take about
  100 s even with a 5 s timeout.
- **macOS workaround.** Celery 5.6 prefork children on macOS failed every task
  with `not enough values to unpack (expected 3, got 0)` until
  `FORKED_BY_MULTIPROCESSING=1` was set; the harness sets it on macOS only.
