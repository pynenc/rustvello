# Benchmark: Rustvello vs Celery

A small comparison of Rustvello and Celery with **equal durability**: every
accepted task and every result is written to storage that fsyncs before it is
acknowledged, and a task whose worker is killed runs again. It measures the
overhead of each runtime on one machine at a modest scale. It does **not**
show how either system behaves at production scale, on dedicated hardware,
or with real task bodies.

The harness, its pinned versions and its Docker Compose file are in
[`benchmarks/`](../benchmarks/README.md). The raw result files of the runs below
are in `benchmarks/results/`.

## Setup

| System               | Queue and results                                              | Durability settings                                                                                     |
| -------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `rustvello-sqlite`   | one SQLite file on the host                                    | WAL, `synchronous=FULL`                                                                                 |
| `rustvello-postgres` | PostgreSQL 16 (container)                                      | `fsync=on`, `synchronous_commit=on` (defaults)                                                          |
| `celery-redis`       | Redis 7.4 (container), broker and results                      | AOF with `appendfsync always`; `task_acks_late`, `task_reject_on_worker_lost`, `visibility_timeout=5`   |
| `celery-rabbitmq`    | RabbitMQ 3.13 (container) + PostgreSQL 16 results (SQLAlchemy) | durable queues, persistent messages, publisher confirms; `task_acks_late`, `task_reject_on_worker_lost` |

All systems use 4 worker slots (Rustvello: 4 threads; Celery: prefork with 4
children, `worker_prefetch_multiplier=1`), JSON serialization, and a trivial
task that returns a timestamp. Rustvello declares a worker dead after 5 s
without a heartbeat (`runner_dead_after_seconds=5`, heartbeat every 1 s),
matching Celery's 5 s visibility timeout on Redis. Rustvello `rustvello 0.7.0`
(this branch), Celery 5.6.3, kombu 5.6.2, redis-py 8.1.0, SQLAlchemy 2.1.1,
psycopg 3.3.6.

**Machine**: Apple M3 Pro, 11 cores, 19 GB, macOS 26.6, Docker Desktop, Python
3.12.10. The machine was **shared with other workloads**: its 1-minute load
average was between 4.5 and 25 during the runs (recorded in each result file).
Every figure below is the median of 3 runs, with the range in parentheses.

## Results

| Metric                                  | rustvello-sqlite | rustvello-postgres | celery-redis        | celery-rabbitmq       |
| --------------------------------------- | ---------------- | ------------------ | ------------------- | --------------------- |
| End-to-end p50 (ms)                     | 38.9 (31.3-41.4) | 60 (57.6-62.4)     | 11.1 (10.2-11.6)    | 28.7 (27.6-31)        |
| End-to-end p95 (ms)                     | 56.9 (55.7-57.2) | 84.4 (75.3-92.7)   | 24.8 (18-27.9)      | 41.3 (39.8-51.7)      |
| End-to-end p99 (ms)                     | 60.2 (58.3-83.9) | 107.5 (82.1-108.9) | 35.1 (26.9-49.4)    | 58.2 (54.7-89)        |
| Submit to executed p50 (ms)             | 36.6 (28.8-37.8) | 49.1 (46.3-50.5)   | 7.4 (6.6-8.1)       | 1.5 (1.4-1.6)         |
| Burst throughput (tasks/s)              | 1269 (607-1380)  | 78.7 (66.1-80.9)   | 280 (201-316)       | 154 (146-187)         |
| Burst CPU s (client + worker + storage) | 0.27 (0.25-0.38) | 4.23 (3.98-4.71)   | 0.61 (0.58-0.95)    | 8.81 (7.07-8.92)      |
| Idle CPU s per 10 s (worker + storage)  | 0.15 (0.15-0.26) | 1.6 (1.53-1.86)    | 0.11 (0.07-0.12)    | 1.95 (1.78-2.31)      |
| Worker peak RSS (MB)                    | 23.4 (23.3-32.6) | 21.5 (21.5-36)     | 143 (137-231)       | 323 (312-356)         |
| Recovery after SIGKILL (s)              | 7.15 (3.12-7.16) | 7.24 (3.2-7.31)    | 102.6 (102.4-102.8) | 2.85 (2.75-3.22)      |
| Task body runs per recovered task       | 2                | 2                  | 2                   | 2                     |
| Services to operate                     | none             | PostgreSQL         | Redis               | RabbitMQ + PostgreSQL |

Rustvello with `idle_sleep_ms=5` (the default above is the Python worker's
50 ms):

| Metric                                 | rustvello-sqlite, 5 ms | rustvello-postgres, 5 ms |
| -------------------------------------- | ---------------------- | ------------------------ |
| End-to-end p50 / p95 / p99 (ms)        | 7.6 / 15.1 / 29.4      | 40.4 / 73.2 / 95.1       |
| Submit to executed p50 (ms)            | 5.2                    | 29.5                     |
| Burst throughput (tasks/s)             | 1207 (1156-1400)       | 74.6 (65.4-90.4)         |
| Idle CPU s per 10 s (worker + storage) | 1.4 (0.78-1.63)        | 3.88 (3.81-4.26)         |

### Definitions

- **End-to-end**: 200 tasks started on a 20 per second schedule, one in flight at
  a time; time from the submit call to the result returned by the client's
  blocking wait (Rustvello `result()`, Celery `get()`, both polling every 5 ms
  where the API polls; Celery's Redis backend pushes results).
- **Submit to executed**: the same tasks, until the end of the task body.
- **Burst throughput**: 300 tasks submitted at once; 300 divided by the time from
  the first submit to the last task body finishing.
- **CPU**: CPU seconds of the client process, the worker process tree and the
  storage containers (cgroup `usage_usec`) during the burst, and of the worker
  and storage during 10 idle seconds.
- **Recovery**: a 2-second task starts, the worker's process group is SIGKILLed
  0.3 s later and a replacement worker starts at once; time from the kill to the
  result being available. Every system ran the killed task's body a second time.

## Reading the results

- **Dispatch latency.** Celery pushes work to idle workers (Redis `BRPOP`,
  RabbitMQ consumers); Rustvello workers poll the database. With the default
  poll interval, Rustvello's end-to-end latency is several times Celery on Redis.
  A 5 ms poll brings SQLite below Celery on Redis in this setup, at the cost of
  idle CPU (about 14% of one core for 4 idle SQLite worker slots).
- **Throughput.** SQLite has the highest burst rate here, but see the fsync
  caveat below: on macOS, SQLite's commits are much cheaper than a full flush to
  the drive. Rustvello on PostgreSQL has the lowest throughput of the four in
  this setup, and costs more CPU per task than Celery on Redis (but less than
  Celery on RabbitMQ with PostgreSQL results).
- **Memory.** A Rustvello worker process with 4 threads used about 20-35 MB;
  Celery's prefork worker (a parent and 4 children) used about 135-360 MB.
- **Recovery** follows each system's detection mechanism, not its speed:
  RabbitMQ re-queues as soon as the consumer's connection drops; Rustvello
  waits until the dead worker's last heartbeat is 5 s old (3-7 s after the kill,
  depending on when that heartbeat was sent); kombu's Redis transport restores
  unacknowledged messages when a worker starts (only those already older than the
  visibility timeout) and otherwise on every tenth 10-second check, so about
  100 s even with a 5 s timeout. With the default settings, Rustvello waits
  300 s and Celery on Redis one hour.
- **At-least-once** is the common contract: every system ran the killed task's
  body twice. See [Idempotency](idempotency.md).

## Caveats

- One loaded, shared machine; client, workers and storage compete for CPU.
  Compare systems within this page, not with numbers from other hardware, and
  rerun on your own before deciding.
- fsync is not equal. SQLite runs on the macOS host, where `fsync()` does not
  force the drive cache (`F_FULLFSYNC`) unless `PRAGMA fullfsync` is set; the
  containers write through Docker Desktop's Linux VM and its virtual disk. On a
  Linux host with local disks all four systems fsync to the device, and SQLite's
  advantage is expected to shrink.
- Trivial tasks measure overhead only. With tasks that take tens of milliseconds
  or more, the differences in dispatch overhead matter less.
- 3 runs of 200 + 300 tasks and 3 kills per system are enough to see differences
  of the size shown, not to establish tail latencies precisely.
- Before this benchmark, `idle_sleep_ms` had no effect (idle workers always
  waited 100 ms); the fix is in this release. The numbers above were measured
  with the fix.

## Reproduce

```bash
make install
make bench-up                       # containers on 127.0.0.1:56379 / 55673 / 55433
for i in 1 2 3; do
  make bench                        # all four systems, Python worker default idle_sleep_ms=50
  make bench BENCH_ARGS="--systems rustvello-sqlite rustvello-postgres --idle-sleep-ms 5"
done
python3 benchmarks/summarize.py benchmarks/results/*.json
make bench-down
```

`make bench` runs
`uv run --no-sync --with-requirements benchmarks/requirements.txt python benchmarks/run.py`
with the defaults `--rate 20 --latency-tasks 200 --burst-tasks 300
--recovery-trials 3 --recover-after 5 --poll-ms 5`.
