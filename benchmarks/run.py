"""Rustvello vs Celery under equal durability: latency, throughput, recovery, resources.

Usage (from the repository root, after ``make develop`` and ``make bench-up``)::

    uv run --no-sync --with-requirements benchmarks/requirements.txt \\
        python benchmarks/run.py --systems rustvello-sqlite celery-redis

Every system runs the same three phases with 4 worker slots:

1. **latency**: ``--latency-tasks`` tasks started on a ``--rate`` per second schedule,
   one in flight at a time; end-to-end = submit call -> result returned by the
   client's blocking wait (``--poll-ms`` where the client API polls). The task body
   stamps its finish time, which gives the submit -> executed part.
2. **throughput**: ``--burst-tasks`` submitted at once; completed tasks per second
   from the first submit to the last task body finishing, then every result is
   read back. Worker CPU/RSS and storage-container CPU are measured in this phase.
3. **recovery**: a 2-second task starts, the worker process group is SIGKILLed,
   a replacement worker starts at once; recovery = kill -> result observed.

Results go to ``benchmarks/results/<timestamp>.json`` and ``.md``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import psutil

HERE = Path(__file__).resolve().parent
SYSTEMS = ["rustvello-sqlite", "rustvello-postgres", "celery-redis", "celery-rabbitmq"]
SLOTS = 4


def ports() -> dict[str, int]:
    return {
        "redis": int(os.environ.get("RVBENCH_REDIS_PORT", "56379")),
        "amqp": int(os.environ.get("RVBENCH_AMQP_PORT", "55673")),
        "postgres": int(os.environ.get("RVBENCH_POSTGRES_PORT", "55433")),
    }


def system_env(system: str, workdir: Path, recover_after: int, idle_sleep_ms: int) -> dict[str, str]:
    p = ports()
    env = dict(os.environ)
    env.update(
        BENCH_SYSTEM=system,
        BENCH_RECOVER_AFTER=str(recover_after),
        BENCH_IDLE_SLEEP_MS=str(idle_sleep_ms),
        BENCH_SQLITE_PATH=str(workdir / "bench.sqlite"),
        BENCH_POSTGRES_URL=f"postgresql://bench:bench@127.0.0.1:{p['postgres']}/rustvello",
        BENCH_CELERY_DB_URL=f"postgresql+psycopg://bench:bench@127.0.0.1:{p['postgres']}/celery",
        BENCH_REDIS_URL=f"redis://127.0.0.1:{p['redis']}/0",
        BENCH_AMQP_URL=f"amqp://guest:guest@127.0.0.1:{p['amqp']}//",
        PYTHONPATH=str(HERE) + os.pathsep + env.get("PYTHONPATH", ""),
        PYTHONUNBUFFERED="1",
    )
    if system.startswith("celery") and sys.platform == "darwin":
        # Without it, Celery 5.6 prefork children on macOS fail every task with
        # "not enough values to unpack (expected 3, got 0)" in fast_trace_task.
        env["FORKED_BY_MULTIPROCESSING"] = "1"
    return env


def reset_storage(system: str) -> None:
    """Start every system from empty storage."""
    if system == "rustvello-postgres" or system == "celery-rabbitmq":
        db = "rustvello" if system == "rustvello-postgres" else "celery"
        for sql in (f"DROP DATABASE IF EXISTS {db} WITH (FORCE)", f"CREATE DATABASE {db}"):
            subprocess.run(
                ["docker", "exec", "rvbench-postgres", "psql", "-q", "-U", "bench", "-d", "bench", "-c", sql],
                check=True,
                stdout=subprocess.DEVNULL,
            )
    if system == "celery-redis":
        subprocess.run(
            ["docker", "exec", "rvbench-redis", "redis-cli", "FLUSHALL"], check=True, stdout=subprocess.DEVNULL
        )
    if system == "celery-rabbitmq":
        subprocess.run(
            ["docker", "exec", "rvbench-rabbitmq", "rabbitmqctl", "-q", "purge_queue", "celery"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def worker_command(system: str) -> list[str]:
    if system.startswith("rustvello"):
        return [
            sys.executable, "-m", "rustvello.worker", "bench_tasks:app", "--workers", str(SLOTS), "--no-triggers",
            "--idle-sleep-ms", os.environ.get("BENCH_IDLE_SLEEP_MS", "50"),
        ]  # fmt: skip
    return [
        sys.executable, "-m", "celery", "-A", "bench_tasks", "worker",
        "--concurrency", str(SLOTS), "--pool", "prefork", "--loglevel", "WARNING", "--without-gossip",
        "--without-mingle", "--without-heartbeat",
    ]  # fmt: skip


STARTED: list[subprocess.Popen[bytes]] = []


def start_worker(system: str, env: dict[str, str], log: Path) -> subprocess.Popen[bytes]:
    proc = subprocess.Popen(
        worker_command(system),
        cwd=HERE,
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
        start_new_session=True,  # own process group: SIGKILL takes the prefork children too
    )
    STARTED.append(proc)
    return proc


def kill_worker(proc: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


class Client:
    """Submit and wait through each system's own client API."""

    def __init__(self, system: str, env: dict[str, str], poll: float) -> None:
        os.environ.update({k: v for k, v in env.items() if k.startswith("BENCH_")})
        sys.path.insert(0, str(HERE))
        import bench_tasks

        self.system = system
        self.tasks = bench_tasks
        self.rustvello = system.startswith("rustvello")
        self.poll = poll

    def submit_stamp(self) -> tuple[float, Any]:
        sent = time.time()
        return sent, self.tasks.stamp(sent) if self.rustvello else self.tasks.stamp.delay(sent)

    def submit_slow(self, seconds: float, marker: str) -> Any:
        return self.tasks.slow(seconds, marker) if self.rustvello else self.tasks.slow.delay(seconds, marker)

    def ready(self, handle: Any) -> bool:
        return handle.status.is_terminal() if self.rustvello else handle.ready()

    def wait(self, handle: Any, timeout: float = 300) -> Any:
        """Block for the result the way an application would (``--poll-ms`` where the API polls)."""
        if self.rustvello:
            return handle.result(timeout=timeout, poll_interval=self.poll)
        # Redis: pushed through pub/sub; database backend: polled every ``interval``.
        return handle.get(timeout=timeout, interval=self.poll)


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))]


def summary_ms(values: list[float]) -> dict[str, float]:
    ms = [v * 1000 for v in values]
    return {
        "p50": round(pct(ms, 0.50), 1),
        "p95": round(pct(ms, 0.95), 1),
        "p99": round(pct(ms, 0.99), 1),
        "max": round(max(ms), 1),
        "mean": round(statistics.fmean(ms), 1),
    }


def warm_up(client: Client) -> None:
    for _ in range(SLOTS * 2):
        client.wait(client.submit_stamp()[1], timeout=120)


def phase_latency(client: Client, rate: float, count: int) -> dict[str, Any]:
    """One task in flight at a time, started on a fixed schedule (``rate`` per second)."""
    interval = 1.0 / rate
    e2e: list[float] = []
    executed: list[float] = []
    start = time.time()
    for i in range(count):
        delay = start + i * interval - time.time()
        if delay > 0:
            time.sleep(delay)
        sent, handle = client.submit_stamp()
        done = float(client.wait(handle))
        e2e.append(time.time() - sent)
        executed.append(done - sent)
    return {
        "tasks": count,
        "rate_per_s": rate,
        "end_to_end_ms": summary_ms(e2e),
        "submit_to_executed_ms": summary_ms(executed),
    }


def tree(proc: subprocess.Popen[bytes]) -> list[psutil.Process]:
    try:
        root = psutil.Process(proc.pid)
        return [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return []


def cpu_seconds(procs: list[psutil.Process]) -> float:
    total = 0.0
    for p in procs:
        try:
            t = p.cpu_times()
            total += t.user + t.system
        except psutil.NoSuchProcess:
            pass
    return total


def rss_mb(procs: list[psutil.Process]) -> float:
    total = 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total / 1e6


def phase_throughput(
    client: Client, worker: subprocess.Popen[bytes], count: int, containers: list[str]
) -> dict[str, Any]:
    """Submit ``count`` tasks at once; completion rate from the tasks' own finish stamps."""
    procs = tree(worker)
    cpu_before = cpu_seconds(procs)
    storage_before = {name: container_cpu_s(name) for name in containers}
    peak_rss = rss_mb(procs)
    sampling = True

    def sample() -> None:
        nonlocal peak_rss
        while sampling:
            peak_rss = max(peak_rss, rss_mb(tree(worker)))
            time.sleep(0.2)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    client_before = cpu_seconds([psutil.Process()])
    start = time.time()
    handles = [client.submit_stamp()[1] for _ in range(count)]
    submitted = time.time()
    finished = [float(client.wait(handle, timeout=600)) for handle in handles]
    read_back = time.time()
    sampling = False
    sampler.join()
    return {
        "tasks": count,
        "submit_per_s": round(count / (submitted - start), 1),
        "completed_per_s": round(count / (max(finished) - start), 1),
        "all_results_read_s": round(read_back - start, 2),
        "worker_cpu_s": round(cpu_seconds(tree(worker)) - cpu_before, 2),
        "client_cpu_s": round(cpu_seconds([psutil.Process()]) - client_before, 2),
        "worker_peak_rss_mb": round(peak_rss, 1),
        "storage_cpu_s": {name: round(container_cpu_s(name) - storage_before[name], 2) for name in containers},
        "storage_memory": {name: container_memory(name) for name in containers},
    }


def phase_idle(worker: subprocess.Popen[bytes], containers: list[str], seconds: float) -> dict[str, Any]:
    """CPU an idle worker and its storage use (polling costs something)."""
    cpu_before = cpu_seconds(tree(worker))
    storage_before = {name: container_cpu_s(name) for name in containers}
    time.sleep(seconds)
    return {
        "seconds": seconds,
        "worker_cpu_s": round(cpu_seconds(tree(worker)) - cpu_before, 2),
        "storage_cpu_s": {name: round(container_cpu_s(name) - storage_before[name], 2) for name in containers},
    }


def phase_recovery(
    client: Client, system: str, env: dict[str, str], log: Path, workdir: Path, trials: int
) -> tuple[dict[str, Any], subprocess.Popen[bytes]]:
    times: list[float] = []
    executions: list[int] = []
    worker = start_worker(system, env, log)
    warm_up(client)
    for trial in range(trials):
        marker = workdir / f"slow-{trial}.log"
        handle = client.submit_slow(2.0, str(marker))
        deadline = time.time() + 120
        while not marker.exists() or not marker.read_text().strip():
            if time.time() > deadline:
                raise TimeoutError("slow task never started")
            time.sleep(0.01)
        time.sleep(0.3)  # well inside the 2 s body
        kill_worker(worker)
        killed = time.time()
        worker = start_worker(system, env, log)
        while not client.ready(handle):
            if time.time() - killed > 600:
                raise TimeoutError("task never recovered")
            time.sleep(0.01)
        times.append(time.time() - killed)
        client.wait(handle)
        executions.append(len(marker.read_text().splitlines()))
    return {
        "trials": trials,
        "kill_to_result_s": [round(t, 2) for t in times],
        "median_s": round(statistics.median(times), 2),
        "body_executions": executions,
    }, worker


CONTAINERS = {
    "rustvello-sqlite": [],
    "rustvello-postgres": ["rvbench-postgres"],
    "celery-redis": ["rvbench-redis"],
    "celery-rabbitmq": ["rvbench-rabbitmq", "rvbench-postgres"],
}


def container_cpu_s(name: str) -> float:
    """CPU seconds the container used so far (cgroup v2 ``usage_usec``)."""
    out = subprocess.run(
        ["docker", "exec", name, "cat", "/sys/fs/cgroup/cpu.stat"], capture_output=True, text=True, check=True
    ).stdout
    usage = next(line for line in out.splitlines() if line.startswith("usage_usec"))
    return int(usage.split()[1]) / 1e6


def container_memory(name: str) -> str:
    return subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def run_system(system: str, args: argparse.Namespace) -> dict[str, Any]:
    workdir = Path(tempfile.mkdtemp(prefix=f"rvbench-{system}-"))
    try:
        reset_storage(system)
        env = system_env(system, workdir, args.recover_after, args.idle_sleep_ms)
        log = workdir / "worker.log"
        client = Client(system, env, args.poll_ms / 1000)
        worker = start_worker(system, env, log)
        started = time.time()
        warm_up(client)
        label = system
        if system.startswith("rustvello"):
            label += f" (idle_sleep_ms={args.idle_sleep_ms})"
        result: dict[str, Any] = {"system": label, "worker_ready_s": round(time.time() - started, 2)}
        print(f"[{system}] latency ...", flush=True)
        result["latency"] = phase_latency(client, args.rate, args.latency_tasks)
        print(f"[{system}] throughput ...", flush=True)
        result["throughput"] = phase_throughput(client, worker, args.burst_tasks, CONTAINERS[system])
        result["idle"] = phase_idle(worker, CONTAINERS[system], seconds=10)
        kill_worker(worker)
        print(f"[{system}] recovery ...", flush=True)
        result["recovery"], worker = phase_recovery(client, system, env, log, workdir, args.recovery_trials)
        kill_worker(worker)
        print(json.dumps(result, indent=2), flush=True)
        return result
    finally:
        for proc in STARTED:  # never leave a worker behind, even after a failure
            kill_worker(proc)
        if args.keep_workdir:
            print(f"[{system}] kept {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


def machine() -> dict[str, Any]:
    load = os.getloadavg()
    info: dict[str, Any] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpus": os.cpu_count(),
        "memory_gb": round(psutil.virtual_memory().total / 1e9, 1),
        "python": platform.python_version(),
        "load_average_at_start": [round(x, 1) for x in load],
    }
    if sys.platform == "darwin":
        info["cpu"] = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, check=False
        ).stdout.strip()
    return info


def versions() -> dict[str, str]:
    from importlib.metadata import PackageNotFoundError, version

    found = {}
    for name in ("rustvello", "celery", "kombu", "redis", "SQLAlchemy", "psycopg", "psutil"):
        try:
            found[name] = version(name)
        except PackageNotFoundError:
            pass
    return found


def markdown(report: dict[str, Any]) -> str:
    rows = [
        "| System | e2e p50 / p95 / p99 (ms) | executed p50 / p99 (ms) | burst tasks/s | burst CPU s client + worker + storage | idle CPU s per 10 s worker + storage | worker peak RSS MB | recovery median (s) | body runs per recovered task |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in report["results"]:
        e2e = r["latency"]["end_to_end_ms"]
        ex = r["latency"]["submit_to_executed_ms"]
        tp = r["throughput"]
        rec = r["recovery"]
        rows.append(
            f"| {r['system']} | {e2e['p50']} / {e2e['p95']} / {e2e['p99']} | {ex['p50']} / {ex['p99']} | "
            f"{tp['completed_per_s']} | {tp['client_cpu_s']} + {tp['worker_cpu_s']} + "
            f"{sum(tp['storage_cpu_s'].values()):.2f} | "
            f"{r['idle']['worker_cpu_s']} + {sum(r['idle']['storage_cpu_s'].values()):.2f} | "
            f"{tp['worker_peak_rss_mb']} | "
            f"{rec['median_s']} ({', '.join(map(str, rec['kill_to_result_s']))}) | {rec['body_executions']} |"
        )
    return "\n".join(rows) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--systems", nargs="+", choices=SYSTEMS, default=SYSTEMS)
    parser.add_argument("--rate", type=float, default=20.0, help="latency phase: tasks per second")
    parser.add_argument("--latency-tasks", type=int, default=200)
    parser.add_argument("--burst-tasks", type=int, default=300)
    parser.add_argument("--recovery-trials", type=int, default=3)
    parser.add_argument("--recover-after", type=int, default=5, help="dead-worker / visibility timeout (s)")
    parser.add_argument("--poll-ms", type=float, default=5.0, help="client result polling interval")
    parser.add_argument("--idle-sleep-ms", type=int, default=50, help="Rustvello worker poll sleep (default 50)")
    parser.add_argument("--keep-workdir", action="store_true")
    parser.add_argument("--one", help=argparse.SUPPRESS)  # internal: one system, JSON to this file
    args = parser.parse_args()

    if args.one:
        # Each system runs in its own interpreter, so client libraries never mix.
        result = run_system(args.systems[0], args)
        Path(args.one).write_text(json.dumps(result))
        return 0

    report: dict[str, Any] = {
        "started": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "command": " ".join(sys.argv),
        "settings": {k: v for k, v in vars(args).items() if k != "systems"},
        "machine": machine(),
        "versions": versions(),
        "results": [],
    }
    forwarded = list(sys.argv[1:])
    if "--systems" in forwarded:  # drop the list; each child gets one system
        i = forwarded.index("--systems")
        j = i + 1
        while j < len(forwarded) and not forwarded[j].startswith("--"):
            j += 1
        del forwarded[i:j]
    for system in args.systems:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            target = Path(handle.name)
        subprocess.run(
            [sys.executable, __file__, *forwarded, "--systems", system, "--one", str(target)],
            check=True,
        )
        report["results"].append(json.loads(target.read_text()))
        target.unlink()
    report["machine"]["load_average_at_end"] = [round(x, 1) for x in os.getloadavg()]

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    stem = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    (out / f"{stem}.json").write_text(json.dumps(report, indent=2) + "\n")
    (out / f"{stem}.md").write_text(markdown(report))
    print(markdown(report))
    print(f"wrote {out / (stem + '.json')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
