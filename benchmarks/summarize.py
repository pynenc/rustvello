"""Combine several ``run.py`` result files: median and range per system and metric.

python benchmarks/summarize.py benchmarks/results/*.json
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

METRICS = {
    "e2e p50 ms": lambda r: r["latency"]["end_to_end_ms"]["p50"],
    "e2e p95 ms": lambda r: r["latency"]["end_to_end_ms"]["p95"],
    "e2e p99 ms": lambda r: r["latency"]["end_to_end_ms"]["p99"],
    "executed p50 ms": lambda r: r["latency"]["submit_to_executed_ms"]["p50"],
    "burst tasks/s": lambda r: r["throughput"]["completed_per_s"],
    "burst CPU s (client+worker+storage)": lambda r: round(
        r["throughput"]["client_cpu_s"]
        + r["throughput"]["worker_cpu_s"]
        + sum(r["throughput"]["storage_cpu_s"].values()),
        2,
    ),
    "idle CPU s per 10 s (worker+storage)": lambda r: round(
        r["idle"]["worker_cpu_s"] + sum(r["idle"]["storage_cpu_s"].values()), 2
    ),
    "worker peak RSS MB": lambda r: r["throughput"]["worker_peak_rss_mb"],
    "recovery s": lambda r: r["recovery"]["kill_to_result_s"],
    "body runs per recovered task": lambda r: r["recovery"]["body_executions"],
}


def cell(values: list[Any]) -> str:
    flat = [v for x in values for v in (x if isinstance(x, list) else [x])]
    if len(set(flat)) == 1:
        return str(flat[0])
    median = statistics.median(flat)
    return f"{median:g} ({min(flat):g}-{max(flat):g})"


def main(paths: list[str]) -> int:
    by_system: dict[str, list[dict[str, Any]]] = defaultdict(list)
    loads = []
    for path in paths:
        report = json.loads(Path(path).read_text())
        loads.append(report["machine"]["load_average_at_start"][0])
        loads.append(report["machine"]["load_average_at_end"][0])
        for result in report["results"]:
            by_system[result["system"]].append(result)
    systems = list(by_system)
    print("| Metric | " + " | ".join(f"{s} (n={len(by_system[s])})" for s in systems) + " |")
    print("| --- |" + " --- |" * len(systems))
    for name, get in METRICS.items():
        print(f"| {name} | " + " | ".join(cell([get(r) for r in by_system[s]]) for s in systems) + " |")
    print(f"\n1-minute load average at start/end of the runs: {min(loads):g}-{max(loads):g}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
