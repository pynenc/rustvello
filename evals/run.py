"""Cross-model eval of Rustvello: install, implement, recover and discovery tasks.

Examples::

    # validate the harness itself: no keys, no network
    python evals/run.py run --model mock:reference --execute

    # real models (keys come from the environment and are never printed)
    python evals/run.py run --model anthropic:claude-opus-5 --model openai:$OPENAI_MODEL \
        --execute --samples 3 --out evals/results

See evals/README.md for the tasks, the scores and how to record a baseline.
"""

from __future__ import annotations

import argparse
import json
import sys

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rustvello_eval import api_surface
from rustvello_eval.runner import load_tasks, run


def _cmd_run(args: argparse.Namespace) -> int:
    tasks = load_tasks()
    if args.task:
        tasks = [t for t in tasks if t["id"] in set(args.task)]
    if args.kind:
        tasks = [t for t in tasks if t["kind"] in set(args.kind)]
    if not tasks:
        print("no task matches", file=sys.stderr)
        return 2
    modes = {"both": [False, True], "with": [True], "without": [False]}[args.skill]
    surface = api_surface.load()
    out_dir = Path(args.out) / surface["version"] if args.out else None
    reports = run(
        args.model,
        tasks,
        surface,
        skill_modes=modes,
        samples=args.samples,
        run_code=args.execute,
        max_turns=args.max_turns,
        out_dir=out_dir,
    )
    summaries = {r["model"]: r["summary"] for r in reports}
    print(json.dumps(summaries, indent=2))
    if not reports:
        print("no model ran (missing keys are skipped, see above)", file=sys.stderr)
    return 0


def _cmd_list(_: argparse.Namespace) -> int:
    for task in load_tasks():
        print(f"{task['id']:<28} {task['kind']:<10} {task.get('execute', 'none')}")
    return 0


def _cmd_api_surface(args: argparse.Namespace) -> int:
    installed = api_surface.introspect()
    text = json.dumps(installed, indent=2, sort_keys=True) + "\n"
    if args.write:
        api_surface.SNAPSHOT.write_text(text)
        print(f"wrote {api_surface.SNAPSHOT}")
        return 0
    if api_surface.SNAPSHOT.read_text() != text:
        print("api_surface.json differs from the installed wheel; run `api-surface --write`", file=sys.stderr)
        return 1
    print("api_surface.json matches the installed wheel")
    return 0


def main() -> int:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run tasks against models")
    run_parser.add_argument("--model", action="append", required=True, help="provider:model (repeatable)")
    run_parser.add_argument("--task", action="append", help="only this task id (repeatable)")
    run_parser.add_argument("--kind", action="append", choices=["install", "implement", "recover", "discovery"])
    run_parser.add_argument("--skill", choices=["both", "with", "without"], default="both")
    run_parser.add_argument("--samples", type=int, default=1, help="answers per task (default 1)")
    run_parser.add_argument("--execute", action="store_true", help="run the code answers (use a disposable env)")
    run_parser.add_argument("--max-turns", type=int, default=3, help="turns per task when code fails (default 3)")
    run_parser.add_argument("--out", help="write <out>/<rustvello version>/<model>.json")
    run_parser.set_defaults(func=_cmd_run)

    list_parser = sub.add_parser("list", help="list the tasks")
    list_parser.set_defaults(func=_cmd_list)

    surface_parser = sub.add_parser("api-surface", help="compare or refresh api_surface.json")
    surface_parser.add_argument("--write", action="store_true")
    surface_parser.set_defaults(func=_cmd_api_surface)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
