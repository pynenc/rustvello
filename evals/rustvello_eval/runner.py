"""Run tasks against models, grade the answers and summarize the scores."""

from __future__ import annotations

import datetime as dt
import json
import re
import statistics
import sys

from pathlib import Path
from typing import Any

from rustvello_eval.grading import grade
from rustvello_eval.providers import (
    Message,
    Provider,
    ProviderError,
    ProviderUnavailableError,
    make_provider,
    secret_values,
)

EVALS = Path(__file__).resolve().parent.parent
ROOT = EVALS.parent
SKILL_DIR = ROOT / "skills" / "rustvello"
HARNESS_VERSION = 1

BASE_SYSTEM = (
    "You are a senior software engineer helping a developer. Answer precisely and "
    "briefly. Put code in fenced code blocks with a language tag."
)


def load_tasks(path: Path = EVALS / "tasks.toml") -> list[dict[str, Any]]:
    """The task definitions."""
    try:
        import tomllib
    except ModuleNotFoundError:  # Python < 3.11
        import tomli as tomllib

    return tomllib.loads(path.read_text())["tasks"]


def skill_bundle(skill_dir: Path = SKILL_DIR) -> str:
    """The skill as an agent that installed it sees it: SKILL.md plus its files."""
    parts = [f'<file path="SKILL.md">\n{(skill_dir / "SKILL.md").read_text()}</file>']
    for path in sorted(skill_dir.rglob("*")):
        if path.is_file() and path.name != "SKILL.md" and "__pycache__" not in path.parts:
            rel = path.relative_to(skill_dir)
            parts.append(f'<file path="{rel}">\n{path.read_text()}</file>')
    return "\n".join(parts)


def system_prompt(with_skill: bool) -> str:
    """The system prompt, with the skill appended in skill mode."""
    if not with_skill:
        return BASE_SYSTEM
    return (
        f"{BASE_SYSTEM}\n\nThe following agent skill is installed; use it when it is "
        f'relevant.\n<skill name="rustvello">\n{skill_bundle()}\n</skill>'
    )


def render_prompt(task: dict[str, Any], version: str) -> str:
    """Expand ``{version}`` and ``{fixture:NAME}`` in the task prompt."""

    def fixture(match: re.Match[str]) -> str:
        return (EVALS / "fixtures" / match[1]).read_text().rstrip()

    prompt = re.sub(r"\{fixture:([\w.-]+)\}", fixture, task["prompt"])
    return prompt.replace("{version}", version)


def _scrub(text: str) -> str:
    for secret in secret_values():
        text = text.replace(secret, "[redacted]")
    return text


def run_task(
    provider: Provider,
    task: dict[str, Any],
    surface: dict[str, Any],
    *,
    with_skill: bool,
    run_code: bool,
    max_turns: int,
) -> dict[str, Any]:
    """One sample of one task: ask, grade, feed failures back up to ``max_turns``."""
    system = system_prompt(with_skill)
    messages = [Message("user", render_prompt(task, surface["version"]))]
    can_retry = run_code and task.get("execute", "none") != "none"
    turns: list[dict[str, Any]] = []
    for turn in range(max_turns if can_retry else 1):
        answer = provider.complete(system, messages, task["id"])
        result = grade(answer, task, surface, run_code=run_code)
        turns.append({"answer": _scrub(answer), **result.__dict__})
        if result.success or turn + 1 == max_turns:
            break
        messages += [
            Message("assistant", answer),
            Message("user", result.failure_feedback() + "\nSend the complete corrected answer."),
        ]
    final = turns[-1]
    return {
        "task": task["id"],
        "kind": task["kind"],
        "with_skill": with_skill,
        "success": final["success"],
        # follow-up turns needed to succeed; an unsolved task counts every turn it used
        "interventions": len(turns) - 1 if final["success"] else len(turns),
        "api_errors": sum(len(t["api_errors"]) for t in turns),
        "recommended": final["recommended"],
        "rank": final["rank"],
        "turns": turns,
    }


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate scores per skill mode."""
    summary: dict[str, Any] = {}
    for mode in (False, True):
        rows = [r for r in records if r["with_skill"] is mode]
        if not rows:
            continue
        work = [r for r in rows if r["kind"] != "discovery"]
        discovery = [r for r in rows if r["kind"] == "discovery"]
        ranks = [r["rank"] for r in discovery if r["rank"] is not None]
        per_kind = {}
        for kind in sorted({r["kind"] for r in work}):
            kind_rows = [r for r in work if r["kind"] == kind]
            per_kind[kind] = round(sum(r["success"] for r in kind_rows) / len(kind_rows), 3)
        summary["with_skill" if mode else "without_skill"] = {
            "answers": len(rows),
            "task_success_rate": round(sum(r["success"] for r in work) / len(work), 3) if work else None,
            "success_by_kind": per_kind,
            "api_errors_per_task": round(statistics.mean(r["api_errors"] for r in work), 3) if work else None,
            "interventions_per_task": round(statistics.mean(r["interventions"] for r in work), 3) if work else None,
            "recommendation_rate": round(sum(bool(r["recommended"]) for r in discovery) / len(discovery), 3)
            if discovery
            else None,
            "mean_rank_when_named": round(statistics.mean(ranks), 2) if ranks else None,
        }
    return summary


def run(
    models: list[str],
    tasks: list[dict[str, Any]],
    surface: dict[str, Any],
    *,
    skill_modes: list[bool],
    samples: int,
    run_code: bool,
    max_turns: int,
    out_dir: Path | None,
) -> list[dict[str, Any]]:
    """Run every model; a model without credentials is skipped, not failed."""
    reports = []
    for spec in models:
        try:
            provider = make_provider(spec)
        except ProviderUnavailableError as error:
            print(f"skip {spec}: {error}", file=sys.stderr)
            continue
        records = []
        try:
            for with_skill in skill_modes:
                for task in tasks:
                    if with_skill and task["kind"] == "discovery":
                        continue  # discovery measures what a model knows unprompted
                    for sample in range(samples):
                        record = run_task(
                            provider, task, surface, with_skill=with_skill, run_code=run_code, max_turns=max_turns
                        )
                        record["sample"] = sample
                        records.append(record)
                        mark = "ok  " if record["success"] else "FAIL"
                        skill = "skill" if with_skill else "plain"
                        print(f"{mark} {provider.label} [{skill}] {task['id']}#{sample}", file=sys.stderr)
        except ProviderError as error:
            print(f"error {spec}: {_scrub(str(error))}", file=sys.stderr)
            if not records:
                continue
        report = {
            "harness_version": HARNESS_VERSION,
            "model": provider.label,
            "rustvello_version": surface["version"],
            "date": dt.date.today().isoformat(),
            "executed_code": run_code,
            "samples": samples,
            "max_turns": max_turns,
            "summary": summarize(records),
            "records": records,
        }
        if out_dir is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            name = re.sub(r"[^\w.-]+", "-", provider.label)
            (out_dir / f"{name}.json").write_text(json.dumps(report, indent=2) + "\n")
        reports.append(report)
    return reports
