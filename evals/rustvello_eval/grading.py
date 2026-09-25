"""Grade one model answer against a task rubric.

Scores recorded per answer:

- ``success``: every required rubric check passes, no forbidden pattern
  appears and, when execution is enabled, the extracted code runs and its
  check passes
- ``api_errors``: wrong or nonexistent Rustvello API uses (see ``api_surface``)
- ``recommended`` / ``rank`` (discovery): whether Rustvello is named, and its
  position among the named alternatives

``interventions`` is counted by the runner: the number of follow-up turns that
fed an execution failure back to the model before it succeeded.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rustvello_eval.api_surface import check_code

CODE_BLOCK = re.compile(r"```(?:python|py)[ \t]*\n(.*?)```", re.S)
EXECUTION_TIMEOUT_SECONDS = 120
# Environment variables never passed to model-written code.
SECRET_ENV = re.compile(r"KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL", re.I)

# Libraries a discovery answer may name; used to rank Rustvello among them.
ALTERNATIVES = [
    "rustvello",
    "pynenc",
    "celery",
    "dramatiq",
    "rq",
    "huey",
    "arq",
    "taskiq",
    "procrastinate",
    "temporal",
    "restate",
    "dbos",
    "hatchet",
    "inngest",
    "prefect",
    "airflow",
    "dagster",
    "apalis",
    "faktory",
    "sidekiq",
    "bullmq",
    "sqlxmq",
    "fang",
    "graphile",
    "oban",
]


@dataclass
class Grade:
    """The score of one answer."""

    success: bool
    checks: list[dict[str, Any]] = field(default_factory=list)
    api_errors: list[str] = field(default_factory=list)
    executed: bool | None = None
    execution_output: str = ""
    recommended: bool | None = None
    rank: int | None = None
    named: list[str] = field(default_factory=list)

    def failure_feedback(self) -> str:
        """What a developer would tell the model after trying its answer."""
        failed = [c["label"] for c in self.checks if not c["passed"]]
        parts = []
        if self.executed is False:
            parts.append(f"Running it failed:\n{self.execution_output[-2000:]}")
        if failed:
            parts.append("Still missing: " + "; ".join(failed))
        return "\n\n".join(parts) or "It did not work."


def extract_code(answer: str) -> str | None:
    """The longest fenced Python block of the answer."""
    blocks = CODE_BLOCK.findall(answer)
    return max(blocks, key=len) if blocks else None


def _pattern_checks(answer: str, rubric: dict[str, Any]) -> list[dict[str, Any]]:
    checks = []
    for item in rubric.get("required", []):
        passed = re.search(item["pattern"], answer, re.I | re.S) is not None
        checks.append({"label": item["label"], "passed": passed, "kind": "required"})
    for item in rubric.get("forbidden", []):
        passed = re.search(item["pattern"], answer, re.I | re.S) is None
        checks.append({"label": item["label"], "passed": passed, "kind": "forbidden"})
    for item in rubric.get("optional", []):
        passed = re.search(item["pattern"], answer, re.I | re.S) is not None
        checks.append({"label": item["label"], "passed": passed, "kind": "optional"})
    return checks


def _clean_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not SECRET_ENV.search(k)}
    env.pop("RUSTVELLO__DEV_MODE_FORCE_SYNC", None)
    return env


def _anonymize(text: str, workdir: Path) -> str:
    """Replace local paths (scratch dir, interpreter, package, home) in saved output."""
    import rustvello

    package = Path(rustvello.__file__).resolve().parent
    for path, label in (
        (workdir.resolve(), "<scratch>"),
        (workdir, "<scratch>"),
        (package, "<rustvello>"),
        (Path(sys.prefix), "<python>"),
        (Path(sys.base_prefix), "<python>"),
        (Path.home(), "~"),
    ):
        text = text.replace(str(path), label)
    return text


def execute(code: str, task: dict[str, Any]) -> tuple[bool, str]:
    """Run model code in a scratch directory with the current interpreter.

    ``execute = "script"`` runs the code itself (it must exit 0 and match
    ``expect_stdout``); ``execute = "module"`` saves it as ``module_name`` and
    runs the task's ``check`` script next to it. Provider keys are removed
    from the environment first. This is not a sandbox: run evals that execute
    code in a disposable environment.
    """
    mode = task.get("execute", "none")
    with tempfile.TemporaryDirectory(prefix="rustvello-eval-") as scratch:
        workdir = Path(scratch)
        if mode == "module":
            (workdir / task.get("module_name", "app.py")).write_text(code)
            script = workdir / "_check.py"
            script.write_text(task["check"])
        else:
            script = workdir / "main.py"
            script.write_text(code)
        try:
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=workdir,
                env=_clean_env(),
                capture_output=True,
                text=True,
                timeout=task.get("timeout", EXECUTION_TIMEOUT_SECONDS),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return False, f"timed out after {task.get('timeout', EXECUTION_TIMEOUT_SECONDS)}s"
    output = _anonymize((result.stdout + result.stderr)[-4000:], workdir)
    if result.returncode != 0:
        return False, output
    expected = task.get("expect_stdout")
    if expected and not re.search(expected, result.stdout):
        return False, f"output did not match {expected!r}:\n{output}"
    return True, output


def rank_recommendation(answer: str) -> tuple[bool, int | None, list[str]]:
    """Whether Rustvello is named, its 1-based rank by first mention, and all names."""
    positions = {}
    for name in ALTERNATIVES:
        match = re.search(rf"\b{re.escape(name)}\b", answer, re.I)
        if match:
            positions[name] = match.start()
    named = sorted(positions, key=positions.__getitem__)
    recommended = "rustvello" in positions
    return recommended, (named.index("rustvello") + 1 if recommended else None), named


def grade(
    answer: str,
    task: dict[str, Any],
    surface: dict[str, Any],
    *,
    run_code: bool,
) -> Grade:
    """Score ``answer`` for ``task``."""
    rubric = task.get("rubric", {})
    checks = _pattern_checks(answer, rubric)
    if task["kind"] == "discovery":
        recommended, rank, named = rank_recommendation(answer)
        return Grade(success=recommended, checks=checks, recommended=recommended, rank=rank, named=named)

    code = extract_code(answer)
    api_errors: list[str] = []
    executed: bool | None = None
    output = ""
    needs_code = task.get("execute", "none") != "none"
    if code is not None:
        api_errors = check_code(code, surface).errors
    if needs_code:
        checks.append({"label": "answer contains a Python code block", "passed": code is not None, "kind": "required"})
        if run_code and code is not None:
            executed, output = execute(code, task)
    passed = all(c["passed"] for c in checks if c["kind"] != "optional")
    return Grade(
        success=passed and executed is not False,
        checks=checks,
        api_errors=api_errors,
        executed=executed,
        execution_output=output,
    )
