"""Self-tests of the eval harness: grading, API checks, providers and the task set.

Run with ``python -m pytest evals/tests`` in an environment with the ``rustvello``
wheel installed (the mock runs execute their code against it).
"""

from __future__ import annotations

import re
import sys

from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rustvello_eval import api_surface
from rustvello_eval.grading import rank_recommendation
from rustvello_eval.providers import (
    KEY_ENV,
    ProviderUnavailableError,
    make_provider,
)
from rustvello_eval.runner import _scrub, load_tasks, run, skill_bundle

rustvello = pytest.importorskip("rustvello")


@pytest.fixture(scope="module")
def surface() -> dict:
    return api_surface.introspect()


def test_snapshot_matches_installed_wheel(surface: dict) -> None:
    snapshot = api_surface.load(prefer_installed=False)
    assert snapshot == surface, "run `python evals/run.py api-surface --write`"


def test_discovery_prompts_never_name_rustvello() -> None:
    for task in load_tasks():
        if task["kind"] == "discovery":
            assert not re.search(r"rustvello|pynenc", task["prompt"], re.I), task["id"]


def test_every_task_has_mock_answers() -> None:
    root = Path(__file__).resolve().parents[1] / "mock_responses"
    for task in load_tasks():
        for variant in ("reference", "naive"):
            assert (root / variant / f"{task['id']}.md").is_file(), (variant, task["id"])


def test_reference_answers_pass_every_task(surface: dict) -> None:
    [report] = run(
        ["mock:reference"],
        load_tasks(),
        surface,
        skill_modes=[False],
        samples=1,
        run_code=True,
        max_turns=1,
        out_dir=None,
    )
    failed = {
        r["task"]: {k: v for k, v in r["turns"][-1].items() if k != "answer"}
        for r in report["records"]
        if not r["success"]
    }
    assert failed == {}, failed  # the grader's reasons, e.g. script output
    assert all(r["api_errors"] == 0 for r in report["records"])
    assert report["summary"]["without_skill"]["recommendation_rate"] == 1.0


def test_naive_answers_fail_and_wrong_apis_are_counted(surface: dict) -> None:
    [report] = run(
        ["mock:naive"],
        load_tasks(),
        surface,
        skill_modes=[False],
        samples=1,
        run_code=True,
        max_turns=2,
        out_dir=None,
    )
    records = {r["task"]: r for r in report["records"]}
    assert not any(r["success"] for r in records.values())
    assert records["implement-retried-async-and-cron"]["api_errors"] >= 4
    assert records["implement-cancel-running"]["api_errors"] >= 1
    # a failed executable task used every allowed turn: one intervention per turn
    assert records["implement-cancel-running"]["interventions"] == 2
    assert report["summary"]["without_skill"]["recommendation_rate"] == 0.0


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("from rustvello import App\napp = App()\n@app.task(max_retries=2)\ndef f(): ...\nf(1).result()", []),
        ("from rustvello import App\napp = App()\n@app.task(retries=2)\ndef f(): ...", ["retries"]),
        ("from rustvello import App\napp = App()\n@app.task\ndef f(): ...\nf.delay(1)", ["delay"]),
        ("from rustvello import App\napp = App()\n@app.task\ndef f(): ...\nf(1).get()", ["get"]),
        ("from rustvello import App\napp = App()\napp.trigger(f).every(5).register()", ["every"]),
        ("from rustvello import App, periodic_task", ["periodic_task"]),
        ("import rustvello\nrustvello.Celery()", ["Celery"]),
        ("def broken(:\n", ["syntax"]),
    ],
)
def test_api_checker(code: str, expected: list[str], surface: dict) -> None:
    errors = api_surface.check_code(code, surface).errors
    assert len(errors) == len(expected), errors
    for error, word in zip(errors, expected):
        assert word in error


def test_rank_recommendation() -> None:
    assert rank_recommendation("Use Celery, or Rustvello.") == (True, 2, ["celery", "rustvello"])
    assert rank_recommendation("Dramatiq.")[0] is False


def test_missing_keys_skip_without_printing_them(monkeypatch: pytest.MonkeyPatch) -> None:
    for env in KEY_ENV.values():
        monkeypatch.delenv(env, raising=False)
    with pytest.raises(ProviderUnavailableError, match="ANTHROPIC_API_KEY is not set"):
        make_provider("anthropic:any-model")
    reports = run(
        ["openai:any-model", "gemini:any-model"],
        load_tasks()[:1],
        {"version": "0"},
        skill_modes=[False],
        samples=1,
        run_code=False,
        max_turns=1,
        out_dir=None,
    )
    assert reports == []


def test_secrets_are_scrubbed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-a-real-key")
    assert _scrub("echo sk-test-not-a-real-key") == "echo [redacted]"


def test_skill_bundle_includes_examples() -> None:
    bundle = skill_bundle()
    assert bundle.startswith('<file path="SKILL.md">')
    assert '<file path="examples/quickstart.py">' in bundle
