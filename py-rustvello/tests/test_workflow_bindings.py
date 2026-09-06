"""Workflow coverage for the standalone Python binding layer."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from rustvello import App, TaskConfig, WorkflowRoot, get_current_workflow_info, workflow_root
from rustvello.app import TaskHandle
from rustvello.rustvello import RustvelloError


def test_task_config_exposes_workflow_flag() -> None:
    cfg = TaskConfig(is_workflow_task=True)

    assert cfg.is_workflow_task is True
    assert "is_workflow_task=true" in repr(cfg)


def test_workflow_root_requires_workflow_context() -> None:
    with pytest.raises(RustvelloError, match="workflow"):
        workflow_root()


def test_workflow_decorator_marks_task_as_workflow() -> None:
    app = App(app_id="test_workflow_decorator", dev_mode_force_sync=True)

    @app.workflow
    def root(value: str) -> str:
        return value

    assert isinstance(root, TaskHandle)
    key = f"{root._language}::{root._module}.{root._name}"
    assert root.is_workflow_task is True
    assert app._task_configs[key]["is_workflow_task"] is True
    assert app._task_configs[key]["blocking"] is True


def test_regular_task_is_not_workflow() -> None:
    app = App(app_id="test_regular_task_config", dev_mode_force_sync=True)

    @app.task
    def ordinary(value: str) -> str:
        return value

    key = f"{ordinary._language}::{ordinary._module}.{ordinary._name}"
    assert ordinary.is_workflow_task is False
    assert app._task_configs[key]["is_workflow_task"] is False


def test_sqlite_workflow_execution_records_run_and_root_ops(tmp_path) -> None:
    app = App(
        app_id="test_sqlite_workflow_execution",
        backend="sqlite",
        db_path=str(tmp_path / "workflow.db"),
    )

    @app.workflow
    def root(label: str) -> dict[str, object]:
        root_ops = workflow_root()
        assert isinstance(root_ops, WorkflowRoot)
        info = get_current_workflow_info()
        return {
            "label": label,
            "workflow": info,
            "random": root_ops.random(),
            "timestamp": root_ops.utc_now(),
            "uuid": root_ops.uuid(),
        }

    invocation = root("order-123")
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)

    assert runner.run_one() is True
    result = invocation.result(timeout=5)

    assert result["label"] == "order-123"
    assert 0.0 <= result["random"] < 1.0
    datetime.fromisoformat(result["timestamp"])
    assert len(result["uuid"]) == 36
    workflow_id, workflow_type, parent_id = result["workflow"]
    assert workflow_id == str(invocation.id)
    assert workflow_type == f"python::{root._module}.{root._name}"
    assert parent_id is None

    state_backend = app._backend_objects["state_backend"]
    runs = [json.loads(raw) for raw in state_backend.get_workflow_runs(root._module, root._name)]
    assert any(run["workflow_id"] == str(invocation.id) for run in runs)
    members = state_backend.get_workflow_invocations(str(invocation.id))
    assert str(invocation.id) in members
