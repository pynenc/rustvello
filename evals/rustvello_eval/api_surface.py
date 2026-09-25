"""The public Python API of Rustvello, used to flag wrong or nonexistent API use.

The surface is introspected from the installed ``rustvello`` wheel. A snapshot
(``evals/api_surface.json``) is committed so the grader also works where the wheel
is not installed; ``python evals/run.py api-surface --write`` refreshes it and the
harness tests fail when the snapshot and the installed wheel disagree.
"""

from __future__ import annotations

import ast
import inspect
import json

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SNAPSHOT = Path(__file__).resolve().parent.parent / "api_surface.json"

# Methods of builtins every object has; never reported as unknown.
_OBJECT_ATTRS = set(dir(object))


def introspect() -> dict[str, Any]:
    """Read the API surface from the installed ``rustvello`` package."""
    import rustvello

    from rustvello.app import App, Invocation, TaskHandle, _TriggerBuilder

    def public(cls: type) -> list[str]:
        return sorted(a for a in dir(cls) if not a.startswith("_"))

    def params(fn: Any) -> list[str]:
        return sorted(p for p in inspect.signature(fn).parameters if p != "self")

    return {
        "version": rustvello.__version__,
        "module": sorted(a for a in dir(rustvello) if not a.startswith("_")),
        "classes": {
            "App": public(App),
            "Invocation": public(Invocation),
            "TaskHandle": public(TaskHandle),
            "TriggerBuilder": public(_TriggerBuilder),
        },
        "signatures": {
            "App": params(App.__init__),
            "App.task": params(App.task),
            "App.workflow": params(App.workflow),
            "App.run": params(App.run),
            "App.start_monitor": params(App.start_monitor),
            "Invocation.result": params(Invocation.result),
            "Invocation.result_async": params(Invocation.result_async),
            "TriggerBuilder.on_cron": params(_TriggerBuilder.on_cron),
            "TriggerBuilder.on_interval": params(_TriggerBuilder.on_interval),
        },
    }


def load(prefer_installed: bool = True) -> dict[str, Any]:
    """The installed wheel's surface, or the committed snapshot without it."""
    if prefer_installed:
        try:
            return introspect()
        except ImportError:
            pass
    return json.loads(SNAPSHOT.read_text())


@dataclass
class ApiReport:
    """Wrong or nonexistent API uses found in one piece of code."""

    errors: list[str] = field(default_factory=list)
    parsed: bool = True


def check_code(code: str, surface: dict[str, Any]) -> ApiReport:
    """Flag Rustvello API uses in ``code`` that the surface does not have.

    Recognized: ``from rustvello import X``, ``rustvello.X``, attributes and
    keyword arguments on ``App(...)`` objects, on task handles (functions
    decorated with ``@app.task``/``@app.workflow``), on ``app.trigger(...)``
    builders and on invocations (``handle(...)`` results). Anything else is not
    judged, so the count is a lower bound.
    """
    report = ApiReport()
    try:
        tree = ast.parse(code)
    except SyntaxError as error:
        report.parsed = False
        report.errors.append(f"syntax error: {error.msg} (line {error.lineno})")
        return report
    _ApiVisitor(surface, report).visit(tree)
    return report


class _ApiVisitor(ast.NodeVisitor):
    def __init__(self, surface: dict[str, Any], report: ApiReport) -> None:
        self.surface = surface
        self.report = report
        self.module = set(surface["module"])
        self.classes = {k: set(v) | _OBJECT_ATTRS for k, v in surface["classes"].items()}
        self.signatures = {k: set(v) for k, v in surface["signatures"].items()}
        self.module_aliases: set[str] = set()
        self.app_names: set[str] = set()
        self.app_classes: set[str] = {"App"}
        self.task_names: set[str] = set()
        self.invocation_names: set[str] = set()

    # -- helpers ---------------------------------------------------------

    def _error(self, node: ast.AST, message: str) -> None:
        line = getattr(node, "lineno", "?")
        self.report.errors.append(f"line {line}: {message}")

    def _is_app_call(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Call):
            return False
        func = node.func
        if isinstance(func, ast.Name):
            return func.id in self.app_classes
        return (
            isinstance(func, ast.Attribute)
            and func.attr == "App"
            and isinstance(func.value, ast.Name)
            and func.value.id in self.module_aliases
        )

    def _is_task_call(self, node: ast.AST) -> bool:
        return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in self.task_names

    def _check_kwargs(self, node: ast.Call, signature: str) -> None:
        allowed = self.signatures.get(signature)
        if allowed is None:
            return
        for keyword in node.keywords:
            if keyword.arg is not None and keyword.arg not in allowed:
                self._error(node, f"{signature}() has no parameter {keyword.arg!r}")

    def _check_attr(self, node: ast.Attribute, cls: str, label: str) -> None:
        if node.attr not in self.classes[cls]:
            self._error(node, f"{label} has no attribute {node.attr!r}")

    # -- visitors --------------------------------------------------------

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "rustvello" or alias.name.startswith("rustvello."):
                self.module_aliases.add(alias.asname or "rustvello")
                sub = alias.name.split(".")[1:2]
                if sub and sub[0] not in self.module and sub[0] not in {"app", "worker"}:
                    self._error(node, f"module rustvello has no submodule {sub[0]!r}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "rustvello":
            for alias in node.names:
                if alias.name not in self.module:
                    self._error(node, f"rustvello has no export {alias.name!r}")
                elif alias.name == "App":
                    self.app_classes.add(alias.asname or "App")
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if self._is_app_call(node.value):
            self.app_names.update(targets)
        elif self._is_task_call(node.value):
            self.invocation_names.update(targets)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._register_task(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._register_task(node)
        self.generic_visit(node)

    def _register_task(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id in self.app_names
                and target.attr in {"task", "workflow"}
            ):
                self.task_names.add(node.name)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if self._is_app_call(node):
            self._check_kwargs(node, "App")
        elif isinstance(func, ast.Attribute):
            owner = func.value
            if isinstance(owner, ast.Name) and owner.id in self.app_names:
                self._check_kwargs(node, f"App.{func.attr}")
            elif _is_trigger_chain(owner, self.app_names):
                self._check_kwargs(node, f"TriggerBuilder.{func.attr}")
            elif isinstance(owner, ast.Name) and owner.id in self.invocation_names:
                self._check_kwargs(node, f"Invocation.{func.attr}")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        owner = node.value
        if isinstance(owner, ast.Name):
            if owner.id in self.module_aliases:
                if node.attr not in self.module and node.attr not in {"app", "worker"}:
                    self._error(node, f"rustvello has no attribute {node.attr!r}")
            elif owner.id in self.app_names:
                self._check_attr(node, "App", "App")
            elif owner.id in self.task_names:
                self._check_attr(node, "TaskHandle", f"task {owner.id!r}")
            elif owner.id in self.invocation_names:
                self._check_attr(node, "Invocation", "Invocation")
        elif _is_trigger_chain(owner, self.app_names):
            self._check_attr(node, "TriggerBuilder", "trigger builder")
        elif self._is_task_call(owner):
            self._check_attr(node, "Invocation", "Invocation")
        self.generic_visit(node)


def _is_trigger_chain(node: ast.AST, app_names: set[str]) -> bool:
    """``app.trigger(x)`` optionally followed by builder calls (``.on_cron(...)``)."""
    while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        owner = node.func.value
        if node.func.attr == "trigger" and isinstance(owner, ast.Name) and owner.id in app_names:
            return True
        node = owner
    return False
