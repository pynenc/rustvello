"""Rustvello standalone application — lightweight task queue with a clean DX layer.

Usage::

    from rustvello import App

    app = App()

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    inv = add(1, 2)
    result = inv.result(timeout=30)   # blocks, returns 3

Backend selection::

    app = App(backend="sqlite", db_path="./tasks.db")
    app = App(backend="redis", redis_url="redis://localhost:6379")

Running a persistent worker::

    app.run()  # blocks, processes queued invocations
"""

import inspect
import json
import threading
import time
from typing import Any, Callable, TypeVar

from rustvello.backends import create_backends as _create_backends
from rustvello.rustvello import (
    AppConfig,
    InvocationId,
    InvocationStatus,
    RustTaskRunnerBuilder,
    Rustvello,
    TaskConfig,
)

F = TypeVar("F", bound=Callable[..., Any])

__all__ = ["App", "Invocation", "TaskHandle"]


_SENTINEL = object()  # marks "no pre-computed result"


class Invocation:
    """Handle to a submitted task invocation.

    Obtain one by calling a :class:`TaskHandle`::

        inv = my_task(arg1, arg2)
        result = inv.result(timeout=30)

    In ``dev_mode_force_sync`` mode the result is computed immediately and
    stored on the object, so ``.result()`` returns without polling.
    """

    def __init__(
        self,
        app: "App",
        invocation_id: InvocationId,
        *,
        sync_result: Any = _SENTINEL,
        sync_status: InvocationStatus | None = None,
    ) -> None:
        self._app = app
        self._invocation_id = invocation_id
        self._sync_result = sync_result  # _SENTINEL → not pre-computed
        self._sync_status = sync_status

    @property
    def id(self) -> InvocationId:
        return self._invocation_id

    @property
    def status(self) -> InvocationStatus:
        if self._sync_status is not None:
            return self._sync_status
        return self._app._engine.get_status(self._invocation_id)

    def result(self, timeout: float = 60.0, poll_interval: float = 0.05) -> Any:
        """Block until the result is available or *timeout* seconds have elapsed.

        Returns the deserialized result (parsed from JSON).

        Raises:
            RuntimeError: if the invocation reached a FAILED terminal state.
            TimeoutError: if the timeout is reached before a terminal state.
        """
        # Fast path: sync mode — result already computed
        if self._sync_result is not _SENTINEL:
            return self._sync_result

        deadline = time.monotonic() + timeout
        while True:
            status = self.status
            if status.is_terminal():
                str_status = str(status)
                if str_status == "FAILED":
                    raw_err = self._app._engine.get_result(self._invocation_id)
                    raise RuntimeError(f"Task failed: {raw_err}")
                raw = self._app._engine.get_result(self._invocation_id)
                if raw is None:
                    return None
                return json.loads(raw)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Invocation {self._invocation_id} still {status} " f"after {timeout}s")
            time.sleep(poll_interval)


class TaskHandle:
    """A registered task. Calling the handle submits the task and returns an :class:`Invocation`."""

    def __init__(
        self,
        app: "App",
        func: Callable[..., Any],
        module: str,
        name: str,
    ) -> None:
        self._app = app
        self._func = func
        self._module = module
        self._name = name
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__wrapped__ = func

    def __call__(self, *args: Any, **kwargs: Any) -> Invocation:
        """Submit the task. Positional and keyword args are both accepted."""
        sig = inspect.signature(self._func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        serialized = {k: json.dumps(v) for k, v in bound.arguments.items()}
        return self._dispatch(serialized)

    def submit(self, **kwargs: Any) -> Invocation:
        """Explicit keyword-only submission (alternative to calling the handle)."""
        serialized = {k: json.dumps(v) for k, v in kwargs.items()}
        return self._dispatch(serialized)

    def _make_rust_wrapper(self) -> Callable[[str], str]:
        """Return a fresh wrapper callable suitable for ``register_task``."""
        fn = self._func

        def _wrapper(args_json: str) -> str:
            args_dict: dict[str, str] = json.loads(args_json)
            deserialized = {k: json.loads(v) for k, v in args_dict.items()}
            result = fn(**deserialized)
            return json.dumps(result)

        return _wrapper

    def _dispatch(self, serialized: dict[str, str]) -> Invocation:
        """Route to call_sync or submit depending on the app mode."""
        if self._app._dev_mode_force_sync:
            raw = self._app._engine.call_sync(self._module, self._name, serialized)
            value = json.loads(raw) if raw is not None else None
            return Invocation(
                self._app,
                InvocationId(),  # synthetic — not stored in the broker
                sync_result=value,
                sync_status=InvocationStatus.success(),
            )
        inv_id = self._app._engine.submit(self._module, self._name, serialized)
        return Invocation(self._app, inv_id)


class App:
    """Rustvello standalone application.

    Provides a minimal, ergonomic task-queue interface on top of the compiled
    ``Rustvello`` PyO3 class.  All task arguments and results are JSON.

    Args:
        app_id: Unique identifier for this application instance.
        dev_mode_force_sync: When ``True``, tasks execute synchronously in the
            calling thread — useful for testing without a separate worker.
        backend: Backend type — ``"memory"`` (default), ``"sqlite"``,
            ``"redis"``, ``"postgres"``, ``"mongo"``, or ``"rabbitmq"``
            (broker-only, requires another backend for state).
        db_path: SQLite database file path (for ``backend="sqlite"``).
        redis_url: Redis connection URL (for ``backend="redis"``).
        postgres_url: PostgreSQL connection string (for ``backend="postgres"``).
        mongo_url: MongoDB connection URI (for ``backend="mongo"``).
        mongo_db: MongoDB database name (for ``backend="mongo"``).

    Example::

        app = App(dev_mode_force_sync=True)

        @app.task
        def add(x: int, y: int) -> int:
            return x + y

        inv = add(1, 2)
        assert inv.result(timeout=5) == 3

    Backend selection::

        app = App(backend="sqlite", db_path="./tasks.db")
        app = App(backend="redis", redis_url="redis://localhost:6379")
    """

    def __init__(
        self,
        app_id: str = "rustvello",
        dev_mode_force_sync: bool = False,
        *,
        backend: str = "memory",
        db_path: str = "./rustvello.db",
        redis_url: str = "redis://127.0.0.1:6379",
        postgres_url: str = "postgresql://localhost/rustvello",
        mongo_url: str = "mongodb://localhost:27017",
        mongo_db: str = "rustvello",
    ) -> None:
        self._app_id = app_id
        self._dev_mode_force_sync = dev_mode_force_sync
        self._backend_name = backend.lower()
        self._tasks: dict[str, TaskHandle] = {}
        self._task_configs: dict[str, dict[str, Any]] = {}
        self._backend_objects: dict[str, Any] | None = None
        self._runner = None
        self._runner_thread: threading.Thread | None = None
        self._triggers: list[_TriggerDef] = []

        config = AppConfig(
            app_id=app_id,
            dev_mode_force_sync=dev_mode_force_sync,
        )

        if self._backend_name == "memory":
            self._engine = Rustvello(config=config)
        else:
            backends = _create_backends(
                self._backend_name,
                app_id,
                db_path=db_path,
                redis_url=redis_url,
                postgres_url=postgres_url,
                mongo_url=mongo_url,
                mongo_db=mongo_db,
            )
            self._backend_objects = backends
            self._engine = Rustvello.from_backends(
                backends["orchestrator"],
                backends["state_backend"],
                backends["broker"],
                backends["trigger"],
                backends["client_data_store"],
                config,
            )

    def task(
        self,
        func: Callable[..., Any] | None = None,
        *,
        max_retries: int = 0,
        cache_results: bool = False,
        running_concurrency: int | None = None,
        concurrency: str = "unlimited",
        key_arguments: list[str] | None = None,
        blocking: bool = False,
        parallel_batch_size: int = 100,
        reroute_on_cc: bool = False,
    ) -> Any:
        """Register a function as a distributed task.

        Can be used as a bare decorator or with keyword arguments::

            @app.task
            def simple(x: int) -> int: ...

            @app.task(max_retries=3, cache_results=True)
            def resilient(x: int) -> int: ...

            @app.task(concurrency="keys", key_arguments=["user_id"])
            def per_user(user_id: str, data: str) -> str: ...

        Args:
            max_retries: How many times to retry on failure (default 0).
            cache_results: Cache results so identical calls re-use the stored result.
            running_concurrency: Maximum simultaneous executions (``None`` = unlimited).
            concurrency: Concurrency control mode — ``"unlimited"``, ``"task"``,
                ``"arguments"``, or ``"keys"``.
            key_arguments: Argument names for ``concurrency="keys"`` mode.
            blocking: If ``True``, the task will block a runner slot while
                waiting for sub-task results.
            parallel_batch_size: How many invocations to retrieve per batch.
            reroute_on_cc: If ``True``, reroute invocations back to the broker
                when concurrency-controlled (instead of failing).

        Returns:
            A :class:`TaskHandle` that submits the task when called.
        """
        extra_config = {
            "concurrency": concurrency,
            "key_arguments": key_arguments or [],
            "blocking": blocking,
            "parallel_batch_size": parallel_batch_size,
            "reroute_on_cc": reroute_on_cc,
        }

        def decorator(fn: Callable[..., Any]) -> TaskHandle:
            module = fn.__module__
            name = fn.__name__

            task_config = TaskConfig(
                max_retries=max_retries,
                cache_results=cache_results,
                running_concurrency=running_concurrency,
            )

            def _rust_wrapper(args_json: str) -> str:
                args_dict: dict[str, str] = json.loads(args_json)
                deserialized = {k: json.loads(v) for k, v in args_dict.items()}
                result = fn(**deserialized)
                return json.dumps(result)

            self._engine.register_task(module, name, _rust_wrapper, task_config)
            handle = TaskHandle(self, fn, module, name)
            self._tasks[f"{module}.{name}"] = handle
            self._task_configs[f"{module}.{name}"] = extra_config
            return handle

        if func is not None:
            return decorator(func)
        return decorator

    # --- Runner ----------------------------------------------------------

    def run(
        self,
        *,
        num_workers: int = 4,
        idle_sleep_ms: int = 50,
        block: bool = True,
    ) -> None:
        """Start a persistent task runner.

        Processes invocations from the broker, executes registered tasks,
        manages heartbeats and recovery.

        Args:
            num_workers: Number of concurrent worker slots.
            idle_sleep_ms: Sleep interval when no work is available (ms).
            block: If ``True`` (default), blocks until :meth:`stop` is
                called. If ``False``, starts the runner in a background thread.
        """
        runner = self._build_runner(num_workers=num_workers, idle_sleep_ms=idle_sleep_ms)
        self._runner = runner

        if block:
            runner.run()
        else:
            self._runner_thread = threading.Thread(target=runner.run, daemon=True, name="rustvello-runner")
            self._runner_thread.start()

    def stop(self) -> None:
        """Shut down the runner gracefully."""
        if self._runner is not None:
            self._runner.shutdown()
        if self._runner_thread is not None:
            self._runner_thread.join(timeout=30)
            self._runner_thread = None

    def _build_runner(self, *, num_workers: int = 4, idle_sleep_ms: int = 50) -> Any:
        builder = RustTaskRunnerBuilder(self._app_id)

        if self._backend_objects is not None:
            builder.with_backends(
                self._backend_objects["broker"],
                self._backend_objects["orchestrator"],
                self._backend_objects["state_backend"],
                self._backend_objects.get("trigger"),
            )
        else:
            builder.memory()

        builder.with_num_workers(num_workers)
        builder.with_idle_sleep(idle_sleep_ms)

        for key, handle in self._tasks.items():
            extra = self._task_configs.get(key, {})
            builder.register_task(
                handle._module,
                handle._name,
                handle._make_rust_wrapper(),
                concurrency_control=extra.get("concurrency", "unlimited"),
                key_arguments=extra.get("key_arguments", []),
                reroute_on_cc=extra.get("reroute_on_cc", False),
                max_retries=0,
                retry_for_errors=[],
                registration_concurrency="unlimited",
                cache_results=False,
                disable_cache_args=[],
                on_diff_non_key_args_raise=False,
                parallel_batch_size=extra.get("parallel_batch_size", 100),
                force_new_workflow=False,
            )

        return builder.build()

    # --- Triggers --------------------------------------------------------

    def trigger(self, task_handle: "TaskHandle") -> "_TriggerBuilder":
        """Create a trigger that fires the given task.

        Returns a :class:`_TriggerBuilder` for fluent configuration::

            app.trigger(cleanup).on_cron("0 */5 * * * *").register()
        """
        return _TriggerBuilder(self, task_handle)

    # --- Introspection ---------------------------------------------------

    @property
    def tasks(self) -> dict[str, "TaskHandle"]:
        """Registered tasks as ``{module.name: TaskHandle}``."""
        return dict(self._tasks)

    @property
    def backend(self) -> str:
        """The backend name (``"memory"``, ``"sqlite"``, etc.)."""
        return self._backend_name


# ── Helper types ────────────────────────────────────────────────────────


class _TriggerDef:
    """Internal representation of a registered trigger."""

    __slots__ = ("task_key", "kind", "schedule", "kwargs")

    def __init__(self, task_key: str, kind: str, schedule: str, **kwargs: Any) -> None:
        self.task_key = task_key
        self.kind = kind
        self.schedule = schedule
        self.kwargs = kwargs


class _TriggerBuilder:
    """Fluent builder for triggers.

    Usage::

        app.trigger(my_task).on_cron("0 */5 * * * *").with_args(x=1).register()
    """

    def __init__(self, app: App, task_handle: TaskHandle) -> None:
        self._app = app
        self._task_handle = task_handle
        self._kind: str | None = None
        self._schedule: str = ""
        self._kwargs: dict[str, Any] = {}

    def on_cron(self, expression: str) -> "_TriggerBuilder":
        """Fire the task on a cron schedule.

        Args:
            expression: A cron expression (6-field: sec min hour day month weekday).
        """
        self._kind = "cron"
        self._schedule = expression
        return self

    def on_interval(self, seconds: float) -> "_TriggerBuilder":
        """Fire the task at a fixed interval.

        Args:
            seconds: Interval in seconds between trigger firings.
        """
        self._kind = "interval"
        self._schedule = str(seconds)
        return self

    def with_args(self, **kwargs: Any) -> "_TriggerBuilder":
        """Set task arguments for each trigger firing."""
        self._kwargs = kwargs
        return self

    def register(self) -> _TriggerDef:
        """Register the trigger with the application.

        Returns the :class:`_TriggerDef` for introspection.
        """
        if self._kind is None:
            raise ValueError("Must specify a trigger type (on_cron / on_interval) " "before calling register()")
        key = f"{self._task_handle._module}.{self._task_handle._name}"
        tdef = _TriggerDef(key, self._kind, self._schedule, **self._kwargs)
        self._app._triggers.append(tdef)
        return tdef
