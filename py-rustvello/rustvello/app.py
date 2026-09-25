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
    app = App(backend="mongo3", mongo_host="mongo", mongo_username="u", mongo_password="p",
              mongo_auth_source="admin", mongo_db="pynenc", broker="rabbitmq",
              rabbitmq_url="amqp://rabbitmq-service/")

Running a persistent worker::

    app.run()                      # in-process workers (I/O-bound Python)
    app.run(num_processes=8)       # one interpreter per worker (CPU-bound Python)

Async tasks::

    @app.task
    async def fetch(url: str) -> int:
        reader, writer = await asyncio.open_connection(...)
        ...

An ``async def`` task runs on an event loop owned by the worker that executes
it: one loop per worker thread (``app.run()``) or per worker process
(``app.run(num_processes=...)``), created on first use and reused. The loop
runs on the worker's own thread, so the invocation context
(:meth:`App.current_invocation`, child submissions, ``workflow_root()``) and
the OpenTelemetry context are the same as for a synchronous task. While the
loop waits on I/O it releases the GIL, so other workers keep running; CPU-bound
code inside a coroutine still holds the GIL and stalls that worker's loop.
Tasks a coroutine leaves running when it returns are cancelled before the
worker takes its next invocation. When the attempt times out or its invocation
is cancelled, the coroutine is cancelled on its loop (``CancelledError`` at the
next ``await``).
"""

from __future__ import annotations

import asyncio
import atexit
import contextvars
import dataclasses
import inspect
import json
import math
import os
import sys
import threading
import time
import warnings
from collections.abc import Sequence
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable, TypeVar

from rustvello.backends import create_backends as _create_backends
from rustvello.rustvello import (
    AppConfig,
    InvocationCancelledError,
    InvocationId,
    InvocationStatus,
    RustTaskRunnerBuilder,
    Rustvello,
    TaskConfig,
    get_current_invocation_id,
    get_current_num_retries,
    get_current_trace_context,
    on_attempt_abandoned,
    wait_runner_python_calls,
)

F = TypeVar("F", bound=Callable[..., Any])

__all__ = ["App", "CurrentInvocation", "ForeignTaskHandle", "Invocation", "TaskHandle", "TaskLanguage"]


_SENTINEL = object()  # marks "no pre-computed result"


def _current_trace_carrier() -> tuple[str | None, str | None]:
    """Inject the active Python OTel context without requiring the SDK."""
    try:
        from opentelemetry.propagate import inject
    except ImportError:
        return None, None
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier.get("traceparent"), carrier.get("tracestate")


@contextmanager
def _invocation_trace_context() -> Any:
    """Attach Rustvello's execution span identity while executing a Python task."""
    carrier = get_current_trace_context()
    if carrier is None or carrier[0] is None:
        yield
        return
    try:
        from opentelemetry.context import attach, detach
        from opentelemetry.propagate import extract
    except ImportError:
        yield
        return
    values = {"traceparent": carrier[0]}
    if carrier[1] is not None:
        values["tracestate"] = carrier[1]
    token = attach(extract(values))
    try:
        yield
    finally:
        detach(token)


def _mongo_url_from_parts(
    host: str | None,
    port: int | None,
    username: str | None,
    password: str | None,
    auth_source: str | None,
) -> str:
    """Assemble a Mongo URI from host/port/credential fields."""
    from urllib.parse import quote

    credentials = ""
    if username:
        credentials = quote(username, safe="")
        if password:
            credentials += ":" + quote(password, safe="")
        credentials += "@"
    query = f"/?authSource={quote(auth_source, safe='')}" if auth_source else ""
    return f"mongodb://{credentials}{host or 'localhost'}:{port or 27017}{query}"


class _WorkerLoop:
    """The event loop of one worker thread; closed when the thread goes away."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()

    def __del__(self) -> None:
        loop = self.loop
        if not loop.is_closed() and not loop.is_running():
            loop.close()


_worker_loops = threading.local()


# Runner threads may still be in Python after a timeout or cancellation abandoned
# their attempt; the interpreter must not finalize under them (see
# ``wait_runner_python_calls``). ``App.stop()`` and this exit hook wait, bounded.
_BODY_DRAIN_SECONDS = 10.0


def _drain_runner_python_calls_at_exit() -> None:
    wait_runner_python_calls(_BODY_DRAIN_SECONDS)


atexit.register(_drain_runner_python_calls_at_exit)


def _worker_event_loop() -> asyncio.AbstractEventLoop:
    """This thread's reusable worker event loop (one per worker thread or process)."""
    holder: _WorkerLoop | None = getattr(_worker_loops, "holder", None)
    if holder is None or holder.loop.is_closed():
        holder = _WorkerLoop()
        _worker_loops.holder = holder
    return holder.loop


def _cancel_leftover_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel tasks a finished coroutine left behind, as ``asyncio.run`` does."""
    leftovers = [task for task in asyncio.all_tasks(loop) if not task.done()]
    if not leftovers:
        return
    for task in leftovers:
        task.cancel()
    loop.run_until_complete(asyncio.gather(*leftovers, return_exceptions=True))


def _cancel_from_runner(loop: asyncio.AbstractEventLoop, task: asyncio.Task[Any]) -> None:
    """Cancel a task body from a runner thread (its attempt timed out or was cancelled)."""
    try:
        loop.call_soon_threadsafe(task.cancel)
    except RuntimeError:
        pass  # the worker loop is already closed: nothing left to stop


def _run_coroutine(coro: Any) -> Any:
    """Drive a task coroutine to completion from synchronous worker code.

    Runs on this thread's worker loop, keeping the thread's invocation context. If a
    loop is already running here (a dev-mode call made from async code), the
    coroutine runs to completion on a helper thread with its own loop instead.

    When the runner abandons the attempt (execution deadline or cancellation), the
    coroutine's task is cancelled on its loop, so the body stops at its next ``await``.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = _worker_event_loop()
        task = loop.create_task(coro)

        on_attempt_abandoned(lambda: _cancel_from_runner(loop, task))
        try:
            return loop.run_until_complete(task)
        finally:
            _cancel_leftover_tasks(loop)

    outcome: dict[str, Any] = {}
    context = contextvars.copy_context()

    def _in_helper_thread() -> None:
        try:
            outcome["value"] = context.run(asyncio.run, coro)
        except BaseException as error:  # noqa: BLE001 - re-raised in the caller
            outcome["error"] = error

    helper = threading.Thread(target=_in_helper_thread, name="rustvello-async-dev")
    helper.start()
    helper.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def _call_task_function(fn: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
    """Call a task body; an ``async def`` body is awaited on the worker's event loop."""
    result = fn(**kwargs)
    if inspect.iscoroutine(result):
        return _run_coroutine(result)
    return result


def _run_python_task(fn: Callable[..., Any], args_json: str) -> str:
    args_dict: dict[str, str] = json.loads(args_json)
    deserialized = {key: json.loads(value) for key, value in args_dict.items()}
    with _invocation_trace_context():
        return json.dumps(_call_task_function(fn, deserialized))


class TaskLanguage(str, Enum):
    Rust = "rust"
    Python = "python"


@dataclasses.dataclass(frozen=True)
class CurrentInvocation:
    """What :meth:`App.current_invocation` knows about the task attempt running right now."""

    invocation_id: str
    task_key: str
    num_retries: int
    arguments: dict[str, Any]


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

    def cancel(self) -> bool:
        """Cancel this invocation if it has not finished.

        Returns ``True`` when this call cancelled it, ``False`` when it had
        already finished. Queued or backing-off invocations never run; a
        running attempt is abandoned by its worker (an ``async def`` body is
        cancelled at its next ``await``; a synchronous function keeps running in
        its thread but its result is discarded). Side effects already performed are not undone.
        """
        if self._sync_status is not None:
            return False  # dev mode: already executed inline
        return bool(self._app._engine.cancel(self._invocation_id))

    def result(self, timeout: float = 60.0, poll_interval: float = 0.05) -> Any:
        """Block until the result is available or *timeout* seconds have elapsed.

        Returns the deserialized result (parsed from JSON).

        Raises:
            RuntimeError: if the invocation reached a FAILED terminal state
                (a task that exceeded its ``timeout`` fails with
                ``TaskTimeoutError`` in the message).
            InvocationCancelledError: if the invocation was cancelled.
            TimeoutError: if the timeout is reached before a terminal state.
        """
        # Fast path: sync mode — result already computed
        if self._sync_result is not _SENTINEL:
            return self._sync_result
        self._mark_waiting()
        deadline = time.monotonic() + timeout
        while True:
            done, value = self._poll(deadline, timeout)
            if done:
                return value
            time.sleep(poll_interval)

    async def result_async(self, timeout: float = 60.0, poll_interval: float = 0.05) -> Any:
        """Await the result without blocking the event loop (for ``async def`` tasks).

        Same contract as :meth:`result`; polls with ``asyncio.sleep``.
        """
        if self._sync_result is not _SENTINEL:
            return self._sync_result
        self._mark_waiting()
        deadline = time.monotonic() + timeout
        while True:
            done, value = self._poll(deadline, timeout)
            if done:
                return value
            await asyncio.sleep(poll_interval)

    def _mark_waiting(self) -> None:
        """Inside a running task, record that it waits on this invocation."""
        current_invocation_id = get_current_invocation_id()
        if current_invocation_id is not None:
            current = InvocationId.from_string(current_invocation_id)
            if str(current) != str(self._invocation_id):
                self._app._engine.set_waiting_for(current, self._invocation_id)

    def _poll(self, deadline: float, timeout: float) -> tuple[bool, Any]:
        """One status check: ``(True, result)`` when terminal; raises on failure, cancel or timeout."""
        status = self.status
        if status.is_terminal():
            str_status = str(status)
            if str_status == "FAILED":
                raise RuntimeError(f"Task failed: {self._app._failure_message(self._invocation_id)}")
            if str_status == "CANCELLED":
                raise InvocationCancelledError(f"invocation {self._invocation_id} was cancelled")
            raw = self._app._engine.get_result(self._invocation_id)
            return True, None if raw is None else json.loads(raw)
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Invocation {self._invocation_id} still {status} after {timeout}s")
        return False, None


class TaskHandle:
    """A registered task. Calling the handle submits the task and returns an :class:`Invocation`."""

    def __init__(
        self,
        app: "App",
        func: Callable[..., Any],
        language: str,
        module: str,
        name: str,
    ) -> None:
        self._app = app
        self._func = func
        self._language = language
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

    def submit_with_id(self, invocation_id: InvocationId, **kwargs: Any) -> Invocation:
        """Retry an ambiguous durable submission with the same ID and content.

        Requires the co-located SQLite runtime; synchronous dev mode is not durable.
        Keep the same parent/workflow/W3C context when replaying a submission.
        """
        if self._app._dev_mode_force_sync:
            raise ValueError("durable submission is unavailable in synchronous dev mode")
        return self._dispatch({k: json.dumps(v) for k, v in kwargs.items()}, invocation_id)

    def submit_with_key(self, key: str, **kwargs: Any) -> Invocation:
        """Submit at most once per idempotency ``key`` (a request id, an order id).

        The invocation id is ``InvocationId.from_key(task key, key)``, submitted
        through :meth:`submit_with_id`: repeating the call with the same key and
        arguments returns the same invocation, the same key with other arguments
        raises. Needs SQLite or PostgreSQL. The key deduplicates submissions only;
        the task body still runs at least once, so keep its side effects idempotent.
        """
        invocation_id = InvocationId.from_key(f"{self._language}::{self._module}.{self._name}", key)
        return self.submit_with_id(invocation_id, **kwargs)

    @property
    def is_workflow_task(self) -> bool:
        """Whether this handle was registered as an explicit workflow root."""
        key = f"{self._language}::{self._module}.{self._name}"
        return bool(self._app._task_configs.get(key, {}).get("is_workflow_task", False))

    def _make_rust_wrapper(self) -> Callable[[str], str]:
        """Return a fresh wrapper callable suitable for ``register_task``."""
        fn = self._func

        def _wrapper(args_json: str) -> str:
            return _run_python_task(fn, args_json)

        return _wrapper

    def _dispatch(self, serialized: dict[str, str], invocation_id: InvocationId | None = None) -> Invocation:
        """Route to call_sync or submit depending on the app mode."""
        if self._app._dev_mode_force_sync:
            deserialized = {key: json.loads(value) for key, value in serialized.items()}
            result = _call_task_function(self._func, deserialized)
            raw = json.dumps(result)
            value = json.loads(raw)
            return Invocation(
                self._app,
                InvocationId(),  # synthetic — not stored in the broker
                sync_result=value,
                sync_status=InvocationStatus.success(),
            )
        traceparent, tracestate = _current_trace_carrier()
        inv_id = self._app._engine.submit_task(
            self._language,
            self._module,
            self._name,
            serialized,
            traceparent,
            tracestate,
            invocation_id,
        )
        return Invocation(self._app, inv_id)


class ForeignTaskHandle:
    """A task implemented by another language runtime."""

    def __init__(
        self,
        app: "App",
        func: Callable[..., Any],
        language: TaskLanguage,
        module: str,
        name: str,
        queue: str = "default",
        priority: float = 0.0,
    ) -> None:
        self._app = app
        self._func = func
        self._language = language.value
        self._queue = queue
        self._priority = priority
        self._module = module
        self._name = name
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__wrapped__ = func

    def __call__(self, *args: Any, **kwargs: Any) -> Invocation:
        sig = inspect.signature(self._func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        serialized = {k: json.dumps(v) for k, v in bound.arguments.items()}
        traceparent, tracestate = _current_trace_carrier()
        inv_id = self._app._engine.submit_task(
            self._language,
            self._module,
            self._name,
            serialized,
            traceparent,
            tracestate,
        )
        return Invocation(self._app, inv_id)

    def submit(self, **kwargs: Any) -> Invocation:
        return self(**kwargs)


class App:
    """Rustvello standalone application.

    Provides a minimal, ergonomic task-queue interface on top of the compiled
    ``Rustvello`` PyO3 class.  All task arguments and results are JSON.

    Args:
        app_id: Unique identifier for this application instance.
        dev_mode_force_sync: When ``True``, tasks execute synchronously in the
            calling thread — useful for testing without a separate worker.
            Left unset it follows the resolved configuration, so
            ``RUSTVELLO__DEV_MODE_FORCE_SYNC=true`` switches a whole test suite to
            inline execution without touching any code.
        backend: Backend type — ``"memory"`` (default), ``"sqlite"``,
            ``"redis"``, ``"postgres"``, ``"mongo"``, or ``"rabbitmq"``
            (broker-only, requires another backend for state).
        db_path: SQLite database file path (for ``backend="sqlite"``).
        redis_url: Redis connection URL (for ``backend="redis"``).
        postgres_url: PostgreSQL connection string (for ``backend="postgres"``).
        mongo_url: MongoDB connection URI (for ``backend="mongo"``).
        mongo_db: MongoDB database name (for ``backend="mongo"``).
        otlp_endpoint: OTLP/HTTP endpoint for lifecycle telemetry.
        otlp_bearer_token: Bearer token paired with ``otlp_endpoint``.

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
        dev_mode_force_sync: bool | None = None,
        *,
        backend: str = "memory",
        db_path: str = "./rustvello.db",
        sqlite_synchronous: str = "FULL",
        sqlite_busy_timeout_ms: int = 5000,
        config: AppConfig | None = None,
        redis_url: str = "redis://127.0.0.1:6379",
        postgres_url: str = "postgresql://localhost/rustvello",
        mongo_url: str | None = None,
        mongo_db: str = "rustvello",
        mongo_host: str | None = None,
        mongo_port: int | None = None,
        mongo_username: str | None = None,
        mongo_password: str | None = None,
        mongo_auth_source: str | None = None,
        broker: str | None = None,
        rabbitmq_url: str = "amqp://guest:guest@127.0.0.1:5672",
        rabbitmq_prefix: str | None = None,
        import_path: str | None = None,
        otlp_endpoint: str | None = None,
        otlp_bearer_token: str | None = None,
    ) -> None:
        if (otlp_endpoint is None) != (otlp_bearer_token is None):
            raise ValueError("otlp_endpoint and otlp_bearer_token must be provided together")
        self._app_id = app_id
        self._backend_name = backend.lower()
        self._broker_name = broker.lower() if broker else None
        self._import_path = import_path
        self._tasks: dict[str, TaskHandle] = {}
        self._foreign_tasks: dict[str, ForeignTaskHandle] = {}
        self._task_configs: dict[str, dict[str, Any]] = {}
        self._backend_objects: dict[str, Any] | None = None
        self._runner = None
        self._runner_thread: threading.Thread | None = None
        self._triggers: list[_TriggerDef] = []
        self._otlp_endpoint = otlp_endpoint
        self._otlp_bearer_token = otlp_bearer_token
        self._telemetry_stopped = False
        self._last_runner_telemetry: dict[str, int] | None = None

        if config is None:
            # RUSTVELLO__* env vars, ./pyproject.toml [tool.rustvello.app] and defaults,
            # like the Rust builder; explicit constructor arguments win.
            config = AppConfig.from_env(app_id=app_id)
        elif config.app_id != app_id:
            raise ValueError(f"AppConfig app_id {config.app_id!r} must match App app_id {app_id!r}")
        if dev_mode_force_sync is None:
            # unset: follow the resolved configuration, so RUSTVELLO__DEV_MODE_FORCE_SYNC reaches
            # an App built with an explicit AppConfig; passing the argument still wins
            dev_mode_force_sync = config.dev_mode_force_sync
        config.dev_mode_force_sync = dev_mode_force_sync
        self._dev_mode_force_sync = dev_mode_force_sync
        self._config = config

        if mongo_url is None:
            mongo_url = (
                _mongo_url_from_parts(mongo_host, mongo_port, mongo_username, mongo_password, mongo_auth_source)
                if any(
                    v is not None for v in (mongo_host, mongo_port, mongo_username, mongo_password, mongo_auth_source)
                )
                else "mongodb://localhost:27017"
            )
        backends = _create_backends(
            self._backend_name,
            app_id,
            db_path=db_path,
            sqlite_synchronous=sqlite_synchronous,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            redis_url=redis_url,
            postgres_url=postgres_url,
            mongo_url=mongo_url,
            mongo_db=mongo_db,
            broker=self._broker_name,
            rabbitmq_url=rabbitmq_url,
            rabbitmq_prefix=rabbitmq_prefix or app_id,
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
        if otlp_endpoint is not None and otlp_bearer_token is not None:
            self._engine.enable_otlp(otlp_endpoint, otlp_bearer_token)

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
        queue: str = "default",
        priority: float = 0.0,
        retry_for: tuple[type[BaseException], ...] = (),
        replace: bool = False,
        retry_delay: float = 0.0,
        retry_max_delay: float = 300.0,
        retry_backoff: float = 2.0,
        retry_jitter: str = "equal",
        timeout: float | None = None,
        retry_on_timeout: bool = True,
        _is_workflow_task: bool = False,
    ) -> Any:
        """Register a function as a distributed task.

        Can be used as a bare decorator or with keyword arguments::

            @app.task
            def simple(x: int) -> int: ...

            @app.task
            async def fetch(url: str) -> str: ...  # awaited on the worker's event loop

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
            queue: Broker queue shared by initial submission, recovery and retries.
            priority: Larger values are claimed first within a queue.
            retry_for: Exception classes that trigger a retry (matched by class name,
                as ``retry_for_errors``). Empty means every exception retries while
                ``max_retries`` allows.
            replace: Re-register a task already known under the same ``module.name``
                (module reloads, tests redefining a task) instead of raising.
            retry_delay: Seconds before the first retry (default 0: retry at once).
                Retry ``n`` waits ``min(retry_max_delay, retry_delay * retry_backoff**n)``
                with jitter. The wait is stored in the backend, not slept in a worker.
            retry_max_delay: Upper bound of the retry delay before jitter (seconds).
            retry_backoff: Growth factor of the delay per retry (>= 1.0).
            retry_jitter: ``"equal"`` (default: half the delay plus a random half),
                ``"full"`` (random in ``[0, delay]``) or ``"none"``.
            timeout: Execution deadline of one attempt in seconds (``None`` = none).
                An expired attempt fails with ``TaskTimeoutError``. An ``async def``
                body is cancelled at its next ``await``; a synchronous function
                cannot be interrupted, so its thread keeps running and its result
                is discarded (use ``num_processes`` to have the worker process
                killed instead).
            retry_on_timeout: Whether a timed-out attempt may be retried.

        Returns:
            A :class:`TaskHandle` that submits the task when called.
        """
        extra_config = {
            "max_retries": max_retries,
            "cache_results": cache_results,
            "running_concurrency": running_concurrency,
            "concurrency": concurrency,
            "key_arguments": key_arguments or [],
            "blocking": blocking,
            "parallel_batch_size": parallel_batch_size,
            "reroute_on_cc": reroute_on_cc,
            "is_workflow_task": _is_workflow_task,
            "queue": queue,
            "priority": priority,
            "retry_for_errors": [cls.__name__ for cls in retry_for],
            "retry_delay": retry_delay,
            "retry_max_delay": retry_max_delay,
            "retry_backoff": retry_backoff,
            "retry_jitter": retry_jitter,
            "timeout": timeout,
            "retry_on_timeout": retry_on_timeout,
        }

        def decorator(fn: Callable[..., Any]) -> TaskHandle:
            if inspect.isasyncgenfunction(fn):
                raise TypeError("Rustvello tasks must be a function or an async function, not an async generator")

            module = fn.__module__
            name = fn.__name__

            task_config = TaskConfig(
                max_retries=max_retries,
                cache_results=cache_results,
                running_concurrency=running_concurrency,
                is_workflow_task=_is_workflow_task,
                queue=queue,
                priority=priority,
                retry_for_errors=[cls.__name__ for cls in retry_for],
            ).with_retry_policy(
                retry_delay=retry_delay,
                retry_max_delay=retry_max_delay,
                retry_backoff=retry_backoff,
                retry_jitter=retry_jitter,
                timeout=timeout,
                retry_on_timeout=retry_on_timeout,
            )

            def _rust_wrapper(args_json: str) -> str:
                return _run_python_task(fn, args_json)

            key = f"python::{module}.{name}"
            if replace and key in self._tasks:
                self._engine.unregister_task(module, name)
            self._engine.register_task(module, name, _rust_wrapper, task_config)
            handle = TaskHandle(self, fn, "python", module, name)
            self._tasks[key] = handle
            self._task_configs[key] = extra_config
            return handle

        if func is not None:
            return decorator(func)
        return decorator

    def workflow(
        self,
        func: Callable[..., Any] | None = None,
        *,
        max_retries: int = 0,
        cache_results: bool = False,
        running_concurrency: int | None = None,
        concurrency: str = "unlimited",
        key_arguments: list[str] | None = None,
        parallel_batch_size: int = 100,
        reroute_on_cc: bool = False,
        queue: str = "default",
        priority: float = 0.0,
        retry_for: tuple[type[BaseException], ...] = (),
        replace: bool = False,
        retry_delay: float = 0.0,
        retry_max_delay: float = 300.0,
        retry_backoff: float = 2.0,
        retry_jitter: str = "equal",
        timeout: float | None = None,
        retry_on_timeout: bool = True,
    ) -> Any:
        """Register a function as an explicit workflow root.

        Workflows are submitted like tasks, but their root invocation records a
        workflow run and child task submissions inherit its workflow identity.
        Use ``rustvello.workflow_root()`` inside the workflow body for
        deterministic ``random()``, ``utc_now()``, and ``uuid()`` operations.
        """
        return self.task(
            func,
            max_retries=max_retries,
            cache_results=cache_results,
            running_concurrency=running_concurrency,
            concurrency=concurrency,
            key_arguments=key_arguments,
            blocking=True,
            parallel_batch_size=parallel_batch_size,
            reroute_on_cc=reroute_on_cc,
            queue=queue,
            priority=priority,
            retry_for=retry_for,
            replace=replace,
            retry_delay=retry_delay,
            retry_max_delay=retry_max_delay,
            retry_backoff=retry_backoff,
            retry_jitter=retry_jitter,
            timeout=timeout,
            retry_on_timeout=retry_on_timeout,
            _is_workflow_task=True,
        )

    def foreign_task(
        self,
        language: TaskLanguage,
        func: Callable[..., Any] | None = None,
        *,
        module: str | None = None,
        name: str | None = None,
        max_retries: int = 0,
        cache_results: bool = False,
        queue: str = "default",
        priority: float = 0.0,
    ) -> Any:
        """Declare a typed task implemented by another language runtime."""
        language = TaskLanguage(language)
        config = TaskConfig(
            max_retries=max_retries,
            cache_results=cache_results,
            queue=queue,
            priority=priority,
        )

        def decorator(fn: Callable[..., Any]) -> ForeignTaskHandle:
            foreign_module = module or fn.__module__
            foreign_name = name or fn.__name__
            self._engine.register_foreign_task(language.value, foreign_module, foreign_name, config)
            handle = ForeignTaskHandle(self, fn, language, foreign_module, foreign_name, queue, priority)
            self._foreign_tasks[f"{language.value}::{foreign_module}.{foreign_name}"] = handle
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
        evaluate_triggers: bool = True,
        block: bool = True,
        num_processes: int | None = None,
        queues: Sequence[str] | None = None,
        import_path: str | None = None,
        worker_env: dict[str, str] | None = None,
    ) -> None:
        """Start a persistent task runner.

        Processes invocations from the broker, executes registered tasks,
        manages heartbeats and recovery.

        Args:
            num_workers: Number of concurrent worker slots (in-process threads).
            idle_sleep_ms: Sleep interval when no work is available (ms).
            evaluate_triggers: Whether this runner should evaluate trigger conditions.
            block: If ``True`` (default), blocks until :meth:`stop` is
                called. If ``False``, starts the runner in a background thread.
            num_processes: Run task code in this many worker processes instead of
                threads. Each worker is its own interpreter (own GIL), so CPU-bound
                Python tasks run in parallel; the control plane stays here. The
                workers import the app through :attr:`import_path`.
            queues: Restrict this runner to these broker queues (overrides
                ``runner_queues`` from the configuration).
            import_path: ``"module:attribute"`` of this app for the worker
                processes; defaults to the constructor's ``import_path`` or to
                auto-discovery through ``sys.modules``.
            worker_env: Extra environment variables for the worker processes.
        """
        if queues is not None:
            self._config.runner_queues = list(queues)
        runner = self._build_runner(
            num_workers=num_workers,
            idle_sleep_ms=idle_sleep_ms,
            evaluate_triggers=evaluate_triggers,
            num_processes=num_processes,
            import_path=import_path,
            worker_env=worker_env,
        )
        self._runner = runner

        if block:
            try:
                runner.run()
            finally:
                self.stop()
        else:
            self._runner_thread = threading.Thread(target=runner.run, daemon=True, name="rustvello-runner")
            self._runner_thread.start()

    def stop(self) -> None:
        """Shut down the runner gracefully."""
        if self._runner is not None:
            self._runner.shutdown()
        if self._runner_thread is not None:
            self._runner_thread.join(timeout=30)
            if self._runner_thread.is_alive():
                raise TimeoutError("runner shutdown timed out; task execution is still draining")
            self._runner_thread = None
        # Abandoned (timed-out or cancelled) bodies are not part of the runner's
        # drain; a cancelled coroutine stops at its next await.
        if not wait_runner_python_calls(_BODY_DRAIN_SECONDS):
            warnings.warn(
                "rustvello: a timed-out or cancelled synchronous task body is still "
                "running after stop(); it keeps its thread until it returns",
                RuntimeWarning,
                stacklevel=2,
            )
        if self._runner is not None:
            if self._runner.is_running():
                return
            if self._otlp_endpoint is not None:
                self._last_runner_telemetry = dict(self._runner.telemetry_stats())
            self._runner = None
        if self._otlp_endpoint is not None and not self._telemetry_stopped:
            self._engine.shutdown_telemetry()
            self._telemetry_stopped = True

    def flush_telemetry(self, timeout_ms: int = 5_000) -> dict[str, dict[str, int]]:
        """Flush enabled lifecycle exporters and return bounded delivery counters."""
        if self._otlp_endpoint is None:
            raise RuntimeError("OTLP telemetry is not enabled")
        exporters = {"submission": dict(self._engine.flush_telemetry(timeout_ms))}
        if self._runner is not None:
            exporters["runner"] = dict(self._runner.flush_telemetry(timeout_ms))
        return exporters

    def telemetry_stats(self) -> dict[str, dict[str, int]]:
        """Read queue and OTLP acknowledgement/loss counters without waiting."""
        if self._otlp_endpoint is None:
            raise RuntimeError("OTLP telemetry is not enabled")
        exporters = {"submission": dict(self._engine.telemetry_stats())}
        if self._runner is not None:
            exporters["runner"] = dict(self._runner.telemetry_stats())
        elif self._last_runner_telemetry is not None:
            exporters["runner"] = self._last_runner_telemetry.copy()
        return exporters

    def _build_runner(
        self,
        *,
        num_workers: int = 4,
        idle_sleep_ms: int = 50,
        evaluate_triggers: bool = True,
        num_processes: int | None = None,
        import_path: str | None = None,
        worker_env: dict[str, str] | None = None,
    ) -> Any:
        builder = RustTaskRunnerBuilder(self._app_id)
        builder.with_config(self._config)
        if num_processes is not None:
            if num_processes < 1:
                raise ValueError("num_processes must be at least 1")
            num_workers = num_processes
            builder.with_process_pool(
                self._worker_command(import_path),
                list(self._worker_env(worker_env).items()),
            )

        if self._backend_objects is not None:
            builder.with_backends(
                self._backend_objects["broker"],
                self._backend_objects["orchestrator"],
                self._backend_objects["state_backend"],
                self._backend_objects.get("trigger") if evaluate_triggers else None,
            )
        else:
            builder.memory()

        builder.with_num_workers(num_workers)
        builder.with_idle_sleep(idle_sleep_ms)
        if self._otlp_endpoint is not None and self._otlp_bearer_token is not None:
            builder.enable_otlp(self._otlp_endpoint, self._otlp_bearer_token)

        for key, handle in self._tasks.items():
            extra = self._task_configs.get(key, {})
            builder.register_task(
                handle._module,
                handle._name,
                handle._make_rust_wrapper(),
                concurrency_control=extra.get("concurrency", "unlimited"),
                key_arguments=extra.get("key_arguments", []),
                reroute_on_cc=extra.get("reroute_on_cc", False),
                running_concurrency=extra.get("running_concurrency"),
                max_retries=extra.get("max_retries", 0),
                retry_for_errors=extra.get("retry_for_errors", []),
                registration_concurrency="unlimited",
                cache_results=extra.get("cache_results", False),
                disable_cache_args=[],
                on_diff_non_key_args_raise=False,
                parallel_batch_size=extra.get("parallel_batch_size", 100),
                is_workflow_task=extra.get("is_workflow_task", False),
                queue=extra.get("queue", "default"),
                priority=extra.get("priority", 0.0),
                retry_delay=extra.get("retry_delay", 0.0),
                retry_max_delay=extra.get("retry_max_delay", 300.0),
                retry_backoff=extra.get("retry_backoff", 2.0),
                retry_jitter=extra.get("retry_jitter", "equal"),
                timeout=extra.get("timeout"),
                retry_on_timeout=extra.get("retry_on_timeout", True),
            )

        for handle in self._foreign_tasks.values():
            builder.register_foreign_task(
                handle._language,
                handle._module,
                handle._name,
                queue=handle._queue,
                priority=handle._priority,
            )

        return builder.build()

    @property
    def config(self) -> AppConfig:
        """The effective :class:`AppConfig`; queue and runner settings are settable on it."""
        return self._config

    # --- Worker processes ------------------------------------------------

    @property
    def import_path(self) -> str:
        """``module:attribute`` under which worker processes import this app."""
        if self._import_path is None:
            self._import_path = self._discover_import_path()
        return self._import_path

    def _discover_import_path(self) -> str:
        """Find the module attribute bound to this app; explicit ``import_path`` avoids the scan."""
        candidates: list[str] = []
        for module_name, module in list(sys.modules.items()):
            namespace = getattr(module, "__dict__", None)
            if not isinstance(namespace, dict):
                continue
            for attribute, value in list(namespace.items()):
                if value is self and not attribute.startswith("_"):
                    if module_name == "__main__":
                        spec = getattr(module, "__spec__", None)
                        module_name = getattr(spec, "name", None) or module_name
                    candidates.append(f"{module_name}:{attribute}")
        importable = [c for c in candidates if not c.startswith("__main__:")]
        if importable:
            return sorted(importable, key=len)[0]
        raise RuntimeError(
            "cannot determine how worker processes should import this App; pass "
            "App(..., import_path='package.module:app') or run(..., import_path=...)"
        )

    def _worker_command(self, import_path: str | None) -> list[str]:
        return [sys.executable, "-m", "rustvello.worker", "--child", "--app", import_path or self.import_path]

    def _worker_env(self, extra: dict[str, str] | None) -> dict[str, str]:
        env = {
            # the children must resolve the same modules as this interpreter
            "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
            "RUSTVELLO_WORKER_CHILD": "1",
        }
        if extra:
            env.update(extra)
        return env

    # --- Operations ------------------------------------------------------

    def purge(self) -> None:
        """Delete every queued invocation, control record, stored state and trigger."""
        assert self._backend_objects is not None
        for name in ("broker", "orchestrator", "state_backend", "trigger", "client_data_store"):
            component = self._backend_objects.get(name)
            if component is not None and hasattr(component, "purge"):
                component.purge()

    def _failure_message(self, invocation_id: InvocationId) -> str:
        """Stored error of a failed invocation as ``ErrorType: message`` (falls back to the raw record)."""
        assert self._backend_objects is not None
        state_backend = self._backend_objects["state_backend"]
        try:
            raw = state_backend.get_error_json(str(invocation_id))
        except Exception:  # noqa: BLE001 - the error record is informative only
            raw = None
        if raw:
            try:
                error = json.loads(raw)
                if isinstance(error, dict):
                    kind = error.get("error_type") or error.get("type") or error.get("kind")
                    message = error.get("message") or error.get("error") or ""
                    if kind:
                        if not message:
                            return str(kind)
                        # a message that already starts with its type is not prefixed twice
                        return message if message.startswith(f"{kind}:") else f"{kind}: {message}"
            except ValueError:
                pass
            return str(raw)
        try:
            return str(state_backend.get_error(str(invocation_id)))
        except Exception:  # noqa: BLE001
            return str(self._engine.get_result(invocation_id))

    def cancel(self, invocation: "Invocation | InvocationId") -> bool:
        """Cancel an invocation that has not finished; see :meth:`Invocation.cancel`."""
        invocation_id = invocation.id if isinstance(invocation, Invocation) else invocation
        return bool(self._engine.cancel(invocation_id))

    def queue_depth(self, queue: str = "default") -> int:
        """Number of queued Python invocations waiting in one logical queue."""
        assert self._backend_objects is not None
        return int(self._backend_objects["broker"].count_invocations_in_queues([queue]))

    def queue_depths(self) -> dict[str, int]:
        """Queued invocations per declared broker queue."""
        return {queue: self.queue_depth(queue) for queue in self._config.broker_queues}

    def get_task(self, key: str) -> "TaskHandle":
        """Look up a task by ``module.name`` or ``python::module.name``."""
        full = key if "::" in key else f"python::{key}"
        try:
            return self._tasks[full]
        except KeyError:
            raise KeyError(f"unknown task {key!r}; registered: {sorted(self._tasks)}") from None

    def current_invocation(self) -> CurrentInvocation | None:
        """Identity, retry count and arguments of the task attempt running in this thread."""
        invocation_id = get_current_invocation_id()
        if invocation_id is None:
            return None
        num_retries = get_current_num_retries() or 0
        task_key = ""
        arguments: dict[str, Any] = {}
        assert self._backend_objects is not None
        state_backend = self._backend_objects["state_backend"]
        try:
            invocation = json.loads(state_backend.get_invocation(invocation_id))
            task = invocation["task_id"]
            task_key = f"{task.get('language', 'python')}::{task['module']}.{task['name']}"
            call_id = invocation["call_id"]
            call_key = f"{task_key}:{call_id['args_id']}"
            call = json.loads(state_backend.get_call(call_key))
            arguments = {k: json.loads(v) for k, v in call.get("serialized_arguments", {}).items()}
        except Exception:  # noqa: BLE001 - metadata is best effort; the id and retries are authoritative
            pass
        return CurrentInvocation(invocation_id, task_key, num_retries, arguments)

    def wait_results(
        self, invocations: Sequence["Invocation"], timeout: float = 60.0, poll_interval: float = 0.05
    ) -> list[Any]:
        """Block until every invocation is terminal; results in the same order.

        Raises RuntimeError on the first failed invocation and TimeoutError when the
        deadline passes; one poll loop covers all invocations instead of one per child.
        """
        pending = dict(enumerate(invocations))
        results: list[Any] = [None] * len(invocations)
        deadline = time.monotonic() + timeout
        while pending:
            for index, invocation in list(pending.items()):
                if not invocation.status.is_terminal():
                    continue
                results[index] = invocation.result(timeout=0)  # terminal: returns or raises without waiting
                del pending[index]
            if pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"{len(pending)} invocation(s) still pending after {timeout}s")
                time.sleep(poll_interval)
        return results

    @property
    def dev_mode_force_sync(self) -> bool:
        """Whether task calls run inline in the caller; settable at runtime (tests)."""
        return self._dev_mode_force_sync

    @dev_mode_force_sync.setter
    def dev_mode_force_sync(self, enabled: bool) -> None:
        self._dev_mode_force_sync = bool(enabled)
        self._config.dev_mode_force_sync = bool(enabled)
        self._engine.set_dev_mode_force_sync(bool(enabled))

    def start_monitor(self, host: str = "127.0.0.1", port: int = 8000, log_level: str = "info") -> Any:
        """Serve the monitoring dashboard for this app's backends; returns a stoppable server handle."""
        from rustvello.rustvello import start_monitor

        assert self._backend_objects is not None
        return start_monitor(
            self._app_id,
            self._backend_objects["broker"],
            self._backend_objects["orchestrator"],
            self._backend_objects["state_backend"],
            self._backend_objects["client_data_store"],
            trigger=self._backend_objects.get("trigger"),
            task_ids=[(handle._module, handle._name) for handle in self._tasks.values()],
            host=host,
            port=port,
            log_level=log_level,
            config=self._config,
        )

    # --- Triggers --------------------------------------------------------

    def trigger(self, task_handle: "TaskHandle | ForeignTaskHandle") -> "_TriggerBuilder":
        """Create a trigger that fires the given task.

        Returns a :class:`_TriggerBuilder` for fluent configuration::

            app.trigger(cleanup).on_cron("0 */5 * * * *").register()
        """
        return _TriggerBuilder(self, task_handle)

    # --- Introspection ---------------------------------------------------

    @property
    def tasks(self) -> dict[str, "TaskHandle"]:
        """Registered tasks as ``{language::module.name: TaskHandle}``."""
        return dict(self._tasks)

    @property
    def backend(self) -> str:
        """The backend name (``"memory"``, ``"sqlite"``, etc.)."""
        return self._backend_name


# ── Helper types ────────────────────────────────────────────────────────


class _TriggerDef:
    """Internal representation of a registered trigger."""

    __slots__ = ("task_key", "kind", "schedule", "kwargs", "condition_id")

    def __init__(self, task_key: str, kind: str, schedule: str, kwargs: dict[str, Any] | None = None) -> None:
        self.task_key = task_key
        self.kind = kind
        self.schedule = schedule
        self.kwargs = dict(kwargs or {})  # task arguments; any name, including "kind"
        self.condition_id: str | None = None


class _TriggerBuilder:
    """Fluent builder for triggers.

    Usage::

        app.trigger(my_task).on_cron("0 */5 * * * *").with_args(x=1).register()

    ``register()`` stores the cron condition and the trigger definition in the
    app's trigger store, so every runner sharing the backend fires it (once per
    slot, whichever runner evaluates it). Registering the same trigger again is
    a no-op: condition and trigger ids are derived from their content.
    """

    def __init__(self, app: App, task_handle: TaskHandle | ForeignTaskHandle) -> None:
        self._app = app
        self._task_handle = task_handle
        self._kind: str | None = None
        self._schedule: str = ""
        self._cron: str = ""
        self._min_interval_seconds = 0
        self._kwargs: dict[str, Any] = {}

    def on_cron(self, expression: str, *, min_interval_seconds: int | None = None) -> "_TriggerBuilder":
        """Fire the task on a cron schedule.

        Args:
            expression: A cron expression: 5 fields (``min hour day month weekday``)
                or 6 fields with seconds first (``sec min hour day month weekday``).
            min_interval_seconds: Minimum time between two firings. Defaults to 50
                for a 5-field expression (as the Rust ``TriggerBuilder::on_cron``),
                so a minute slot fires once, and to 0 for a 6-field expression.
        """
        if min_interval_seconds is None:
            min_interval_seconds = 50 if len(expression.split()) == 5 else 0
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be >= 0")
        self._kind = "cron"
        self._schedule = expression
        self._cron = expression
        self._min_interval_seconds = min_interval_seconds
        return self

    def on_interval(self, seconds: float) -> "_TriggerBuilder":
        """Fire the task at a fixed interval.

        The interval is enforced on whole seconds, and triggers are evaluated every
        few seconds by the runner, so a firing can come up to one evaluation period
        late; use ``on_cron`` for wall-clock schedules.

        Args:
            seconds: Interval in seconds between trigger firings (at least 1).
        """
        if seconds < 1:
            raise ValueError("on_interval needs at least 1 second")
        self._kind = "interval"
        self._schedule = str(seconds)
        self._cron = "* * * * * *"
        self._min_interval_seconds = math.ceil(seconds)
        return self

    def with_args(self, **kwargs: Any) -> "_TriggerBuilder":
        """Set task arguments for each trigger firing (JSON-serializable values)."""
        self._kwargs = kwargs
        return self

    def register(self) -> _TriggerDef:
        """Store the trigger in the app's trigger store.

        Returns the :class:`_TriggerDef` for introspection.

        Raises:
            ValueError: no schedule was set, the cron expression is invalid, or the
                task is a foreign task (register those from their own runtime).
            RuntimeError: the app's backend has no trigger store.
        """
        if self._kind is None:
            raise ValueError("Must specify a trigger type (on_cron / on_interval) before calling register()")
        handle = self._task_handle
        if handle._language != "python":
            raise ValueError(
                f"cannot register a trigger for {handle._language} task {handle._module}.{handle._name} "
                "from Python; register it in the runtime that implements the task"
            )
        assert self._app._backend_objects is not None
        store = self._app._backend_objects.get("trigger")
        if store is None:
            raise RuntimeError(f"backend {self._app.backend!r} has no trigger store")
        arguments = json.dumps(self._kwargs) if self._kwargs else None
        condition_id = store.register_cron_condition(self._cron, self._min_interval_seconds)
        store.register_trigger_typed(handle._module, handle._name, [condition_id], "All", arguments)
        key = f"{handle._language}::{handle._module}.{handle._name}"
        tdef = _TriggerDef(key, self._kind, self._schedule, self._kwargs)
        tdef.condition_id = condition_id
        self._app._triggers.append(tdef)
        return tdef
