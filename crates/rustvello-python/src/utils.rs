use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::PyResult;
use rustvello_core::context::{
    clear_thread_invocation_context, current_attempt_signal, get_invocation_context,
    set_thread_invocation_context, InvocationContext,
};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};
use rustvello_proto::invocation::TraceContextCarrier;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Parse `s` as an invocation ID and return an `InvocationId`.
///
/// Validation is intentionally lax — only empty strings are rejected.
/// pynenc tests routinely use short readable IDs like "inv-abc", so format
/// validation must not be enforced here. Persistent backends (SQLite, Postgres)
/// that require UUID format will reject invalid IDs at query time.
pub fn parse_invocation_id(s: &str) -> PyResult<InvocationId> {
    if s.is_empty() {
        return Err(PyValueError::new_err("invocation_id must not be empty"));
    }
    Ok(InvocationId::from_string(s))
}

/// Build a task ID from the textual language used at the Python ABI boundary.
pub fn parse_task_id(language: &str, module: &str, name: &str) -> PyResult<TaskId> {
    let language = language
        .parse::<TaskLanguage>()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    TaskId::try_for_language(language, module, name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

/// Build the optional task identity used by queue-aware routing; module and name come together.
pub fn optional_task_id(
    language: &str,
    module: Option<&str>,
    name: Option<&str>,
) -> PyResult<Option<TaskId>> {
    match (module, name) {
        (Some(module), Some(name)) => parse_task_id(language, module, name).map(Some),
        (None, None) => Ok(None),
        _ => Err(PyValueError::new_err(
            "task_module and task_name must be given together",
        )),
    }
}

/// Install the invocation context in this thread, as a worker process does before it
/// runs task code it received from the subprocess executor.
#[pyfunction]
#[pyo3(signature = (invocation_id, task_module, task_name, num_retries=0, language="python", parent_invocation_id=None, traceparent=None, tracestate=None))]
#[allow(clippy::too_many_arguments)]
pub fn set_current_invocation_context(
    invocation_id: &str,
    task_module: &str,
    task_name: &str,
    num_retries: u32,
    language: &str,
    parent_invocation_id: Option<&str>,
    traceparent: Option<String>,
    tracestate: Option<String>,
) -> PyResult<()> {
    let task_id = parse_task_id(language, task_module, task_name)?;
    set_thread_invocation_context(InvocationContext {
        invocation_id: parse_invocation_id(invocation_id)?,
        task_id,
        workflow: None,
        is_workflow_defining: false,
        state_backend: None,
        parent_invocation_id: parent_invocation_id.map(InvocationId::from_string),
        num_retries,
        trace_context: TraceContextCarrier {
            traceparent,
            tracestate,
        },
    });
    Ok(())
}

/// Remove the invocation context installed with `set_current_invocation_context`.
#[pyfunction]
pub fn clear_current_invocation_context() {
    clear_thread_invocation_context();
}

/// Return the invocation ID from Rust's thread-local context if set.
///
/// The Rust executor calls ``set_thread_invocation_context`` in the
/// ``spawn_blocking`` closure before invoking ``task.execute()``, so this
/// function returns the correct invocation ID when called from a Python
/// ``TaskFn`` callback running in that thread.
#[pyfunction]
pub fn get_current_invocation_id() -> Option<String> {
    get_invocation_context().map(|ctx| ctx.invocation_id.to_string())
}

/// Runner threads currently inside Python (holding or releasing the GIL).
///
/// A timed-out or cancelled attempt is abandoned by the runner, so its thread,
/// and the thread that delivers the cancellation, are outside the runner's
/// shutdown drain. The interpreter must not finalize while one of them is
/// still in Python: CPython aborts with ``PyGILState_Release ... finalizing``.
static RUNNER_PYTHON_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Counts one runner-thread call into Python until dropped, which happens
/// after the call's GIL has been released.
pub(crate) struct RunnerPythonCall;

impl RunnerPythonCall {
    pub(crate) fn enter() -> Self {
        RUNNER_PYTHON_CALLS.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for RunnerPythonCall {
    fn drop(&mut self) {
        RUNNER_PYTHON_CALLS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Wait, without holding the GIL, until no runner thread is inside Python.
///
/// Returns `False` if `timeout_seconds` passed first. `App.stop()` and an exit
/// hook call it so the interpreter never finalizes under a runner thread.
#[pyfunction]
pub fn wait_runner_python_calls(py: Python<'_>, timeout_seconds: f64) -> bool {
    let deadline = Instant::now() + Duration::from_secs_f64(timeout_seconds.max(0.0));
    py.allow_threads(|| loop {
        if RUNNER_PYTHON_CALLS.load(Ordering::SeqCst) == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    })
}

/// Call `callback()` once if the runner abandons the running attempt.
///
/// The runner abandons an attempt when its execution deadline expires or its
/// invocation is cancelled. `callback` runs on a runner thread (at once if the
/// attempt was already abandoned); exceptions it raises are reported as
/// unraisable. Returns `False`, registering nothing, outside a runner attempt
/// (dev mode, worker processes that the runner kills instead).
#[pyfunction]
pub fn on_attempt_abandoned(callback: PyObject) -> bool {
    let Some(signal) = current_attempt_signal() else {
        return false;
    };
    signal.on_abandon(move || {
        let _calling = RunnerPythonCall::enter();
        Python::with_gil(|py| {
            if let Err(error) = callback.call0(py) {
                error.write_unraisable_bound(py, None);
            }
        });
    });
    true
}

/// Return the running task as `module.name` from Rust's thread-local invocation context.
///
/// Log formatters and monitoring hooks use this together with
/// ``get_current_invocation_id`` without holding a reference to the task handle.
#[pyfunction]
pub fn get_current_task_key() -> Option<String> {
    get_invocation_context().map(|ctx| format!("{}.{}", ctx.task_id.module(), ctx.task_id.name()))
}

/// Return the retry count from Rust's thread-local invocation context, if set.
///
/// Avoids async backend calls: the Rust executor pre-computes ``num_retries``
/// from the invocation history and stores it in the thread-local context before
/// calling into Python.  Use this instead of
/// ``orchestrator.get_invocation_retries()`` when called from within a Rust
/// ``spawn_blocking`` task to prevent nested ``block_on`` deadlocks.
#[pyfunction]
pub fn get_current_num_retries() -> Option<u32> {
    get_invocation_context().map(|ctx| ctx.num_retries)
}

/// Return the persisted execution span carrier for the running task attempt.
#[pyfunction]
pub fn get_current_trace_context() -> Option<(Option<String>, Option<String>)> {
    get_invocation_context()
        .map(|ctx| (ctx.trace_context.traceparent, ctx.trace_context.tracestate))
}

/// Return workflow identity fields from Rust's thread-local invocation context.
///
/// Returns a tuple of `(workflow_id, workflow_type, parent_id_or_none)` when
/// an invocation context is set, or `None` otherwise.  The Python proxy uses
/// this to construct a `WorkflowIdentity` without async backend calls.
#[pyfunction]
pub fn get_current_workflow_info() -> Option<(String, String, Option<String>)> {
    get_invocation_context().and_then(|ctx| {
        ctx.workflow.map(|workflow| {
            (
                workflow.workflow_id.to_string(),
                workflow.workflow_type.to_string(),
                workflow
                    .parent_id
                    .as_ref()
                    .map(std::string::ToString::to_string),
            )
        })
    })
}

/// Compute a deterministic argument hash from serialized arguments.
///
/// Takes a dict of `{arg_name: serialized_value}` and returns the SHA-256
/// hash string using Rust's canonical algorithm (JSON-escaped keys/values
/// with `=` and `;` delimiters).
#[pyfunction]
pub fn compute_args_id(serialized_args: BTreeMap<String, String>) -> String {
    if serialized_args.is_empty() {
        return "no_args".to_string();
    }
    let mut args = SerializedArguments::new();
    for (k, v) in serialized_args {
        args.insert(k, v);
    }
    args.compute_args_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn valid_uuid_accepted() {
        Python::with_gil(|_py| {
            let result = parse_invocation_id("550e8400-e29b-41d4-a716-446655440000");
            assert!(result.is_ok());
            assert_eq!(
                result.unwrap().as_str(),
                "550e8400-e29b-41d4-a716-446655440000"
            );
        });
    }

    #[test]
    fn empty_string_rejected() {
        Python::with_gil(|py| {
            let result = parse_invocation_id("");
            let err = result.unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn non_uuid_string_accepted() {
        Python::with_gil(|_py| {
            let result = parse_invocation_id("not-a-uuid");
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_str(), "not-a-uuid");
        });
    }

    #[test]
    fn arbitrary_string_accepted() {
        Python::with_gil(|_py| {
            let result = parse_invocation_id("my-task-id-123");
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_str(), "my-task-id-123");
        });
    }
}
