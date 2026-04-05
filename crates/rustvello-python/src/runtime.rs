//! Shared tokio runtime for PyO3 bridge objects.
//!
//! All Python wrapper structs share a single runtime to avoid spawning
//! separate OS thread-pools per object.
//!
//! **Important**: Every Python method that calls async Rust code must use
//! `shared_runtime()?.block_on(...)`. Never call `block_on` from within
//! an already-running tokio context (i.e. from inside another `block_on`
//! or from a spawned task). Doing so will deadlock.

use std::sync::OnceLock;

use pyo3::prelude::*;

/// Stores the runtime or an error message if creation failed.
static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

/// Get or initialise the shared tokio runtime.
///
/// Returns `PyResult` so failures propagate to Python as `PyRuntimeError`
/// instead of panicking inside a library.
pub fn shared_runtime() -> PyResult<&'static tokio::runtime::Runtime> {
    let result = RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().map_err(|e| e.to_string()));
    result.as_ref().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to create shared tokio runtime: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_runtime_returns_ok() {
        pyo3::prepare_freethreaded_python();
        let rt = shared_runtime();
        assert!(rt.is_ok());
    }

    #[test]
    fn shared_runtime_is_same_instance() {
        pyo3::prepare_freethreaded_python();
        let a = shared_runtime().unwrap() as *const _;
        let b = shared_runtime().unwrap() as *const _;
        assert_eq!(a, b);
    }

    #[test]
    fn shared_runtime_can_block_on_future() {
        pyo3::prepare_freethreaded_python();
        let rt = shared_runtime().unwrap();
        let result = rt.block_on(async { 1 + 1 });
        assert_eq!(result, 2);
    }
}
