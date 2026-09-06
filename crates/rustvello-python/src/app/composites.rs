//! PyO3 composite operations for `PyRustvello`.
//!
//! Split from `mod.rs` to keep each file under the 500-line limit.
//! Multiple `#[pymethods]` impl blocks on the same `#[pyclass]` are
//! supported by PyO3.

use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::TaskId;

use super::PyRustvello;
use crate::error::to_py_err;

// ---------------------------------------------------------------------------
// Explicit-context composite operations (hot-path)
// ---------------------------------------------------------------------------

#[pymethods]
impl PyRustvello {
    /// Atomic status transition with all side-effects (history, waiters, auto-purge, triggers).
    ///
    /// Returns (status_name, runner_id, timestamp) tuple.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (invocation_id, status, runner_id, task_module, task_name, arguments=None))]
    fn set_invocation_status_with_context(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        status: &str,
        runner_id: &str,
        task_module: &str,
        task_name: &str,
        arguments: Option<BTreeMap<String, String>>,
    ) -> PyResult<(String, Option<String>, f64)> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let task_id = TaskId::try_for_language(
            rustvello_proto::identifiers::TaskLanguage::Python,
            task_module,
            task_name,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let inv_status = crate::orchestrator::parse_status(status)?;
        let args = arguments.unwrap_or_default();

        let app = Arc::clone(&self.inner);
        let record = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_status_with_context(
                        &inv_id, inv_status, &runner, &task_id, args,
                    )
                    .await
                })
                .map_err(to_py_err)
        })?;

        Ok((
            record.status.to_string(),
            record.runner_id.map(|r| r.as_str().to_string()),
            record.timestamp.timestamp() as f64
                + record.timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
        ))
    }

    /// Register invocations with all side-effects (upsert, register, history, triggers, broker).
    #[pyo3(signature = (invocations, runner_id))]
    fn register_invocations(
        &self,
        py: Python<'_>,
        invocations: Vec<(String, String, String, BTreeMap<String, String>)>,
        runner_id: &str,
    ) -> PyResult<()> {
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let mut inv_pairs = Vec::with_capacity(invocations.len());
        for (inv_id_str, task_module, task_name, args) in &invocations {
            let inv_id = rustvello_proto::identifiers::InvocationId::from(inv_id_str.clone());
            let task_id = TaskId::try_for_language(
                rustvello_proto::identifiers::TaskLanguage::Python,
                task_module,
                task_name,
            )
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            let mut ser_args = SerializedArguments::new();
            for (k, v) in args {
                ser_args.insert(k, v.clone());
            }
            let call_dto = rustvello_proto::call::CallDTO::new(task_id.clone(), ser_args);
            let inv_dto = rustvello_proto::invocation::InvocationDTO::new(
                inv_id,
                task_id,
                call_dto.call_id.clone(),
            );
            inv_pairs.push((inv_dto, call_dto));
        }

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.register_invocations(&inv_pairs, &runner).await
                })
                .map_err(to_py_err)
        })
    }

    /// Store result and transition to Success with all side-effects.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (invocation_id, result, runner_id, task_module, task_name, arguments=None))]
    fn set_invocation_result_with_context(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        result: &str,
        runner_id: &str,
        task_module: &str,
        task_name: &str,
        arguments: Option<BTreeMap<String, String>>,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let task_id = TaskId::try_for_language(
            rustvello_proto::identifiers::TaskLanguage::Python,
            task_module,
            task_name,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let args = arguments.unwrap_or_default();

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_result_with_context(&inv_id, result, &runner, &task_id, args)
                        .await
                })
                .map_err(to_py_err)
        })
    }

    /// Store exception and transition to Failed with all side-effects.
    #[pyo3(signature = (invocation_id, error_type, error_message, runner_id, task_module, task_name, arguments=None))]
    #[allow(clippy::too_many_arguments)]
    fn set_invocation_exception_with_context(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        error_type: &str,
        error_message: &str,
        runner_id: &str,
        task_module: &str,
        task_name: &str,
        arguments: Option<BTreeMap<String, String>>,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let task_id = TaskId::try_for_language(
            rustvello_proto::identifiers::TaskLanguage::Python,
            task_module,
            task_name,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let args = arguments.unwrap_or_default();

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_exception_with_context(
                        &inv_id,
                        error_type,
                        error_message,
                        &runner,
                        &task_id,
                        args,
                    )
                    .await
                })
                .map_err(to_py_err)
        })
    }

    /// Set retry with all side-effects (status, retry counter, reroute).
    #[pyo3(signature = (invocation_id, runner_id, task_module, task_name, arguments=None))]
    fn set_invocation_retry_with_context(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        runner_id: &str,
        task_module: &str,
        task_name: &str,
        arguments: Option<BTreeMap<String, String>>,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let task_id = TaskId::try_for_language(
            rustvello_proto::identifiers::TaskLanguage::Python,
            task_module,
            task_name,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let args = arguments.unwrap_or_default();

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_retry_with_context(&inv_id, &runner, &task_id, args)
                        .await
                })
                .map_err(to_py_err)
        })
    }

    /// Retrieve invocations ready to run (blocking priority + broker + CC).
    ///
    /// Returns list of invocation ID strings that have been set to PENDING.
    #[pyo3(signature = (max_num_invocations, runner_id))]
    fn get_invocations_to_run(
        &self,
        py: Python<'_>,
        max_num_invocations: usize,
        runner_id: &str,
    ) -> PyResult<Vec<String>> {
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        let inv_ids = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.get_invocations_to_run(max_num_invocations, &runner)
                        .await
                })
                .map_err(to_py_err)
        })?;

        Ok(inv_ids.into_iter().map(|id| id.to_string()).collect())
    }

    // -----------------------------------------------------------------------
    // Phase 6 composites
    // -----------------------------------------------------------------------

    /// Route a call: check registration CC, create or reuse invocation, route.
    ///
    /// Returns `(kind, invocation_id, existing_call_id_or_empty)` where `kind`
    /// is `"new"`, `"reused"`, or `"reused_diff_call"`.
    #[pyo3(signature = (new_invocation_id, task_module, task_name, arguments, cc_args, registration_cc, index_cc, runner_id))]
    #[allow(clippy::too_many_arguments)]
    fn route_call(
        &self,
        py: Python<'_>,
        new_invocation_id: &str,
        task_module: &str,
        task_name: &str,
        arguments: BTreeMap<String, String>,
        cc_args: Option<BTreeMap<String, String>>,
        registration_cc: &str,
        index_cc: bool,
        runner_id: &str,
    ) -> PyResult<(String, String, String)> {
        use rustvello::orchestration::RouteCallResult;

        let inv_id = rustvello_proto::identifiers::InvocationId::from(new_invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let task_id = TaskId::try_for_language(
            rustvello_proto::identifiers::TaskLanguage::Python,
            task_module,
            task_name,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let mut ser_args = SerializedArguments::new();
        for (k, v) in &arguments {
            ser_args.insert(k.clone(), v.clone());
        }
        let call_dto = rustvello_proto::call::CallDTO::new(task_id.clone(), ser_args);

        let cc_sa = cc_args.map(|m| {
            let mut sa = SerializedArguments::new();
            for (k, v) in m {
                sa.insert(k, v);
            }
            sa
        });

        let reg_cc = crate::orchestrator::parse_cc_type(registration_cc)?;

        let app = Arc::clone(&self.inner);
        let result = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.route_call(
                        &inv_id,
                        &call_dto,
                        cc_sa.as_ref(),
                        reg_cc,
                        index_cc,
                        &runner,
                    )
                    .await
                })
                .map_err(to_py_err)
        })?;

        match result {
            RouteCallResult::New(id) => Ok(("new".to_owned(), id.to_string(), String::new())),
            RouteCallResult::Reused(id) => Ok(("reused".to_owned(), id.to_string(), String::new())),
            RouteCallResult::ReusedDifferentCall {
                invocation_id,
                existing_call_id,
            } => Ok((
                "reused_diff_call".to_owned(),
                invocation_id.to_string(),
                existing_call_id.to_string(),
            )),
            _ => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "unexpected RouteCallResult variant",
            )),
        }
    }

    /// Reroute a set of invocations (transition to Rerouted + re-enqueue).
    #[pyo3(signature = (invocation_ids, runner_id))]
    fn reroute_invocations(
        &self,
        py: Python<'_>,
        invocation_ids: Vec<String>,
        runner_id: &str,
    ) -> PyResult<()> {
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let inv_ids: Vec<rustvello_proto::identifiers::InvocationId> = invocation_ids
            .into_iter()
            .map(rustvello_proto::identifiers::InvocationId::from)
            .collect();

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.reroute_invocations(&inv_ids, &runner).await
                })
                .map_err(to_py_err)
        })
    }

    /// Execute one trigger evaluation loop iteration.
    ///
    /// Returns list of invocation ID strings created by triggers.
    #[pyo3(signature = (runner_id))]
    fn trigger_loop_iteration(&self, py: Python<'_>, runner_id: &str) -> PyResult<Vec<String>> {
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        let ids = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.trigger_loop_iteration(&runner).await
                })
                .map_err(to_py_err)
        })?;

        Ok(ids.into_iter().map(|id| id.to_string()).collect())
    }

    /// Execute one atomic service check: coordination + triggers + recording.
    ///
    /// Returns `None` if this runner is not authorized to run now,
    /// or a list of invocation ID strings created by triggers.
    #[pyo3(signature = (runner_id, service_interval_minutes, spread_margin_minutes, runner_timeout_seconds))]
    fn check_atomic_services(
        &self,
        py: Python<'_>,
        runner_id: &str,
        service_interval_minutes: f64,
        spread_margin_minutes: f64,
        runner_timeout_seconds: f64,
    ) -> PyResult<Option<Vec<String>>> {
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        let result = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.check_atomic_services(
                        &runner,
                        service_interval_minutes,
                        spread_margin_minutes,
                        runner_timeout_seconds,
                    )
                    .await
                })
                .map_err(to_py_err)
        })?;

        Ok(result.map(|ids| ids.into_iter().map(|id| id.to_string()).collect()))
    }

    // -----------------------------------------------------------------------
    // Auto-context composite operations (for native orchestrator)
    // -----------------------------------------------------------------------

    /// Atomic status transition with auto-resolved trigger context.
    ///
    /// Returns (status_name, runner_id, timestamp) tuple.
    #[pyo3(signature = (invocation_id, status, runner_id))]
    fn set_invocation_status(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        status: &str,
        runner_id: &str,
    ) -> PyResult<(String, Option<String>, f64)> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());
        let inv_status = crate::orchestrator::parse_status(status)?;

        let app = Arc::clone(&self.inner);
        let record = py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_status(&inv_id, inv_status, &runner)
                        .await
                })
                .map_err(to_py_err)
        })?;

        Ok((
            record.status.to_string(),
            record.runner_id.map(|r| r.as_str().to_string()),
            record.timestamp.timestamp() as f64
                + record.timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
        ))
    }

    /// Store result and transition to Success with auto-resolved trigger context.
    #[pyo3(signature = (invocation_id, result, runner_id))]
    fn set_invocation_result(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        result: &str,
        runner_id: &str,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_result(&inv_id, result, &runner).await
                })
                .map_err(to_py_err)
        })
    }

    /// Store exception and transition to Failed with auto-resolved trigger context.
    #[pyo3(signature = (invocation_id, error_type, error_message, runner_id))]
    fn set_invocation_exception(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        error_type: &str,
        error_message: &str,
        runner_id: &str,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_exception(&inv_id, error_type, error_message, &runner)
                        .await
                })
                .map_err(to_py_err)
        })
    }

    /// Set retry with auto-resolved trigger context.
    #[pyo3(signature = (invocation_id, runner_id))]
    fn set_invocation_retry(
        &self,
        py: Python<'_>,
        invocation_id: &str,
        runner_id: &str,
    ) -> PyResult<()> {
        let inv_id = rustvello_proto::identifiers::InvocationId::from(invocation_id.to_owned());
        let runner = rustvello_proto::identifiers::RunnerId::from(runner_id.to_owned());

        let app = Arc::clone(&self.inner);
        py.allow_threads(|| {
            crate::runtime::shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_invocation_retry(&inv_id, &runner).await
                })
                .map_err(to_py_err)
        })
    }
}
