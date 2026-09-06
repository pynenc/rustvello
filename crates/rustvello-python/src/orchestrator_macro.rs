/// Generates all trait-forwarding `#[pymethods]` for a PyO3 orchestrator wrapper.
///
/// The target type must have an `inner` field that dereferences to a type
/// implementing `OrchestratorStatus + OrchestratorConcurrency + OrchestratorBlocking + OrchestratorQuery + OrchestratorRecovery`.
///
/// Each backend keeps its own `#[new]` constructor in a separate
/// `#[pymethods]` block. This macro generates the remaining 30 methods.
macro_rules! impl_py_orchestrator {
    ($py_type:ty) => {
        #[pyo3::pymethods]
        impl $py_type {
            // ─── OrchestratorStatus: Status ───────────────────────────

            fn get_invocation_status(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<(String, Option<String>, f64)> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let record = py.allow_threads(|| rt.block_on(orch.get_invocation_status(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok((
                    record.status.to_string(),
                    record.runner_id.map(|r| r.as_str().to_string()),
                    record.timestamp.timestamp() as f64
                        + record.timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
                ))
            }

            #[pyo3(signature = (invocation_id, status, runner_id=None))]
            fn set_invocation_status(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                status: &str,
                runner_id: Option<&str>,
            ) -> pyo3::PyResult<(String, Option<String>, f64)> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let new_status = crate::orchestrator::parse_status(status)?;
                let rid = runner_id
                    .map(rustvello_proto::identifiers::RunnerId::from_string);
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let record = py.allow_threads(|| rt.block_on(orch.set_invocation_status(&id, new_status, rid.as_ref())))
                    .map_err(crate::error::to_py_err)?;
                Ok((
                    record.status.to_string(),
                    record.runner_id.map(|r| r.as_str().to_string()),
                    record.timestamp.timestamp() as f64
                        + record.timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
                ))
            }

            // ─── OrchestratorBlocking: Waiting ──────────────────────────

            fn set_waiting_for(
                &self, py: pyo3::Python<'_>,
                waiter: &str,
                waited_on: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorBlocking;
                let waiter_id = crate::utils::parse_invocation_id(waiter)?;
                let waited_on_id = crate::utils::parse_invocation_id(waited_on)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.set_waiting_for(&waiter_id, &waited_on_id)))
                    .map_err(crate::error::to_py_err)
            }

            fn release_waiters(
                &self, py: pyo3::Python<'_>,
                completed: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorBlocking;
                let id = crate::utils::parse_invocation_id(completed)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.release_waiters(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn get_waiters(
                &self, py: pyo3::Python<'_>,
                waited_on: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorBlocking;
                let id = crate::utils::parse_invocation_id(waited_on)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_waiters(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            // ─── OrchestratorStatus: Registration ─────────────────────

            fn register_invocation(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                serialized_arguments: std::collections::BTreeMap<String, String>,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let mut args = rustvello_proto::call::SerializedArguments::new();
                for (k, v) in serialized_arguments {
                    args.insert(k, v);
                }
                let call = rustvello_proto::call::CallDTO::new(task_id, args);
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let inv_id = py.allow_threads(|| rt.block_on(orch.register_invocation(&call)))
                    .map_err(crate::error::to_py_err)?;
                Ok(inv_id.as_str().to_string())
            }

            #[pyo3(signature = (invocation_id, task_module, task_name, serialized_arguments, runner_id=None))]
            fn register_invocation_with_id(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                serialized_arguments: std::collections::BTreeMap<String, String>,
                runner_id: Option<&str>,
            ) -> pyo3::PyResult<(String, Option<String>, f64)> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let inv_id = crate::utils::parse_invocation_id(invocation_id)?;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let mut args = rustvello_proto::call::SerializedArguments::new();
                for (k, v) in serialized_arguments {
                    args.insert(k, v);
                }
                let call = rustvello_proto::call::CallDTO::new(task_id, args);
                let rid = runner_id
                    .map(rustvello_proto::identifiers::RunnerId::from_string);
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let record = py.allow_threads(|| rt.block_on(orch.register_invocation_with_id(
                        &inv_id, &call, rid.as_ref(),
                    )))
                    .map_err(crate::error::to_py_err)?;
                Ok((
                    record.status.to_string(),
                    record.runner_id.map(|r| r.as_str().to_string()),
                    record.timestamp.timestamp() as f64
                        + record.timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
                ))
            }

            // ─── OrchestratorStatus: Retries ──────────────────────────

            fn increment_invocation_retries(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<u32> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.increment_invocation_retries(&id)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_invocation_retries(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<u32> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.get_invocation_retries(&id)))
                    .map_err(crate::error::to_py_err)
            }

            // ─── OrchestratorStatus: Removal & purge ──────────────────

            fn remove_invocation(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.remove_invocation(&id)))
                    .map_err(crate::error::to_py_err)
            }

            fn purge(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.purge()))
                    .map_err(crate::error::to_py_err)
            }

            // ─── OrchestratorConcurrency: Concurrency control ──────────────

            #[pyo3(signature = (task_module, task_name, task_config_json, cc_args=None))]
            fn check_running_concurrency(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                task_config_json: &str,
                cc_args: Option<std::collections::BTreeMap<String, String>>,
            ) -> pyo3::PyResult<bool> {
                use rustvello_core::orchestrator::OrchestratorConcurrency;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let config: rustvello_proto::config::TaskConfig =
                    serde_json::from_str(task_config_json).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?;
                let args = cc_args.map(|m| {
                    let mut sa = rustvello_proto::call::SerializedArguments::new();
                    for (k, v) in m {
                        sa.insert(k, v);
                    }
                    sa
                });
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.check_running_concurrency(
                        &task_id, &config, args.as_ref(),
                    )))
                    .map_err(crate::error::to_py_err)
            }

            #[pyo3(signature = (invocation_id, task_module, task_name, cc_args=None))]
            fn index_for_concurrency_control(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                cc_args: Option<std::collections::BTreeMap<String, String>>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorConcurrency;
                let inv_id = crate::utils::parse_invocation_id(invocation_id)?;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let args = cc_args.map(|m| {
                    let mut sa = rustvello_proto::call::SerializedArguments::new();
                    for (k, v) in m {
                        sa.insert(k, v);
                    }
                    sa
                });
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.index_for_concurrency_control(
                        &inv_id, &task_id, args.as_ref(),
                    )))
                    .map_err(crate::error::to_py_err)
            }

            fn remove_from_concurrency_index(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorConcurrency;
                let inv_id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.remove_from_concurrency_index(&inv_id)))
                    .map_err(crate::error::to_py_err)
            }

            // ─── OrchestratorStatus: Auto-purge ───────────────────────

            fn schedule_auto_purge(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.schedule_auto_purge(&id)))
                    .map_err(crate::error::to_py_err)
            }

            fn run_auto_purge(
                &self, py: pyo3::Python<'_>,
                max_age_secs: u64,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorStatus;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.run_auto_purge(max_age_secs)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            // ─── OrchestratorQuery ──────────────────────────────────

            #[pyo3(signature = (status, task_module=None, task_name=None))]
            fn get_invocations_by_status(
                &self, py: pyo3::Python<'_>,
                status: &str,
                task_module: Option<&str>,
                task_name: Option<&str>,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let s = crate::orchestrator::parse_status(status)?;
                let task_id = match (task_module, task_name) {
                    (Some(m), Some(n)) => Some(
                        rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, m, n).map_err(
                            |e| pyo3::exceptions::PyValueError::new_err(e.to_string()),
                        )?,
                    ),
                    (None, None) => None,
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Both task_module and task_name must be provided together, or neither",
                        ));
                    }
                };
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_invocations_by_status(s, task_id.as_ref())))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn get_invocations_by_task(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_invocations_by_task(&task_id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn get_invocations_by_call(
                &self, py: pyo3::Python<'_>,
                call_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let cid = call_id
                    .parse::<rustvello_proto::identifiers::CallId>()
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_invocations_by_call(&cid)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            #[pyo3(signature = (task_module=None, task_name=None, statuses=None))]
            fn count_invocations(
                &self, py: pyo3::Python<'_>,
                task_module: Option<&str>,
                task_name: Option<&str>,
                statuses: Option<Vec<String>>,
            ) -> pyo3::PyResult<usize> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let task_id = match (task_module, task_name) {
                    (Some(m), Some(n)) => Some(
                        rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, m, n).map_err(
                            |e| pyo3::exceptions::PyValueError::new_err(e.to_string()),
                        )?,
                    ),
                    (None, None) => None,
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Both task_module and task_name must be provided together, or neither",
                        ));
                    }
                };
                let parsed_statuses: Option<Vec<rustvello_proto::status::InvocationStatus>> =
                    statuses
                        .map(|ss| {
                            ss.iter()
                                .map(|s| crate::orchestrator::parse_status(s))
                                .collect::<pyo3::PyResult<_>>()
                        })
                        .transpose()?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.count_invocations(
                        task_id.as_ref(),
                        parsed_statuses.as_deref(),
                    )))
                    .map_err(crate::error::to_py_err)
            }

            #[pyo3(signature = (task_module=None, task_name=None, statuses=None, limit=100, offset=0))]
            fn get_invocation_ids_paginated(
                &self, py: pyo3::Python<'_>,
                task_module: Option<&str>,
                task_name: Option<&str>,
                statuses: Option<Vec<String>>,
                limit: usize,
                offset: usize,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let task_id = match (task_module, task_name) {
                    (Some(m), Some(n)) => Some(
                        rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, m, n).map_err(
                            |e| pyo3::exceptions::PyValueError::new_err(e.to_string()),
                        )?,
                    ),
                    (None, None) => None,
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Both task_module and task_name must be provided together, or neither",
                        ));
                    }
                };
                let parsed_statuses: Option<Vec<rustvello_proto::status::InvocationStatus>> =
                    statuses
                        .map(|ss| {
                            ss.iter()
                                .map(|s| crate::orchestrator::parse_status(s))
                                .collect::<pyo3::PyResult<_>>()
                        })
                        .transpose()?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_invocation_ids_paginated(
                        task_id.as_ref(),
                        parsed_statuses.as_deref(),
                        limit,
                        offset,
                    )))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn filter_by_status(
                &self, py: pyo3::Python<'_>,
                invocation_ids: Vec<String>,
                statuses: Vec<String>,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let ids: Vec<_> = invocation_ids
                    .iter()
                    .map(|s| crate::utils::parse_invocation_id(s))
                    .collect::<pyo3::PyResult<_>>()?;
                let parsed_statuses: Vec<rustvello_proto::status::InvocationStatus> =
                    statuses
                        .iter()
                        .map(|s| crate::orchestrator::parse_status(s))
                        .collect::<pyo3::PyResult<_>>()?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let filtered = py.allow_threads(|| rt.block_on(orch.filter_by_status(&ids, &parsed_statuses)))
                    .map_err(crate::error::to_py_err)?;
                Ok(filtered.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            #[pyo3(signature = (max_num=100))]
            fn get_blocking_invocations(
                &self, py: pyo3::Python<'_>,
                max_num: usize,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_blocking_invocations(max_num)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            #[pyo3(signature = (task_module, task_name, statuses, cc_args=None))]
            fn get_existing_invocations(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                statuses: Vec<String>,
                cc_args: Option<std::collections::BTreeMap<String, String>>,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorQuery;
                let task_id = rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module, task_name,
                )
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let parsed_statuses: Vec<rustvello_proto::status::InvocationStatus> =
                    statuses
                        .iter()
                        .map(|s| crate::orchestrator::parse_status(s))
                        .collect::<pyo3::PyResult<_>>()?;
                let args = cc_args.map(|m| {
                    let mut sa = rustvello_proto::call::SerializedArguments::new();
                    for (k, v) in m {
                        sa.insert(k, v);
                    }
                    sa
                });
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_existing_invocations(
                        &task_id,
                        args.as_ref(),
                        &parsed_statuses,
                    )))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            // ─── OrchestratorRecovery ───────────────────────────────

            #[pyo3(signature = (runner_id, can_run_atomic_service=false))]
            fn register_heartbeat(
                &self, py: pyo3::Python<'_>,
                runner_id: &str,
                can_run_atomic_service: bool,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let rid = rustvello_proto::identifiers::RunnerId::from_string(runner_id);
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.register_heartbeat(&rid, can_run_atomic_service)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_stale_pending_invocations(
                &self, py: pyo3::Python<'_>,
                max_pending_seconds: u64,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_stale_pending_invocations(max_pending_seconds)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn get_stale_running_invocations(
                &self, py: pyo3::Python<'_>,
                runner_dead_after_seconds: u64,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_stale_running_invocations(runner_dead_after_seconds)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            fn get_active_runner_ids(
                &self, py: pyo3::Python<'_>,
                timeout_seconds: u64,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py.allow_threads(|| rt.block_on(orch.get_active_runner_ids(timeout_seconds)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
            }

            #[pyo3(signature = (timeout_seconds, can_run_atomic_service=None))]
            fn get_active_runners<'py>(
                &self,
                py: pyo3::Python<'py>,
                timeout_seconds: u64,
                can_run_atomic_service: Option<bool>,
            ) -> pyo3::PyResult<Vec<pyo3::Bound<'py, pyo3::types::PyDict>>> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let runners = py.allow_threads(|| rt.block_on(orch.get_active_runners(
                        timeout_seconds,
                        can_run_atomic_service,
                    )))
                    .map_err(crate::error::to_py_err)?;
                runners
                    .into_iter()
                    .map(|r| {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("runner_id", &*r.runner_id.as_str())?;
                        dict.set_item("creation_time", r.creation_time.to_rfc3339())?;
                        dict.set_item("last_heartbeat", r.last_heartbeat.to_rfc3339())?;
                        dict.set_item(
                            "can_run_atomic_service",
                            r.can_run_atomic_service,
                        )?;
                        dict.set_item(
                            "last_service_start",
                            r.last_service_start.map(|dt| dt.to_rfc3339()),
                        )?;
                        dict.set_item(
                            "last_service_end",
                            r.last_service_end.map(|dt| dt.to_rfc3339()),
                        )?;
                        Ok(dict)
                    })
                    .collect()
            }

            fn record_atomic_service_execution(
                &self, py: pyo3::Python<'_>,
                runner_id: &str,
                start_ts: f64,
                end_ts: f64,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                use chrono::{DateTime, Utc};
                let rid = rustvello_proto::identifiers::RunnerId::from_string(runner_id);
                let start_secs = start_ts as i64;
                let start_nanos =
                    ((start_ts - start_secs as f64) * 1_000_000_000.0) as u32;
                let start = DateTime::<Utc>::from_timestamp(start_secs, start_nanos)
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err("invalid start timestamp")
                    })?;
                let end_secs = end_ts as i64;
                let end_nanos =
                    ((end_ts - end_secs as f64) * 1_000_000_000.0) as u32;
                let end = DateTime::<Utc>::from_timestamp(end_secs, end_nanos)
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err("invalid end timestamp")
                    })?;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(orch.record_atomic_service_execution(&rid, start, end)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_atomic_service_timeline<'py>(
                &self,
                py: pyo3::Python<'py>,
            ) -> pyo3::PyResult<Vec<pyo3::Bound<'py, pyo3::types::PyDict>>> {
                use rustvello_core::orchestrator::OrchestratorRecovery;
                let orch = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let timeline = py.allow_threads(|| rt.block_on(orch.get_atomic_service_timeline()))
                    .map_err(crate::error::to_py_err)?;
                timeline
                    .into_iter()
                    .map(|exec| {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("runner_id", exec.runner_id)?;
                        dict.set_item("start_time", exec.start.timestamp_millis() as f64 / 1000.0)?;
                        dict.set_item("end_time", exec.end.timestamp_millis() as f64 / 1000.0)?;
                        Ok(dict)
                    })
                    .collect()
            }
        }
    };
}
