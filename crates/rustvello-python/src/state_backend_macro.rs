/// Macro that generates all trait-forwarding `#[pymethods]` for any PyO3
/// state-backend wrapper whose struct has `inner: Arc<T>` where
/// `T: StateBackend`.
///
/// Usage:
/// ```ignore
/// impl_py_state_backend!(PyMemStateBackend);
/// impl_py_state_backend!(PySqliteStateBackend);
/// ```
///
/// The `#[new]` constructor is NOT generated — each backend defines its
/// own constructor in a separate `#[pymethods]` block.
macro_rules! impl_py_state_backend {
    ($py_type:ty) => {
        #[pymethods]
        impl $py_type {
            // ── StateBackendCore ────────────────────────────────

            #[pyo3(signature = (
                                        invocation_id, task_module, task_name, serialized_arguments,
                                        parent_invocation_id=None, workflow_json=None
                                    ))]
            #[allow(clippy::too_many_arguments)]
            fn upsert_invocation(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                serialized_arguments: std::collections::BTreeMap<String, String>,
                parent_invocation_id: Option<&str>,
                workflow_json: Option<&str>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::{StateBackendCore, StateBackendQuery};
                use rustvello_proto::call::{CallDTO, SerializedArguments};
                use rustvello_proto::identifiers::TaskId;
                use rustvello_proto::invocation::{InvocationDTO, WorkflowIdentity};

                let task_id = TaskId::try_new(task_module, task_name)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let mut args = SerializedArguments::new();
                for (k, v) in serialized_arguments {
                    args.insert(k, v);
                }
                let call = CallDTO::new(task_id.clone(), args);
                let inv_id = crate::utils::parse_invocation_id(invocation_id)?;
                let mut inv = InvocationDTO::new(inv_id, task_id, call.call_id.clone());

                if let Some(pid) = parent_invocation_id {
                    inv.parent_invocation_id = Some(crate::utils::parse_invocation_id(pid)?);
                }

                let workflow: Option<WorkflowIdentity> = workflow_json
                    .map(|s| {
                        serde_json::from_str(s)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                    })
                    .transpose()?;
                if let Some(ref wf) = workflow {
                    inv.workflow = Some(wf.clone());
                }

                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| {
                    rt.block_on(async {
                        backend.upsert_invocation(&inv, &call).await?;
                        if let Some(ref wf) = workflow {
                            backend.store_workflow_run(wf).await?;
                        }
                        Ok::<(), rustvello_core::error::RustvelloError>(())
                    })
                })
                .map_err(crate::error::to_py_err)
            }

            fn store_result(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                result: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.store_result(&id, result)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_result(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.get_result(&id)))
                    .map_err(crate::error::to_py_err)
            }

            #[pyo3(signature = (invocation_id, error_type, message, traceback=None))]
            fn store_error(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                error_type: &str,
                message: &str,
                traceback: Option<&str>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let error = rustvello_core::error::TaskError {
                    error_type: error_type.to_string(),
                    message: message.to_string(),
                    traceback: traceback.map(std::string::ToString::to_string),
                };
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.store_error(&id, &error)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_error(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let error = py
                    .allow_threads(|| rt.block_on(backend.get_error(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(error.map(|e| e.to_string()))
            }

            fn get_error_json(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let error = py
                    .allow_threads(|| rt.block_on(backend.get_error(&id)))
                    .map_err(crate::error::to_py_err)?;
                match error {
                    Some(e) => {
                        let json = serde_json::to_string(&e).map_err(|err| {
                            pyo3::exceptions::PyValueError::new_err(err.to_string())
                        })?;
                        Ok(Some(json))
                    }
                    None => Ok(None),
                }
            }

            fn purge(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendCore;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.purge()))
                    .map_err(crate::error::to_py_err)
            }

            // ── History ────────────────────────────────────────

            #[pyo3(signature = (
                                        invocation_id, status,
                                        runner_id=None, runner_context_id=None, message=None,
                                        sr_timestamp_us=None, history_timestamp_us=None
                                    ))]
            #[allow(clippy::too_many_arguments)]
            fn add_history(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                status: &str,
                runner_id: Option<&str>,
                runner_context_id: Option<&str>,
                message: Option<&str>,
                sr_timestamp_us: Option<i64>,
                history_timestamp_us: Option<i64>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendCore;
                use rustvello_proto::identifiers::RunnerId;
                use rustvello_proto::invocation::InvocationHistory;
                use rustvello_proto::status::InvocationStatusRecord;

                let inv_id = crate::utils::parse_invocation_id(invocation_id)?;
                let parsed_status = crate::orchestrator::parse_status(status)?;
                let rid = runner_id.map(RunnerId::from_string);
                let ctx_rid = runner_context_id.map(RunnerId::from_string);

                let record = if let Some(us) = sr_timestamp_us {
                    let secs = us / 1_000_000;
                    let nanos = ((us % 1_000_000) * 1000) as u32;
                    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                        .ok_or_else(|| {
                            pyo3::exceptions::PyValueError::new_err("invalid timestamp")
                        })?;
                    InvocationStatusRecord {
                        status: parsed_status,
                        runner_id: rid,
                        timestamp: dt,
                    }
                } else {
                    InvocationStatusRecord::new(parsed_status, rid)
                };

                let history_ts = history_timestamp_us.and_then(|us| {
                    let secs = us / 1_000_000;
                    let nanos = ((us % 1_000_000) * 1000) as u32;
                    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                });

                let history = InvocationHistory {
                    invocation_id: inv_id,
                    status_record: record,
                    message: message.map(str::to_string),
                    runner_id: ctx_rid,
                    registered_by_inv_id: None,
                    history_timestamp: history_ts,
                };
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.add_history(&history)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_history(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let history = py
                    .allow_threads(|| rt.block_on(backend.get_history(&id)))
                    .map_err(crate::error::to_py_err)?;
                serde_json::to_string(&history)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            }

            // ── Invocation retrieval ───────────────────────────

            fn get_invocation(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::state_backend::StateBackendCore;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let inv = py
                    .allow_threads(|| rt.block_on(backend.get_invocation(&id)))
                    .map_err(crate::error::to_py_err)?;
                serde_json::to_string(&inv)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            }

            fn get_call(&self, py: pyo3::Python<'_>, call_id: &str) -> pyo3::PyResult<String> {
                use rustvello_core::state_backend::StateBackendCore;
                let cid = call_id
                    .parse::<rustvello_proto::identifiers::CallId>()
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let call = py
                    .allow_threads(|| rt.block_on(backend.get_call(&cid)))
                    .map_err(crate::error::to_py_err)?;
                serde_json::to_string(&call)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            }

            // ── StateBackendQuery ──────────────────────────────

            fn get_child_invocations(
                &self,
                py: pyo3::Python<'_>,
                parent_invocation_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let id = crate::utils::parse_invocation_id(parent_invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let children = py
                    .allow_threads(|| rt.block_on(backend.get_child_invocations(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(children.iter().map(|c| c.to_string()).collect())
            }

            fn store_workflow_run(
                &self,
                py: pyo3::Python<'_>,
                workflow_json: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendQuery;
                use rustvello_proto::invocation::WorkflowIdentity;
                let wf: WorkflowIdentity = serde_json::from_str(workflow_json)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.store_workflow_run(&wf)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_all_workflow_types(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let types = py
                    .allow_threads(|| rt.block_on(backend.get_all_workflow_types()))
                    .map_err(crate::error::to_py_err)?;
                Ok(types.iter().map(|t| t.to_string()).collect())
            }

            fn get_workflow_runs(
                &self,
                py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                use rustvello_proto::identifiers::TaskId;
                let task_id = TaskId::try_new(task_module, task_name)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let runs = py
                    .allow_threads(|| rt.block_on(backend.get_workflow_runs(&task_id)))
                    .map_err(crate::error::to_py_err)?;
                runs.iter()
                    .map(|r| {
                        serde_json::to_string(r)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                    })
                    .collect()
            }

            fn set_workflow_data(
                &self,
                py: pyo3::Python<'_>,
                workflow_invocation_id: &str,
                key: &str,
                value: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendQuery;
                let id = crate::utils::parse_invocation_id(workflow_invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.set_workflow_data(&id, key, value)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_workflow_data(
                &self,
                py: pyo3::Python<'_>,
                workflow_invocation_id: &str,
                key: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let id = crate::utils::parse_invocation_id(workflow_invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.get_workflow_data(&id, key)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_workflow_invocations(
                &self,
                py: pyo3::Python<'_>,
                workflow_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let id = crate::utils::parse_invocation_id(workflow_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let invocations = py
                    .allow_threads(|| rt.block_on(backend.get_workflow_invocations(&id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(invocations.iter().map(|i| i.to_string()).collect())
            }

            fn store_app_info(
                &self,
                py: pyo3::Python<'_>,
                app_id: &str,
                info_json: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendQuery;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.store_app_info(app_id, info_json)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_app_info(
                &self,
                py: pyo3::Python<'_>,
                app_id: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.get_app_info(app_id)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_all_app_infos(
                &self,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<Vec<(String, String)>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.get_all_app_infos()))
                    .map_err(crate::error::to_py_err)
            }

            fn store_workflow_sub_invocation(
                &self,
                py: pyo3::Python<'_>,
                workflow_id: &str,
                sub_invocation_id: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendQuery;
                let wf_id = crate::utils::parse_invocation_id(workflow_id)?;
                let sub_id = crate::utils::parse_invocation_id(sub_invocation_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| {
                    rt.block_on(backend.store_workflow_sub_invocation(&wf_id, &sub_id))
                })
                .map_err(crate::error::to_py_err)
            }

            fn get_workflow_sub_invocations(
                &self,
                py: pyo3::Python<'_>,
                workflow_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let wf_id = crate::utils::parse_invocation_id(workflow_id)?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py
                    .allow_threads(|| rt.block_on(backend.get_workflow_sub_invocations(&wf_id)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.iter().map(|id| id.to_string()).collect())
            }

            fn get_all_workflow_runs(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendQuery;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let runs = py
                    .allow_threads(|| rt.block_on(backend.get_all_workflow_runs()))
                    .map_err(crate::error::to_py_err)?;
                runs.iter()
                    .map(|r| {
                        serde_json::to_string(r)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                    })
                    .collect()
            }

            // ── StateBackendRunner ─────────────────────────────

            #[pyo3(signature = (
                                        runner_id, runner_cls, pid, hostname, thread_id,
                                        parent_runner_id=None, parent_runner_cls=None
                                    ))]
            #[allow(clippy::too_many_arguments)]
            fn store_runner_context(
                &self,
                py: pyo3::Python<'_>,
                runner_id: &str,
                runner_cls: &str,
                pid: u32,
                hostname: &str,
                thread_id: u64,
                parent_runner_id: Option<&str>,
                parent_runner_cls: Option<&str>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::state_backend::StateBackendRunner;
                let ctx = rustvello_core::state_backend::StoredRunnerContext {
                    runner_cls: runner_cls.to_string(),
                    runner_id: runner_id.to_string(),
                    pid,
                    hostname: hostname.to_string(),
                    thread_id,
                    started_at: chrono::Utc::now(),
                    parent_runner_id: parent_runner_id.map(str::to_string),
                    parent_runner_cls: parent_runner_cls.map(str::to_string),
                };
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.store_runner_context(&ctx)))
                    .map_err(crate::error::to_py_err)
            }

            fn get_runner_context(
                &self,
                py: pyo3::Python<'_>,
                runner_id: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::state_backend::StateBackendRunner;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ctx = py
                    .allow_threads(|| rt.block_on(backend.get_runner_context(runner_id)))
                    .map_err(crate::error::to_py_err)?;
                match ctx {
                    Some(c) => {
                        let json = serde_json::to_string(&c).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                        Ok(Some(json))
                    }
                    None => Ok(None),
                }
            }

            fn get_runner_contexts_by_parent(
                &self,
                py: pyo3::Python<'_>,
                parent_runner_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendRunner;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let contexts = py
                    .allow_threads(|| {
                        rt.block_on(backend.get_runner_contexts_by_parent(parent_runner_id))
                    })
                    .map_err(crate::error::to_py_err)?;
                contexts
                    .iter()
                    .map(|c| {
                        serde_json::to_string(c)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                    })
                    .collect()
            }

            #[pyo3(signature = (runner_id, limit=0, offset=0))]
            fn get_invocation_ids_by_runner(
                &self,
                py: pyo3::Python<'_>,
                runner_id: &str,
                limit: usize,
                offset: usize,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendRunner;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let ids = py
                    .allow_threads(|| {
                        rt.block_on(backend.get_invocation_ids_by_runner(runner_id, limit, offset))
                    })
                    .map_err(crate::error::to_py_err)?;
                Ok(ids.iter().map(|id| id.to_string()).collect())
            }

            fn count_invocations_by_runner(
                &self,
                py: pyo3::Python<'_>,
                runner_id: &str,
            ) -> pyo3::PyResult<usize> {
                use rustvello_core::state_backend::StateBackendRunner;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(backend.count_invocations_by_runner(runner_id)))
                    .map_err(crate::error::to_py_err)
            }

            #[pyo3(signature = (start_ts, end_ts, limit=100, offset=0))]
            fn get_history_in_timerange(
                &self,
                py: pyo3::Python<'_>,
                start_ts: f64,
                end_ts: f64,
                limit: usize,
                offset: usize,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::state_backend::StateBackendRunner;
                let start = chrono::DateTime::<chrono::Utc>::from_timestamp(
                    start_ts as i64,
                    ((start_ts % 1.0) * 1e9) as u32,
                )
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("invalid start timestamp")
                })?;
                let end = chrono::DateTime::<chrono::Utc>::from_timestamp(
                    end_ts as i64,
                    ((end_ts % 1.0) * 1e9) as u32,
                )
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid end timestamp"))?;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let history = py
                    .allow_threads(|| {
                        rt.block_on(backend.get_history_in_timerange(start, end, limit, offset))
                    })
                    .map_err(crate::error::to_py_err)?;
                serde_json::to_string(&history)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            }

            fn get_matching_runner_contexts(
                &self,
                py: pyo3::Python<'_>,
                partial_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::state_backend::StateBackendRunner;
                let backend = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let contexts = py
                    .allow_threads(|| rt.block_on(backend.get_matching_runner_contexts(partial_id)))
                    .map_err(crate::error::to_py_err)?;
                contexts
                    .iter()
                    .map(|c| {
                        serde_json::to_string(c)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                    })
                    .collect()
            }
        }
    };
}
