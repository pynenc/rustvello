//! Macro to generate identical `#[pymethods]` for every PyO3 trigger-store
//! wrapper. Every backend (mem, sqlite, postgres, redis, mongo) must expose
//! the same Python-visible API.
//!
//! **Requirements on the target struct**:
//! - A field `store: Arc<T>` where `T: TriggerStore`
//! - A field `manager: TriggerManager`

macro_rules! impl_py_trigger_store {
    ($py_type:ty) => {
        #[pyo3::pymethods]
        impl $py_type {
            // ─── TriggerStore methods ────────────────────────────────

            /// Register a condition from JSON. Returns the condition_id string.
            fn register_condition(&self, py: pyo3::Python<'_>, condition_json: &str) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::TriggerCondition;
                let condition: TriggerCondition = serde_json::from_str(condition_json)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Get a condition by ID. Returns JSON string or None.
            fn get_condition(&self, py: pyo3::Python<'_>, condition_id: &str) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::trigger::TriggerStore;
                let cid =
                    rustvello_proto::trigger::ConditionId::from(condition_id.to_string());
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cond = py.allow_threads(|| rt.block_on(store.get_condition(&cid)))
                    .map_err(crate::error::to_py_err)?;
                match cond {
                    Some(c) => {
                        let json = serde_json::to_string(&c).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                        Ok(Some(json))
                    }
                    None => Ok(None),
                }
            }

            /// Register a trigger definition from JSON.
            fn register_trigger(&self, py: pyo3::Python<'_>, trigger_json: &str) -> pyo3::PyResult<()> {
                use rustvello_core::trigger::TriggerStore;
                let trigger: rustvello_proto::trigger::TriggerDefinitionDTO =
                    serde_json::from_str(trigger_json)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.register_trigger(&trigger)))
                    .map_err(crate::error::to_py_err)
            }

            /// Get a trigger definition by ID. Returns JSON string or None.
            fn get_trigger(&self, py: pyo3::Python<'_>, trigger_id: &str) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::trigger::TriggerStore;
                let tid = rustvello_proto::trigger::TriggerDefinitionId::from(
                    trigger_id.to_string(),
                );
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let trigger = py.allow_threads(|| rt.block_on(store.get_trigger(&tid)))
                    .map_err(crate::error::to_py_err)?;
                match trigger {
                    Some(t) => {
                        let json = serde_json::to_string(&t).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                        Ok(Some(json))
                    }
                    None => Ok(None),
                }
            }

            /// Get triggers for a condition. Returns list of JSON strings.
            fn get_triggers_for_condition(
                &self, py: pyo3::Python<'_>,
                condition_id: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::trigger::TriggerStore;
                let cid =
                    rustvello_proto::trigger::ConditionId::from(condition_id.to_string());
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let triggers = py.allow_threads(|| rt.block_on(store.get_triggers_for_condition(&cid)))
                    .map_err(crate::error::to_py_err)?;
                triggers
                    .iter()
                    .map(|t| {
                        serde_json::to_string(t).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })
                    })
                    .collect()
            }

            /// Get conditions for a task. Returns list of (condition_id, condition_json).
            fn get_conditions_for_task(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<Vec<(String, String)>> {
                use rustvello_core::trigger::TriggerStore;
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let conditions = py.allow_threads(|| rt.block_on(store.get_conditions_for_task(&task_id)))
                    .map_err(crate::error::to_py_err)?;
                conditions
                    .iter()
                    .map(|(cid, cond)| {
                        let json = serde_json::to_string(cond).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                        Ok((cid.to_string(), json))
                    })
                    .collect()
            }

            /// Get all registered conditions. Returns list of (condition_id, condition_json).
            fn get_all_conditions(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<(String, String)>> {
                use rustvello_core::trigger::TriggerStore;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let conditions = py.allow_threads(|| rt.block_on(store.get_all_conditions()))
                    .map_err(crate::error::to_py_err)?;
                conditions
                    .iter()
                    .map(|(cid, cond)| {
                        let json = serde_json::to_string(cond).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                        Ok((cid.to_string(), json))
                    })
                    .collect()
            }

            /// Remove all triggers targeting a task. Returns count removed.
            fn remove_triggers_for_task(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<u32> {
                use rustvello_core::trigger::TriggerStore;
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.remove_triggers_for_task(&task_id)))
                    .map_err(crate::error::to_py_err)
            }

            /// Purge all trigger data.
            fn purge(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
                use rustvello_core::trigger::TriggerStore;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.purge()))
                    .map_err(crate::error::to_py_err)
            }

            /// Record a valid condition. Takes JSON of a ValidCondition.
            fn record_valid_condition(
                &self, py: pyo3::Python<'_>,
                valid_condition_json: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::trigger::TriggerStore;
                let vc: rustvello_proto::trigger::ValidCondition =
                    serde_json::from_str(valid_condition_json).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.record_valid_condition(&vc)))
                    .map_err(crate::error::to_py_err)
            }

            /// Get all valid conditions. Returns list of JSON strings.
            fn get_valid_conditions(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<String>> {
                use rustvello_core::trigger::TriggerStore;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let vcs = py.allow_threads(|| rt.block_on(store.get_valid_conditions()))
                    .map_err(crate::error::to_py_err)?;
                vcs.iter()
                    .map(|vc| {
                        serde_json::to_string(vc).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })
                    })
                    .collect()
            }

            /// Clear valid conditions by ID list.
            fn clear_valid_conditions(
                &self, py: pyo3::Python<'_>,
                valid_condition_ids: Vec<String>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::trigger::TriggerStore;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.clear_valid_conditions(&valid_condition_ids)))
                    .map_err(crate::error::to_py_err)
            }

            /// Get the last cron execution time. Returns Unix timestamp or None.
            fn get_last_cron_execution(
                &self, py: pyo3::Python<'_>,
                condition_id: &str,
            ) -> pyo3::PyResult<Option<f64>> {
                use rustvello_core::trigger::TriggerStore;
                let cid =
                    rustvello_proto::trigger::ConditionId::from(condition_id.to_string());
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let ts = py.allow_threads(|| rt.block_on(store.get_last_cron_execution(&cid)))
                    .map_err(crate::error::to_py_err)?;
                Ok(ts.map(|dt| {
                    dt.timestamp() as f64
                        + dt.timestamp_subsec_nanos() as f64 / 1e9
                }))
            }

            /// Store a cron execution time with optimistic locking.
            #[pyo3(signature = (condition_id, execution_timestamp, expected_last_timestamp=None))]
            fn store_cron_execution(
                &self, py: pyo3::Python<'_>,
                condition_id: &str,
                execution_timestamp: f64,
                expected_last_timestamp: Option<f64>,
            ) -> pyo3::PyResult<bool> {
                use chrono::{DateTime, Utc};
                use rustvello_core::trigger::TriggerStore;
                let cid =
                    rustvello_proto::trigger::ConditionId::from(condition_id.to_string());
                let exec_time = DateTime::<Utc>::from_timestamp(
                    execution_timestamp as i64,
                    ((execution_timestamp % 1.0) * 1e9) as u32,
                )
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("invalid timestamp")
                })?;
                let expected = expected_last_timestamp
                    .map(|ts| {
                        DateTime::<Utc>::from_timestamp(
                            ts as i64,
                            ((ts % 1.0) * 1e9) as u32,
                        )
                        .ok_or_else(|| {
                            pyo3::exceptions::PyValueError::new_err(
                                "invalid expected timestamp",
                            )
                        })
                    })
                    .transpose()?;
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.store_cron_execution(&cid, exec_time, expected)))
                    .map_err(crate::error::to_py_err)
            }

            /// Claim a trigger run. Returns true if successfully claimed.
            fn claim_trigger_run(
                &self, py: pyo3::Python<'_>,
                trigger_run_id: &str,
            ) -> pyo3::PyResult<bool> {
                use rustvello_core::trigger::TriggerStore;
                let run_id = rustvello_proto::trigger::TriggerRunId::from(
                    trigger_run_id.to_string(),
                );
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.claim_trigger_run(&run_id)))
                    .map_err(crate::error::to_py_err)
            }

            // ─── TriggerManager methods ──────────────────────────────

            /// Report a status change. Returns list of valid condition IDs.
            fn report_status_change(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                status: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_proto::trigger::StatusContext;
                let inv_status = crate::orchestrator::parse_status(status)?;
                let ctx = StatusContext {
                    invocation_id: crate::utils::parse_invocation_id(invocation_id)?,
                    task_id: rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                        task_module,
                        task_name,
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?,
                    status: inv_status,
                    arguments: std::collections::BTreeMap::new(),
                };
                let rt = crate::runtime::shared_runtime()?;
                let valid = py.allow_threads(|| rt.block_on(self.manager.report_status_change(&ctx)))
                    .map_err(crate::error::to_py_err)?;
                Ok(valid.iter().map(|vc| vc.valid_condition_id.clone()).collect())
            }

            /// Report a successful result. Returns list of valid condition IDs.
            fn report_result(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                result: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_proto::trigger::ResultContext;
                let ctx = ResultContext {
                    invocation_id: crate::utils::parse_invocation_id(invocation_id)?,
                    task_id: rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                        task_module,
                        task_name,
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?,
                    result: serde_json::Value::String(result.to_string()),
                    arguments: std::collections::BTreeMap::new(),
                };
                let rt = crate::runtime::shared_runtime()?;
                let valid = py.allow_threads(|| rt.block_on(self.manager.report_result(&ctx)))
                    .map_err(crate::error::to_py_err)?;
                Ok(valid.iter().map(|vc| vc.valid_condition_id.clone()).collect())
            }

            /// Report a failure. Returns list of valid condition IDs.
            fn report_failure(
                &self, py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
                error_type: &str,
                error_message: &str,
            ) -> pyo3::PyResult<Vec<String>> {
                use rustvello_proto::trigger::ExceptionContext;
                let ctx = ExceptionContext {
                    invocation_id: crate::utils::parse_invocation_id(invocation_id)?,
                    task_id: rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python,
                        task_module,
                        task_name,
                    )
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?,
                    error_type: error_type.to_string(),
                    error_message: error_message.to_string(),
                    arguments: std::collections::BTreeMap::new(),
                };
                let rt = crate::runtime::shared_runtime()?;
                let valid = py.allow_threads(|| rt.block_on(self.manager.report_failure(&ctx)))
                    .map_err(crate::error::to_py_err)?;
                Ok(valid.iter().map(|vc| vc.valid_condition_id.clone()).collect())
            }

            /// Emit a custom event. Returns the generated event ID.
            fn emit_event(
                &self, py: pyo3::Python<'_>,
                event_code: &str,
                payload_json: &str,
            ) -> pyo3::PyResult<String> {
                let payload: serde_json::Value =
                    serde_json::from_str(payload_json).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(self.manager.emit_event(event_code, payload)))
                    .map_err(crate::error::to_py_err)
            }

            /// Evaluate all cron conditions. Returns list of valid condition IDs.
            fn evaluate_cron_conditions(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<String>> {
                let rt = crate::runtime::shared_runtime()?;
                let valid = py.allow_threads(|| rt.block_on(self.manager.evaluate_cron_conditions()))
                    .map_err(crate::error::to_py_err)?;
                Ok(valid.iter().map(|vc| vc.valid_condition_id.clone()).collect())
            }

            /// Evaluate all pending triggers. Returns list of (trigger_json, args_json).
            fn evaluate_triggers(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Vec<(String, String)>> {
                let rt = crate::runtime::shared_runtime()?;
                let to_invoke = py.allow_threads(|| rt.block_on(self.manager.evaluate_triggers()))
                    .map_err(crate::error::to_py_err)?;
                to_invoke
                    .iter()
                    .map(|(trigger, args)| {
                        let trigger_json =
                            serde_json::to_string(trigger).map_err(|e| {
                                pyo3::exceptions::PyValueError::new_err(e.to_string())
                            })?;
                        let args_json =
                            serde_json::to_string(args).map_err(|e| {
                                pyo3::exceptions::PyValueError::new_err(e.to_string())
                            })?;
                        Ok((trigger_json, args_json))
                    })
                    .collect()
            }

            // ─── Typed condition registration ────────────────────────

            /// Register a status condition. Returns the condition_id.
            #[pyo3(signature = (task_module, task_name, statuses, argument_filter_json=None))]
            fn register_status_condition(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                statuses: Vec<String>,
                argument_filter_json: Option<&str>,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::{StatusCondition, TriggerCondition};
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let parsed_statuses: Vec<_> = statuses
                    .iter()
                    .map(|s| crate::orchestrator::parse_status(s))
                    .collect::<pyo3::PyResult<_>>()?;
                let argument_filter =
                    crate::trigger::parse_optional_filter(argument_filter_json)?;
                let condition = TriggerCondition::Status(StatusCondition {
                    task_id,
                    statuses: parsed_statuses,
                    argument_filter,
                });
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Register a cron condition. Returns the condition_id.
            #[pyo3(signature = (cron_expression, min_interval_seconds=0))]
            fn register_cron_condition(
                &self, py: pyo3::Python<'_>,
                cron_expression: &str,
                min_interval_seconds: u64,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::{CronCondition, TriggerCondition};
                let condition = TriggerCondition::Cron(CronCondition {
                    cron_expression: cron_expression.to_string(),
                    min_interval_seconds,
                });
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Register an event condition. Returns the condition_id.
            #[pyo3(signature = (event_code, payload_filter_json=None))]
            fn register_event_condition(
                &self, py: pyo3::Python<'_>,
                event_code: &str,
                payload_filter_json: Option<&str>,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::{EventCondition, TriggerCondition};
                let payload_filter =
                    crate::trigger::parse_optional_filter(payload_filter_json)?;
                let condition = TriggerCondition::Event(EventCondition {
                    event_code: event_code.to_string(),
                    payload_filter,
                });
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Register a result condition. Returns the condition_id.
            #[pyo3(signature = (task_module, task_name, argument_filter_json=None))]
            fn register_result_condition(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                argument_filter_json: Option<&str>,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::{
                    ResultCondition, TriggerCondition,
                };
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let argument_filter =
                    crate::trigger::parse_optional_filter(argument_filter_json)?;
                let condition = TriggerCondition::Result(ResultCondition {
                    task_id,
                    argument_filter,
                    result_filter: None,
                });
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Register an exception condition. Returns the condition_id.
            #[pyo3(signature = (task_module, task_name, exception_types=None, argument_filter_json=None))]
            fn register_exception_condition(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                exception_types: Option<Vec<String>>,
                argument_filter_json: Option<&str>,
            ) -> pyo3::PyResult<String> {
                use rustvello_core::trigger::TriggerStore;
                use rustvello_proto::trigger::{
                    ExceptionCondition, TriggerCondition,
                };
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let argument_filter =
                    crate::trigger::parse_optional_filter(argument_filter_json)?;
                let condition =
                    TriggerCondition::Exception(ExceptionCondition {
                        task_id,
                        exception_types: exception_types.unwrap_or_default(),
                        argument_filter,
                    });
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                let cid = py.allow_threads(|| rt.block_on(store.register_condition(&condition)))
                    .map_err(crate::error::to_py_err)?;
                Ok(cid.to_string())
            }

            /// Register a trigger definition with typed fields.
            #[pyo3(signature = (task_module, task_name, condition_ids, logic="All", argument_template_json=None))]
            fn register_trigger_typed(
                &self, py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
                condition_ids: Vec<String>,
                logic: &str,
                argument_template_json: Option<&str>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::trigger::TriggerStore;
                let task_id =
                    rustvello_proto::identifiers::TaskId::try_for_language(rustvello_proto::identifiers::TaskLanguage::Python, task_module, task_name)
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })?;
                let cids: Vec<rustvello_proto::trigger::ConditionId> = condition_ids
                    .into_iter()
                    .map(rustvello_proto::trigger::ConditionId::from)
                    .collect();
                let trigger_logic = match logic {
                    "All" | "And" | "AND" => {
                        rustvello_proto::trigger::TriggerLogic::And
                    }
                    "Any" | "Or" | "OR" => {
                        rustvello_proto::trigger::TriggerLogic::Or
                    }
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "logic must be 'All'/'And' or 'Any'/'Or'",
                        ));
                    }
                };
                let arg_template = argument_template_json
                    .map(|s| {
                        serde_json::from_str(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(e.to_string())
                        })
                    })
                    .transpose()?;
                let trigger_id =
                    rustvello_proto::trigger::TriggerDefinitionDTO::compute_trigger_id(
                        &task_id,
                        &cids,
                        trigger_logic.clone(),
                    );
                let trigger = rustvello_proto::trigger::TriggerDefinitionDTO {
                    trigger_id,
                    task_id,
                    condition_ids: cids,
                    logic: trigger_logic,
                    argument_template: arg_template,
                };
                let store = std::sync::Arc::clone(&self.store)
                    as std::sync::Arc<dyn TriggerStore>;
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(store.register_trigger(&trigger)))
                    .map_err(crate::error::to_py_err)
            }
        }
    };
}
