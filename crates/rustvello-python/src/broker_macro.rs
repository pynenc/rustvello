/// Generates all trait-forwarding `#[pymethods]` for a PyO3 broker wrapper.
///
/// The target type must have an `inner` field that dereferences to a type
/// implementing `rustvello_core::broker::Broker`.
///
/// Each backend keeps its own `#[new]` constructor in a separate
/// `#[pymethods]` block. This macro generates the remaining 10 methods,
/// all of which release the GIL via `py.allow_threads()`.
macro_rules! impl_py_broker {
    ($py_type:ty) => {
        #[pyo3::pymethods]
        impl $py_type {
            /// Queue an invocation for processing.
            fn route_invocation(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.route_invocation(&id)))
                    .map_err(crate::error::to_py_err)
            }

            /// Queue multiple invocations at once.
            fn route_invocations(
                &self,
                py: pyo3::Python<'_>,
                invocation_ids: Vec<String>,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let ids: Vec<rustvello_proto::identifiers::InvocationId> = invocation_ids
                    .iter()
                    .map(|s| crate::utils::parse_invocation_id(s))
                    .collect::<pyo3::PyResult<_>>()?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.route_invocations(&ids)))
                    .map_err(crate::error::to_py_err)
            }

            /// Retrieve the next invocation to process. Returns None if empty.
            fn retrieve_invocation(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::broker::Broker;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let result = py
                    .allow_threads(|| rt.block_on(broker.retrieve_invocation(None)))
                    .map_err(crate::error::to_py_err)?;
                Ok(result.map(|id| id.as_str().to_string()))
            }

            /// Count queued invocations.
            fn count_invocations(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<usize> {
                use rustvello_core::broker::Broker;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.count_invocations(None)))
                    .map_err(crate::error::to_py_err)
            }

            /// Remove all queued invocations.
            fn purge(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.purge(None)))
                    .map_err(crate::error::to_py_err)
            }

            /// Queue an invocation for a specific task queue.
            fn route_invocation_for_task(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let task_id = rustvello_proto::identifiers::TaskId::for_language(
                    rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module,
                    task_name,
                );
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.route_invocation_for_task(&id, &task_id)))
                    .map_err(crate::error::to_py_err)
            }

            /// Retrieve the next invocation for a specific task. Returns None if empty.
            fn retrieve_invocation_for_task(
                &self,
                py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::broker::Broker;
                let task_id = rustvello_proto::identifiers::TaskId::for_language(
                    rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module,
                    task_name,
                );
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let result = py
                    .allow_threads(|| rt.block_on(broker.retrieve_invocation(Some(&task_id))))
                    .map_err(crate::error::to_py_err)?;
                Ok(result.map(|id| id.as_str().to_string()))
            }

            /// Retrieve the next invocation for a specific language worker.
            fn retrieve_invocation_for_language(
                &self,
                py: pyo3::Python<'_>,
                language: &str,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::broker::Broker;
                let language = language
                    .parse::<rustvello_proto::identifiers::TaskLanguage>()
                    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let result = py
                    .allow_threads(|| {
                        rt.block_on(broker.retrieve_invocation_for_language(language))
                    })
                    .map_err(crate::error::to_py_err)?;
                Ok(result.map(|id| id.as_str().to_string()))
            }

            /// Count queued invocations for a specific task.
            fn count_invocations_for_task(
                &self,
                py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<usize> {
                use rustvello_core::broker::Broker;
                let task_id = rustvello_proto::identifiers::TaskId::for_language(
                    rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module,
                    task_name,
                );
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.count_invocations(Some(&task_id))))
                    .map_err(crate::error::to_py_err)
            }

            /// Remove all queued invocations for a specific task.
            fn purge_task(
                &self,
                py: pyo3::Python<'_>,
                task_module: &str,
                task_name: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let task_id = rustvello_proto::identifiers::TaskId::for_language(
                    rustvello_proto::identifiers::TaskLanguage::Python,
                    task_module,
                    task_name,
                );
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(broker.purge(Some(&task_id))))
                    .map_err(crate::error::to_py_err)
            }

            /// Queue an invocation on a logical queue with a priority.
            ///
            /// A task identity keeps the invocation on that language lane; without one it is
            /// queued task-less on the administrative lane, like `route_invocation`.
            #[pyo3(signature = (invocation_id, queue_name, priority, task_module=None, task_name=None, language="python"))]
            #[allow(clippy::too_many_arguments)]
            fn route_invocation_to_queue(
                &self,
                py: pyo3::Python<'_>,
                invocation_id: &str,
                queue_name: &str,
                priority: f64,
                task_module: Option<&str>,
                task_name: Option<&str>,
                language: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let id = crate::utils::parse_invocation_id(invocation_id)?;
                let task_id = crate::utils::optional_task_id(language, task_module, task_name)?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| {
                    rt.block_on(broker.route_invocation_with_options(
                        &id,
                        task_id.as_ref(),
                        queue_name,
                        priority,
                    ))
                })
                .map_err(crate::error::to_py_err)
            }

            /// Queue several invocations on one logical queue with one priority.
            #[pyo3(signature = (invocation_ids, queue_name, priority, task_module=None, task_name=None, language="python"))]
            #[allow(clippy::too_many_arguments)]
            fn route_invocations_to_queue(
                &self,
                py: pyo3::Python<'_>,
                invocation_ids: Vec<String>,
                queue_name: &str,
                priority: f64,
                task_module: Option<&str>,
                task_name: Option<&str>,
                language: &str,
            ) -> pyo3::PyResult<()> {
                use rustvello_core::broker::Broker;
                let ids: Vec<rustvello_proto::identifiers::InvocationId> = invocation_ids
                    .iter()
                    .map(|s| crate::utils::parse_invocation_id(s))
                    .collect::<pyo3::PyResult<_>>()?;
                let task_id = crate::utils::optional_task_id(language, task_module, task_name)?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                for id in &ids {
                    py.allow_threads(|| {
                        rt.block_on(broker.route_invocation_with_options(
                            id,
                            task_id.as_ref(),
                            queue_name,
                            priority,
                        ))
                    })
                    .map_err(crate::error::to_py_err)?;
                }
                Ok(())
            }

            /// Retrieve the next invocation from one logical queue.
            ///
            /// A task identity filters to that task, a `language` reads one lane, and neither
            /// scans every lane (administrative retrieval). Returns None if the queue is empty.
            #[pyo3(signature = (queue_name, language=None, task_module=None, task_name=None))]
            fn retrieve_invocation_from_queue(
                &self,
                py: pyo3::Python<'_>,
                queue_name: &str,
                language: Option<&str>,
                task_module: Option<&str>,
                task_name: Option<&str>,
            ) -> pyo3::PyResult<Option<String>> {
                use rustvello_core::broker::Broker;
                let task_id = crate::utils::optional_task_id(
                    language.unwrap_or("python"),
                    task_module,
                    task_name,
                )?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                let result = match (task_id, language) {
                    (Some(task_id), _) => py.allow_threads(|| {
                        rt.block_on(broker.retrieve_invocation_from_queue(queue_name, Some(&task_id)))
                    }),
                    (None, Some(language)) => {
                        let language = language
                            .parse::<rustvello_proto::identifiers::TaskLanguage>()
                            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
                        py.allow_threads(|| {
                            rt.block_on(
                                broker.retrieve_invocation_for_language_from_queue(language, queue_name),
                            )
                        })
                    }
                    (None, None) => py.allow_threads(|| {
                        rt.block_on(broker.retrieve_invocation_from_queue(queue_name, None))
                    }),
                }
                .map_err(crate::error::to_py_err)?;
                Ok(result.map(|id| id.as_str().to_string()))
            }

            /// Count queued invocations in the given logical queues, optionally for one task.
            #[pyo3(signature = (queue_names, task_module=None, task_name=None, language="python"))]
            fn count_invocations_in_queues(
                &self,
                py: pyo3::Python<'_>,
                queue_names: Vec<String>,
                task_module: Option<&str>,
                task_name: Option<&str>,
                language: &str,
            ) -> pyo3::PyResult<usize> {
                use rustvello_core::broker::Broker;
                let task_id = crate::utils::optional_task_id(language, task_module, task_name)?;
                let broker = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| {
                    rt.block_on(broker.count_invocations_in_queues(&queue_names, task_id.as_ref()))
                })
                .map_err(crate::error::to_py_err)
            }
        }
    };
}
