//! Final assembly: `build()` and backend resolution.

use std::sync::Arc;

use cistell_core::Resolver;
use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_proto::config::AppConfig;

use crate::app::{RustvelloApp, TaskEntry};

use super::RustvelloBuilder;

/// All five backend trait objects created from a preset.
struct ResolvedBackends {
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn Orchestrator>,
    state_backend: Arc<dyn StateBackend>,
    client_data_store: Arc<dyn ClientDataStore>,
    trigger_store: Arc<dyn TriggerStore>,
}

impl RustvelloBuilder {
    /// Consume the builder and produce a [`RustvelloApp`].
    ///
    /// Establishes all backend connections (database, Redis, etc.) that were
    /// configured via preset methods like [`.sqlite()`](Self::sqlite),
    /// [`.redis()`](Self::redis), or [`.postgres()`](Self::postgres).
    ///
    /// If no backends were explicitly provided, falls back to:
    /// - In-memory backends (when the `mem` feature is enabled)
    /// - Error otherwise
    ///
    /// If `auto_discover_tasks()` was called, all tasks registered via
    /// `#[rustvello::task]` are automatically added to the registry.
    /// Additionally, any task modules added via `task_module()` are
    /// registered.
    pub async fn build(mut self) -> RustvelloResult<RustvelloApp> {
        // -----------------------------------------------------------------------
        // Resolve AppConfig via cistell (programmatic > [env >] file > pyproject > defaults)
        // -----------------------------------------------------------------------
        let config: AppConfig = {
            let mut builder = Resolver::builder().add_source(self.programmatic.clone());

            if self.use_env {
                builder = builder.env();
            }

            for path in &self.file_paths {
                builder = builder
                    .file(path)
                    .map_err(|e| RustvelloError::Configuration {
                        message: e.to_string(),
                    })?;
            }

            builder = builder.pyproject_toml("rustvello", "app").map_err(|e| {
                RustvelloError::Configuration {
                    message: e.to_string(),
                }
            })?;

            let resolved = builder.build().resolve::<AppConfig>().map_err(|e| {
                RustvelloError::Configuration {
                    message: e.to_string(),
                }
            })?;

            tracing::info!("Configuration loaded:\n{}", resolved.explain());
            resolved.value
        };

        // Validate timing configuration
        if config.heartbeat_interval_seconds == 0 {
            return Err(RustvelloError::Configuration {
                message: "heartbeat_interval_seconds must be > 0".into(),
            });
        }
        if config.recovery_check_interval_seconds == 0 {
            return Err(RustvelloError::Configuration {
                message: "recovery_check_interval_seconds must be > 0".into(),
            });
        }

        // Materialise the preset (if any) into concrete backend objects.
        let preset_backends = self.resolve_preset().await?;

        // Apply preset backends — individual `.broker()` / `.state_backend()`
        // etc. take precedence over the preset.
        if let Some(pb) = preset_backends {
            if self.broker.is_none() {
                self.broker = Some(pb.broker);
            }
            if self.orchestrator.is_none() {
                self.orchestrator = Some(pb.orchestrator);
            }
            if self.state_backend.is_none() {
                self.state_backend = Some(pb.state_backend);
            }
            if self.client_data_store.is_none() {
                self.client_data_store = Some(pb.client_data_store);
            }
            if self.trigger_store.is_none() {
                self.trigger_store = Some(pb.trigger_store);
            }
        }

        // RabbitMQ overrides only the broker (if configured).
        #[cfg(feature = "rabbitmq")]
        if let Some(ref rmq) = self.rabbitmq_config {
            self.broker = Some(Arc::new(rustvello_rabbitmq::prelude::RabbitMqBroker::new(
                &rmq.uri,
                &rmq.prefix,
            )));
        }

        let broker = self.resolve_broker()?;
        let orchestrator = self.resolve_orchestrator()?;
        let state_backend = self.resolve_state_backend()?;
        let cds_backend = self.resolve_client_data_store()?;
        let cds_manager = Arc::new(ClientDataStoreManager::new(
            cds_backend,
            self.client_data_store_config,
        ));

        let mut app =
            RustvelloApp::with_backends(config, broker, orchestrator, state_backend, cds_manager);
        app.set_task_config_overrides(self.task_config_overrides, self.task_defaults_override);

        // Auto-discover tasks from inventory (compile-time registration)
        if self.auto_discover {
            let mut discovered = 0u32;
            for entry in inventory::iter::<TaskEntry> {
                (entry.register_fn)(&mut app.task_registry)?;
                discovered += 1;
            }
            tracing::info!("Auto-discovered {} tasks via inventory", discovered);
        }

        // Register task modules
        for module in &self.task_modules {
            tracing::info!("Registering task module: {}", module.name());
            module.register(&mut app.task_registry)?;
        }

        // Configure trigger manager if a trigger store was provided
        if let Some(trigger_store) = self.trigger_store {
            app.set_trigger_manager(TriggerManager::new(trigger_store));
        }

        Ok(app)
    }

    // ------------------------------------------------------------------
    // Preset materialisation
    // ------------------------------------------------------------------

    async fn resolve_preset(&self) -> RustvelloResult<Option<ResolvedBackends>> {
        let preset = match self.backend_preset {
            Some(ref p) => p,
            None => return Ok(None),
        };
        match preset {
            #[cfg(feature = "mem")]
            super::BackendPreset::Memory => Ok(Some(ResolvedBackends {
                broker: Arc::new(rustvello_mem::broker::MemBroker::new()),
                orchestrator: Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()),
                state_backend: Arc::new(rustvello_mem::state_backend::MemStateBackend::new()),
                client_data_store: Arc::new(
                    rustvello_mem::client_data_store::MemClientDataStore::new(),
                ),
                trigger_store: Arc::new(rustvello_mem::trigger::MemTriggerStore::new()),
            })),
            #[cfg(feature = "sqlite")]
            super::BackendPreset::Sqlite { path, app_id } => {
                let db = Arc::new(rustvello_sqlite::db::Database::open(path, app_id)?);
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_sqlite::broker::SqliteBroker::new(Arc::clone(&db))),
                    orchestrator: Arc::new(
                        rustvello_sqlite::orchestrator::SqliteOrchestrator::new(Arc::clone(&db)),
                    ),
                    state_backend: Arc::new(
                        rustvello_sqlite::state_backend::SqliteStateBackend::new(Arc::clone(&db)),
                    ),
                    client_data_store: Arc::new(
                        rustvello_sqlite::client_data_store::SqliteClientDataStore::new(
                            Arc::clone(&db),
                        ),
                    ),
                    trigger_store: Arc::new(rustvello_sqlite::trigger::SqliteTriggerStore::new(db)),
                }))
            }
            #[cfg(feature = "redis")]
            super::BackendPreset::Redis { uri, app_id } => {
                let pool = Arc::new(rustvello_redis::prelude::RedisPool::new(uri, app_id)?);
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_redis::prelude::RedisBroker::new(Arc::clone(
                        &pool,
                    ))),
                    orchestrator: Arc::new(rustvello_redis::prelude::RedisOrchestrator::new(
                        Arc::clone(&pool),
                    )),
                    state_backend: Arc::new(rustvello_redis::prelude::RedisStateBackend::new(
                        Arc::clone(&pool),
                    )),
                    client_data_store: Arc::new(
                        rustvello_redis::prelude::RedisClientDataStore::new(Arc::clone(&pool)),
                    ),
                    trigger_store: Arc::new(rustvello_redis::prelude::RedisTriggerStore::new(pool)),
                }))
            }
            #[cfg(feature = "postgres")]
            super::BackendPreset::Postgres {
                connection_string,
                app_id,
            } => {
                let db = Arc::new(
                    rustvello_postgres::prelude::Database::connect(connection_string, app_id)
                        .await?,
                );
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_postgres::prelude::PostgresBroker::new(
                        Arc::clone(&db),
                    )),
                    orchestrator: Arc::new(rustvello_postgres::prelude::PostgresOrchestrator::new(
                        Arc::clone(&db),
                    )),
                    state_backend: Arc::new(
                        rustvello_postgres::prelude::PostgresStateBackend::new(Arc::clone(&db)),
                    ),
                    client_data_store: Arc::new(
                        rustvello_postgres::prelude::PostgresClientDataStore::new(Arc::clone(&db)),
                    ),
                    trigger_store: Arc::new(
                        rustvello_postgres::prelude::PostgresTriggerStore::new(db),
                    ),
                }))
            }
            #[cfg(all(feature = "postgres", feature = "tls"))]
            super::BackendPreset::PostgresTls {
                connection_string,
                app_id,
            } => {
                let db = Arc::new(
                    rustvello_postgres::prelude::Database::connect_tls(connection_string, app_id)
                        .await?,
                );
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_postgres::prelude::PostgresBroker::new(
                        Arc::clone(&db),
                    )),
                    orchestrator: Arc::new(rustvello_postgres::prelude::PostgresOrchestrator::new(
                        Arc::clone(&db),
                    )),
                    state_backend: Arc::new(
                        rustvello_postgres::prelude::PostgresStateBackend::new(Arc::clone(&db)),
                    ),
                    client_data_store: Arc::new(
                        rustvello_postgres::prelude::PostgresClientDataStore::new(Arc::clone(&db)),
                    ),
                    trigger_store: Arc::new(
                        rustvello_postgres::prelude::PostgresTriggerStore::new(db),
                    ),
                }))
            }
            #[cfg(feature = "mongodb")]
            super::BackendPreset::MongoDB {
                uri,
                db_name,
                app_id,
            } => {
                let pool = Arc::new(rustvello_mongo::prelude::MongoPool::new(
                    uri, db_name, app_id,
                ));
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_mongo::prelude::MongoBroker::new(Arc::clone(
                        &pool,
                    ))),
                    orchestrator: Arc::new(rustvello_mongo::prelude::MongoOrchestrator::new(
                        Arc::clone(&pool),
                    )),
                    state_backend: Arc::new(rustvello_mongo::prelude::MongoStateBackend::new(
                        Arc::clone(&pool),
                    )),
                    client_data_store: Arc::new(
                        rustvello_mongo::prelude::MongoClientDataStore::new(Arc::clone(&pool)),
                    ),
                    trigger_store: Arc::new(rustvello_mongo::prelude::MongoTriggerStore::new(pool)),
                }))
            }
            #[cfg(feature = "mongodb3")]
            super::BackendPreset::Mongo3 {
                uri,
                db_name,
                app_id,
            } => {
                let pool = Arc::new(rustvello_mongo3::prelude::MongoPool::new(
                    uri, db_name, app_id,
                ));
                Ok(Some(ResolvedBackends {
                    broker: Arc::new(rustvello_mongo3::prelude::Mongo3Broker::new(Arc::clone(
                        &pool,
                    ))),
                    orchestrator: Arc::new(rustvello_mongo3::prelude::Mongo3Orchestrator::new(
                        Arc::clone(&pool),
                    )),
                    state_backend: Arc::new(rustvello_mongo3::prelude::Mongo3StateBackend::new(
                        Arc::clone(&pool),
                    )),
                    client_data_store: Arc::new(
                        rustvello_mongo3::prelude::Mongo3ClientDataStore::new(Arc::clone(&pool)),
                    ),
                    trigger_store: Arc::new(rustvello_mongo3::prelude::Mongo3TriggerStore::new(
                        pool,
                    )),
                }))
            }
        }
    }

    // ------------------------------------------------------------------
    // Fallback resolution (explicit backend or mem default)
    // ------------------------------------------------------------------

    fn resolve_broker(&self) -> RustvelloResult<Arc<dyn Broker>> {
        if let Some(ref b) = self.broker {
            return Ok(Arc::clone(b));
        }
        #[cfg(feature = "mem")]
        {
            return Ok(Arc::new(rustvello_mem::broker::MemBroker::new()));
        }
        #[allow(unreachable_code)]
        Err(RustvelloError::Configuration {
            message: "no broker configured and no default backend feature enabled".into(),
        })
    }

    fn resolve_orchestrator(&self) -> RustvelloResult<Arc<dyn Orchestrator>> {
        if let Some(ref o) = self.orchestrator {
            return Ok(Arc::clone(o));
        }
        #[cfg(feature = "mem")]
        {
            return Ok(Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()));
        }
        #[allow(unreachable_code)]
        Err(RustvelloError::Configuration {
            message: "no orchestrator configured and no default backend feature enabled".into(),
        })
    }

    fn resolve_state_backend(&self) -> RustvelloResult<Arc<dyn StateBackend>> {
        if let Some(ref sb) = self.state_backend {
            return Ok(Arc::clone(sb));
        }
        #[cfg(feature = "mem")]
        {
            return Ok(Arc::new(
                rustvello_mem::state_backend::MemStateBackend::new(),
            ));
        }
        #[allow(unreachable_code)]
        Err(RustvelloError::Configuration {
            message: "no state backend configured and no default backend feature enabled".into(),
        })
    }

    fn resolve_client_data_store(&self) -> RustvelloResult<Arc<dyn ClientDataStore>> {
        if let Some(ref cds) = self.client_data_store {
            return Ok(Arc::clone(cds));
        }
        #[cfg(feature = "mem")]
        {
            return Ok(Arc::new(
                rustvello_mem::client_data_store::MemClientDataStore::new(),
            ));
        }
        #[allow(unreachable_code)]
        Err(RustvelloError::Configuration {
            message: "no client data store configured and no default backend feature enabled"
                .into(),
        })
    }
}
