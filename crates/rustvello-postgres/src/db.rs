//! PostgreSQL database wrapper with connection pooling and schema initialization.

use deadpool_postgres::Pool;
use tokio_postgres::NoTls;

use rustvello_core::error::{RustvelloError, RustvelloResult};

/// Format a `tokio_postgres::Error` with full `DbError` details when available.
///
/// `tokio_postgres::Error::Display` only writes the kind string (e.g. `"db error"`)
/// and does NOT include the server message. This helper extracts the `DbError`
/// fields so we get actionable diagnostics.
fn fmt_pg(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        use std::fmt::Write;
        let mut msg = format!(
            "{}: {} (code: {})",
            db.severity(),
            db.message(),
            db.code().code()
        );
        if let Some(detail) = db.detail() {
            let _ = write!(msg, " detail={detail}");
        }
        if let Some(hint) = db.hint() {
            let _ = write!(msg, " hint={hint}");
        }
        msg
    } else {
        e.to_string()
    }
}

/// Shared PostgreSQL database connection pool with schema initialization.
///
/// Data is isolated by `app_id`: each app gets its own PostgreSQL schema
/// (`CREATE SCHEMA IF NOT EXISTS "{app_id}"`), and every connection
/// obtained through [`conn()`](Self::conn) has `search_path` set to that
/// schema.  Two `Database` instances with different `app_id` values
/// sharing the same physical database will not see each other's tables.
pub struct Database {
    pub(crate) pool: Pool,
    app_id: String,
}

impl Database {
    /// Create a new database from a connection string.
    ///
    /// The connection string should be in the format:
    /// `host=localhost user=postgres password=secret dbname=rustvello`
    /// or a URI: `postgres://user:pass@host/dbname`
    ///
    /// The `app_id` is used to create a dedicated PostgreSQL schema so
    /// that multiple applications can share the same database without
    /// interference.
    ///
    /// **Note:** This method uses `NoTls`, so all data is sent in cleartext.
    /// For encrypted connections, use [`connect_tls()`](Self::connect_tls)
    /// (requires the `tls` feature) or [`from_pool()`](Self::from_pool)
    /// with a custom pool configuration.
    pub async fn connect(connection_string: &str, app_id: &str) -> RustvelloResult<Self> {
        Self::connect_with_pool_size(connection_string, app_id, None).await
    }

    /// Like [`connect()`](Self::connect) but with a configurable maximum pool size.
    ///
    /// When `max_size` is `None`, deadpool's default (CPU count × 4) is used.
    pub async fn connect_with_pool_size(
        connection_string: &str,
        app_id: &str,
        max_size: Option<usize>,
    ) -> RustvelloResult<Self> {
        let pg_config: tokio_postgres::Config =
            connection_string
                .parse()
                .map_err(|e: tokio_postgres::Error| {
                    RustvelloError::state_backend(format!("invalid Postgres config: {e}"))
                })?;

        let mgr = deadpool_postgres::Manager::new(pg_config, NoTls);
        let mut builder = Pool::builder(mgr);
        if let Some(size) = max_size {
            builder = builder.max_size(size);
        }
        let pool = builder
            .build()
            .map_err(|e| RustvelloError::state_backend(format!("failed to create pool: {e}")))?;

        let db = Self {
            pool,
            app_id: app_id.to_string(),
        };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Create a new database with TLS encryption from a connection string.
    ///
    /// Uses the system CA trust store to verify server certificates.
    /// Requires the `tls` feature.
    ///
    /// The connection string should be in the format:
    /// `host=localhost user=postgres password=secret dbname=rustvello sslmode=require`
    /// or a URI: `postgres://user:pass@host/dbname?sslmode=require`
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub async fn connect_tls(connection_string: &str, app_id: &str) -> RustvelloResult<Self> {
        Self::connect_tls_with_pool_size(connection_string, app_id, None).await
    }

    /// Like [`connect_tls()`](Self::connect_tls) but with a configurable maximum pool size.
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub async fn connect_tls_with_pool_size(
        connection_string: &str,
        app_id: &str,
        max_size: Option<usize>,
    ) -> RustvelloResult<Self> {
        let pg_config: tokio_postgres::Config =
            connection_string
                .parse()
                .map_err(|e: tokio_postgres::Error| {
                    RustvelloError::state_backend(format!("invalid Postgres config: {e}"))
                })?;

        let tls_connector = native_tls::TlsConnector::new().map_err(|e| {
            RustvelloError::state_backend(format!("failed to create TLS connector: {e}"))
        })?;
        let pg_tls = postgres_native_tls::MakeTlsConnector::new(tls_connector);

        let mgr = deadpool_postgres::Manager::new(pg_config, pg_tls);
        let mut builder = Pool::builder(mgr);
        if let Some(size) = max_size {
            builder = builder.max_size(size);
        }
        let pool = builder
            .build()
            .map_err(|e| RustvelloError::state_backend(format!("failed to create pool: {e}")))?;

        let db = Self {
            pool,
            app_id: app_id.to_string(),
        };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Create a database from an existing deadpool pool.
    pub async fn from_pool(pool: Pool, app_id: &str) -> RustvelloResult<Self> {
        let db = Self {
            pool,
            app_id: app_id.to_string(),
        };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Get a connection from the pool.
    ///
    /// Every connection has its `search_path` set to the app-specific
    /// schema so that all subsequent SQL operates in the correct namespace.
    pub(crate) async fn conn(&self) -> RustvelloResult<deadpool_postgres::Client> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RustvelloError::state_backend(format!("pool error: {e}")))?;
        // Double-quote escaping prevents SQL injection in identifiers.
        let escaped = self.app_id.replace('"', "\"\"");
        client
            .execute(&format!("SET search_path TO \"{escaped}\""), &[])
            .await
            .map_err(|e| {
                RustvelloError::state_backend(format!("SET search_path failed: {}", fmt_pg(&e)))
            })?;
        Ok(client)
    }

    async fn initialize_schema(&self) -> RustvelloResult<()> {
        // Use a raw pool connection (without SET search_path) so we can
        // bootstrap the schema itself.
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RustvelloError::state_backend(format!("pool error: {e}")))?;

        // Create the app-specific schema and switch to it.
        let escaped = self.app_id.replace('"', "\"\"");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS \"{escaped}\"; SET search_path TO \"{escaped}\";"
            ))
            .await
            .map_err(|e| {
                RustvelloError::state_backend(format!("schema creation failed: {}", fmt_pg(&e)))
            })?;

        client
            .batch_execute(
                "
            -- Broker queue
            CREATE TABLE IF NOT EXISTS broker_queue (
                id BIGSERIAL PRIMARY KEY,
                invocation_id TEXT NOT NULL,
                task_id TEXT,
                queue_name TEXT NOT NULL,
                priority DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_broker_queue_route
                ON broker_queue(queue_name, priority DESC, id ASC);
            CREATE INDEX IF NOT EXISTS idx_broker_queue_task
                ON broker_queue(queue_name, task_id, priority DESC, id ASC);

            -- Invocations
            CREATE TABLE IF NOT EXISTS invocations (
                invocation_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                call_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                parent_invocation_id TEXT,
                workflow_id TEXT,
                workflow_type TEXT,
                workflow_depth INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_invocations_task
                ON invocations(task_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_call
                ON invocations(call_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_status
                ON invocations(status);
            CREATE INDEX IF NOT EXISTS idx_invocations_workflow
                ON invocations(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_parent
                ON invocations(parent_invocation_id);

            -- Calls (arguments)
            CREATE TABLE IF NOT EXISTS calls (
                call_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                serialized_arguments TEXT NOT NULL
            );

            -- Results
            CREATE TABLE IF NOT EXISTS results (
                invocation_id TEXT PRIMARY KEY,
                result TEXT NOT NULL
            );

            -- Errors
            CREATE TABLE IF NOT EXISTS errors (
                invocation_id TEXT PRIMARY KEY,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                traceback TEXT
            );

            -- Status history
            CREATE TABLE IF NOT EXISTS history (
                id BIGSERIAL PRIMARY KEY,
                invocation_id TEXT NOT NULL,
                status TEXT NOT NULL,
                runner_id TEXT,
                timestamp TIMESTAMPTZ NOT NULL,
                message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_invocation
                ON history(invocation_id);

            -- Status records (current status with runner ownership)
            CREATE TABLE IF NOT EXISTS status_records (
                invocation_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                runner_id TEXT,
                timestamp TIMESTAMPTZ NOT NULL
            );

            -- Waiting-for relationships (blocking control)
            CREATE TABLE IF NOT EXISTS waiting_for (
                waiter_id TEXT NOT NULL,
                waited_on_id TEXT NOT NULL,
                PRIMARY KEY (waiter_id, waited_on_id)
            );
            CREATE INDEX IF NOT EXISTS idx_waiting_for_waited_on
                ON waiting_for(waited_on_id);

            -- Concurrency control: per-argument-pair index
            CREATE TABLE IF NOT EXISTS cc_arg_pairs (
                invocation_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                arg_key TEXT NOT NULL,
                arg_value TEXT NOT NULL,
                PRIMARY KEY (invocation_id, arg_key, arg_value)
            );
            CREATE INDEX IF NOT EXISTS idx_cc_arg_lookup
                ON cc_arg_pairs(task_id, arg_key, arg_value);

            -- Client data store
            CREATE TABLE IF NOT EXISTS client_data (
                data_key TEXT PRIMARY KEY,
                data_value TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Runner heartbeats
            CREATE TABLE IF NOT EXISTS runner_heartbeats (
                runner_id TEXT PRIMARY KEY,
                last_heartbeat TIMESTAMPTZ NOT NULL
            );

            -- Bounded atomic-service execution history for monitoring
            CREATE TABLE IF NOT EXISTS atomic_service_timeline (
                id BIGSERIAL PRIMARY KEY,
                runner_id TEXT NOT NULL,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_atomic_service_timeline_start
                ON atomic_service_timeline(start_time DESC, id DESC);

            -- Invocation retry counts
            CREATE TABLE IF NOT EXISTS retries (
                invocation_id TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            );

            -- Trigger conditions
            CREATE TABLE IF NOT EXISTS trg_conditions (
                condition_id TEXT PRIMARY KEY,
                condition_type TEXT NOT NULL DEFAULT '',
                condition_json TEXT NOT NULL,
                event_code TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trg_cond_type
                ON trg_conditions(condition_type);
            CREATE INDEX IF NOT EXISTS idx_trg_cond_event_code
                ON trg_conditions(event_code);

            -- Trigger definitions
            CREATE TABLE IF NOT EXISTS trg_triggers (
                trigger_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                logic TEXT NOT NULL,
                argument_template TEXT
            );

            -- Condition-to-trigger mapping (many-to-many)
            CREATE TABLE IF NOT EXISTS trg_condition_triggers (
                condition_id TEXT NOT NULL,
                trigger_id TEXT NOT NULL,
                PRIMARY KEY (condition_id, trigger_id)
            );
            CREATE INDEX IF NOT EXISTS idx_trg_ct_trigger
                ON trg_condition_triggers(trigger_id);

            -- Valid conditions (pending evaluation)
            CREATE TABLE IF NOT EXISTS trg_valid_conditions (
                valid_condition_id TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                context_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trg_vc_condition
                ON trg_valid_conditions(condition_id);

            -- Source task → condition mapping (for fast lookup)
            CREATE TABLE IF NOT EXISTS trg_source_task_conditions (
                task_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                PRIMARY KEY (task_id, condition_id)
            );

            -- Cron execution tracking
            CREATE TABLE IF NOT EXISTS trg_cron_executions (
                condition_id TEXT PRIMARY KEY,
                last_execution TIMESTAMPTZ NOT NULL
            );

            -- Trigger run claims (dedup)
            CREATE TABLE IF NOT EXISTS trg_trigger_run_claims (
                trigger_run_id TEXT PRIMARY KEY,
                claimed_at TIMESTAMPTZ NOT NULL
            );

            -- Auto-purge schedule
            CREATE TABLE IF NOT EXISTS auto_purge_schedule (
                invocation_id TEXT PRIMARY KEY,
                scheduled_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_auto_purge_schedule_at
                ON auto_purge_schedule(scheduled_at);

            -- Durable trigger monitoring records
            CREATE TABLE IF NOT EXISTS trg_events (
                event_id TEXT PRIMARY KEY,
                event_code TEXT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                emitted_by_invocation_id TEXT,
                event_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trg_events_code_time
                ON trg_events(event_code, event_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trg_events_emitter
                ON trg_events(emitted_by_invocation_id, event_timestamp DESC);
            CREATE TABLE IF NOT EXISTS trg_trigger_runs (
                trigger_run_id TEXT PRIMARY KEY,
                claimed_at TIMESTAMPTZ NOT NULL,
                triggered_invocation_id TEXT,
                run_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trg_runs_claimed
                ON trg_trigger_runs(claimed_at DESC);
            CREATE INDEX IF NOT EXISTS idx_trg_runs_triggered_invocation
                ON trg_trigger_runs(triggered_invocation_id);
            CREATE TABLE IF NOT EXISTS trg_trigger_run_events (
                trigger_run_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY (trigger_run_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_trg_run_events_event
                ON trg_trigger_run_events(event_id);
            CREATE TABLE IF NOT EXISTS trg_trigger_run_sources (
                trigger_run_id TEXT NOT NULL,
                invocation_id TEXT NOT NULL,
                PRIMARY KEY (trigger_run_id, invocation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_trg_run_sources_invocation
                ON trg_trigger_run_sources(invocation_id);

            -- Workflow runs (discovery + tracking)
            CREATE TABLE IF NOT EXISTS workflow_runs (
                workflow_id TEXT PRIMARY KEY,
                workflow_type TEXT NOT NULL,
                parent_workflow_id TEXT,
                depth INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_workflow_runs_type
                ON workflow_runs(workflow_type);

            -- Workflow key-value data store
            CREATE TABLE IF NOT EXISTS workflow_data (
                workflow_id TEXT NOT NULL,
                data_key TEXT NOT NULL,
                data_value TEXT NOT NULL,
                PRIMARY KEY (workflow_id, data_key)
            );

            -- App info storage
            CREATE TABLE IF NOT EXISTS app_infos (
                app_id TEXT PRIMARY KEY,
                info_json TEXT NOT NULL
            );

            -- Workflow sub-invocation tracking
            CREATE TABLE IF NOT EXISTS workflow_sub_invocations (
                workflow_id TEXT NOT NULL,
                sub_invocation_id TEXT NOT NULL,
                PRIMARY KEY (workflow_id, sub_invocation_id)
            );

            -- Runner execution contexts
            CREATE TABLE IF NOT EXISTS runner_contexts (
                runner_id TEXT PRIMARY KEY,
                runner_cls TEXT NOT NULL,
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                thread_id BIGINT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                parent_runner_id TEXT,
                parent_runner_cls TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runner_contexts_parent
                ON runner_contexts(parent_runner_id);

            -- Add history_timestamp for time-range queries
            ALTER TABLE history ADD COLUMN IF NOT EXISTS history_timestamp TIMESTAMPTZ;
            CREATE INDEX IF NOT EXISTS idx_history_runner
                ON history(runner_id);
            ",
            )
            .await
            .map_err(|e| {
                RustvelloError::state_backend(format!("schema init failed: {}", fmt_pg(&e)))
            })?;

        Ok(())
    }
}

pub(crate) fn pg_err(e: tokio_postgres::Error) -> RustvelloError {
    RustvelloError::state_backend(format!("Postgres error: {}", fmt_pg(&e)))
}

pub(crate) fn parse_status(s: &str) -> RustvelloResult<rustvello_proto::status::InvocationStatus> {
    s.parse::<rustvello_proto::status::InvocationStatus>()
        .map_err(RustvelloError::state_backend)
}
