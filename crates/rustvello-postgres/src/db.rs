//! PostgreSQL database wrapper with connection pooling and schema initialization.

use crate::bounded::Client;
use deadpool_postgres::Pool;
#[cfg(feature = "tls")]
use std::fmt;
use std::{sync::Arc, time::Duration};
use tokio::time::{timeout_at, Instant};
use tokio_postgres::NoTls;

use rustvello_core::error::{RustvelloError, RustvelloResult};

/// Format a `tokio_postgres::Error` with full `DbError` details when available.
///
/// `tokio_postgres::Error::Display` only writes the kind string (e.g. `"db error"`)
/// and does NOT include the server message. This helper extracts the `DbError`
/// fields so we get actionable diagnostics.
fn configuration(message: &str) -> RustvelloError {
    RustvelloError::Configuration {
        message: message.into(),
    }
}

/// Finite per-checkout budgets; admission policy is persisted and must match all workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresOptions {
    pub max_pool_size: usize,
    pub operation_timeout_ms: u64,
    pub delivery_lease_ms: u64,
    pub max_queue_rows: u32,
    pub max_payload_bytes: u32,
}

/// Verified TLS policy for PostgreSQL connections.
///
/// The expected hostname is matched against every configured TCP host and is
/// then passed to the platform TLS verifier. Private trust roots are process
/// local and never added to the host trust store.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct PostgresTlsOptions {
    expected_hostname: String,
    private_ca_pem: Option<Arc<[u8]>>,
}

#[cfg(feature = "tls")]
impl fmt::Debug for PostgresTlsOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresTlsOptions")
            .field("expected_hostname", &self.expected_hostname)
            .field(
                "trust",
                &if self.private_ca_pem.is_some() {
                    "private-ca"
                } else {
                    "system-roots"
                },
            )
            .finish()
    }
}

#[cfg(feature = "tls")]
impl PostgresTlsOptions {
    /// Verify `expected_hostname` using the operating system trust roots.
    pub fn system_roots(expected_hostname: impl Into<String>) -> RustvelloResult<Self> {
        Self::new(expected_hostname.into(), None)
    }

    /// Verify `expected_hostname` exclusively using the supplied PEM CA.
    pub fn private_ca_pem(
        expected_hostname: impl Into<String>,
        ca_pem: impl Into<Vec<u8>>,
    ) -> RustvelloResult<Self> {
        Self::new(expected_hostname.into(), Some(Arc::from(ca_pem.into())))
    }

    fn new(expected_hostname: String, private_ca_pem: Option<Arc<[u8]>>) -> RustvelloResult<Self> {
        if expected_hostname.is_empty()
            || expected_hostname.len() > 253
            || expected_hostname.bytes().any(|byte| {
                byte.is_ascii_whitespace() || byte == 0 || byte == b'/' || byte == b'\\'
            })
        {
            return Err(configuration("invalid PostgreSQL TLS hostname"));
        }
        if private_ca_pem
            .as_ref()
            .is_some_and(|pem| pem.is_empty() || pem.len() > 1_048_576)
        {
            return Err(configuration("PostgreSQL private CA exceeds bounds"));
        }
        Ok(Self {
            expected_hostname,
            private_ca_pem,
        })
    }

    /// Certificate name required by this connection policy.
    pub fn expected_hostname(&self) -> &str {
        &self.expected_hostname
    }

    /// Whether this policy isolates trust to a caller-supplied private CA.
    pub fn uses_private_ca(&self) -> bool {
        self.private_ca_pem.is_some()
    }

    /// rustls client configuration for this trust policy: verified server certificates,
    /// either the private CA alone or the operating system roots, no client certificate.
    fn client_config(&self) -> RustvelloResult<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca_pem) = &self.private_ca_pem {
            let invalid = || configuration("invalid PostgreSQL private CA certificate");
            let mut added = 0usize;
            for certificate in rustls_pemfile::certs(&mut &ca_pem[..]) {
                roots
                    .add(certificate.map_err(|_| invalid())?)
                    .map_err(|_| invalid())?;
                added += 1;
            }
            if added == 0 {
                return Err(invalid());
            }
        } else {
            // unparsable system entries are skipped, as native trust stores do
            for certificate in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(certificate);
            }
            if roots.is_empty() {
                return Err(RustvelloError::state_backend(
                    "no operating system trust roots available for PostgreSQL TLS".to_owned(),
                ));
            }
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        Ok(rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| {
                RustvelloError::state_backend(format!("failed to create TLS connector: {e}"))
            })?
            .with_root_certificates(roots)
            .with_no_client_auth())
    }

    fn validate_hosts(&self, config: &tokio_postgres::Config) -> RustvelloResult<()> {
        if config.get_hosts().is_empty()
            || config.get_hosts().iter().any(|host| {
                !matches!(host, tokio_postgres::config::Host::Tcp(host) if host == &self.expected_hostname)
            })
        {
            return Err(configuration(
                "PostgreSQL TLS hostname must match every configured TCP host",
            ));
        }
        Ok(())
    }
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            max_pool_size: 4,
            operation_timeout_ms: 5_000,
            delivery_lease_ms: 60_000,
            max_queue_rows: 100_000,
            max_payload_bytes: 1_048_576,
        }
    }
}

impl PostgresOptions {
    fn validate(&self, app_id: &str) -> RustvelloResult<()> {
        if app_id.is_empty()
            || app_id.len() > 63
            || !app_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(configuration(
                "Postgres app ID must be 1-63 lowercase ASCII letters/digits/_/-",
            ));
        }
        if !(1..=64).contains(&self.max_pool_size)
            || !(100..=60_000).contains(&self.operation_timeout_ms)
            || !(100..=3_600_000).contains(&self.delivery_lease_ms)
            || !(1..=1_000_000).contains(&self.max_queue_rows)
            || !(1..=4_194_304).contains(&self.max_payload_bytes)
        {
            return Err(configuration(
                "Postgres options exceed finite profile bounds",
            ));
        }
        Ok(())
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
    pool: Pool,
    app_id: String,
    pub(crate) options: PostgresOptions,
    pub(crate) domain: rustvello_core::publication::PublicationDomain,
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
    /// When `max_size` is `None`, the bounded four-connection default is used.
    pub async fn connect_with_pool_size(
        connection_string: &str,
        app_id: &str,
        max_size: Option<usize>,
    ) -> RustvelloResult<Self> {
        Self::connect_with_options(
            connection_string,
            app_id,
            PostgresOptions {
                max_pool_size: max_size.unwrap_or(4),
                ..PostgresOptions::default()
            },
        )
        .await
    }

    /// Plaintext is deliberately restricted to explicit loopback hosts.
    pub async fn connect_with_options(
        connection_string: &str,
        app_id: &str,
        options: PostgresOptions,
    ) -> RustvelloResult<Self> {
        options.validate(app_id)?;
        let mut pg_config: tokio_postgres::Config = connection_string
            .parse()
            .map_err(|_| configuration("invalid private Postgres configuration"))?;
        if pg_config.get_hosts().is_empty() || pg_config.get_hosts().iter().any(|host| {
            !matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback()))
        }) || pg_config.get_hostaddrs().iter().any(|ip| !ip.is_loopback()) {
            return Err(configuration("plaintext PostgreSQL requires explicit loopback hosts; use TLS for remote hosts"));
        }
        pg_config.connect_timeout(Duration::from_millis(options.operation_timeout_ms));
        let mgr = deadpool_postgres::Manager::new(pg_config, NoTls);
        let pool = Pool::builder(mgr)
            .max_size(options.max_pool_size)
            .build()
            .map_err(|e| RustvelloError::state_backend(format!("failed to create pool: {e}")))?;

        Self::initialize(pool, app_id, options).await
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
        let options = PostgresOptions {
            max_pool_size: max_size.unwrap_or(4),
            ..PostgresOptions::default()
        };
        let config: tokio_postgres::Config = connection_string
            .parse()
            .map_err(|_| configuration("invalid private Postgres configuration"))?;
        let expected_hostname = config
            .get_hosts()
            .first()
            .and_then(|host| match host {
                tokio_postgres::config::Host::Tcp(host) => Some(host.clone()),
                #[allow(unreachable_patterns)]
                _ => None,
            })
            .ok_or_else(|| configuration("PostgreSQL TLS requires an explicit TCP hostname"))?;
        Self::connect_tls_with_options(
            connection_string,
            app_id,
            options,
            PostgresTlsOptions::system_roots(expected_hostname)?,
        )
        .await
    }

    /// Connect with verified TLS and the complete bounded PostgreSQL profile.
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub async fn connect_tls_with_options(
        connection_string: &str,
        app_id: &str,
        options: PostgresOptions,
        tls: PostgresTlsOptions,
    ) -> RustvelloResult<Self> {
        options.validate(app_id)?;
        let mut pg_config: tokio_postgres::Config = connection_string
            .parse()
            .map_err(|_| configuration("invalid private Postgres configuration"))?;
        tls.validate_hosts(&pg_config)?;
        pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
        pg_config.connect_timeout(Duration::from_millis(options.operation_timeout_ms));

        let pg_tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls.client_config()?);

        let mgr = deadpool_postgres::Manager::new(pg_config, pg_tls);
        let pool = Pool::builder(mgr)
            .max_size(options.max_pool_size)
            .build()
            .map_err(|e| RustvelloError::state_backend(format!("failed to create pool: {e}")))?;

        Self::initialize(pool, app_id, options).await
    }

    /// Create a database from an existing deadpool pool.
    pub async fn from_pool(pool: Pool, app_id: &str) -> RustvelloResult<Self> {
        Self::initialize(pool, app_id, PostgresOptions::default()).await
    }

    async fn initialize(
        pool: Pool,
        app_id: &str,
        options: PostgresOptions,
    ) -> RustvelloResult<Self> {
        options.validate(app_id)?;
        let db = Self {
            pool,
            app_id: app_id.to_string(),
            options,
            domain: Arc::from(rustvello_proto::identifiers::RunnerId::new().to_string()),
        };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Get a connection from the pool.
    ///
    /// Every connection has its `search_path` set to the app-specific
    /// schema so that all subsequent SQL operates in the correct namespace.
    async fn raw_conn(&self) -> RustvelloResult<Client> {
        let deadline = Instant::now() + Duration::from_millis(self.options.operation_timeout_ms);
        let client = timeout_at(deadline, self.pool.get())
            .await
            .map_err(|_| {
                RustvelloError::state_backend("Postgres pool/connection deadline exceeded")
            })?
            .map_err(|_| {
                RustvelloError::state_backend("Postgres connection/authentication failed")
            })?;
        let client = Client::new(client, deadline);
        client.batch_execute(&format!("SET synchronous_commit=on; SET statement_timeout={}; SET lock_timeout={}; SET idle_in_transaction_session_timeout={};",
            self.options.operation_timeout_ms, self.options.operation_timeout_ms / 2, self.options.operation_timeout_ms)).await?;
        Ok(client)
    }

    pub(crate) async fn conn(&self) -> RustvelloResult<Client> {
        let client = self.raw_conn().await?;
        // Double-quote escaping prevents SQL injection in identifiers.
        let escaped = self.app_id.replace('"', "\"\"");
        client
            .execute(&format!("SET search_path TO \"{escaped}\""), &[])
            .await
            .map_err(pg_err)?;
        Ok(client)
    }

    async fn initialize_schema(&self) -> RustvelloResult<()> {
        // Use a raw pool connection (without SET search_path) so we can
        // bootstrap the schema itself.
        let mut connection = self.raw_conn().await?;
        let durable: String = connection.query_one("SHOW fsync", &[]).await?.get(0);
        if durable != "on" {
            return Err(configuration("Postgres runtime requires server fsync=on"));
        }
        let client = connection.transaction().await?;
        client
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!("rustvello-schema:{}", self.app_id)],
            )
            .await?;

        // Create the app-specific schema and switch to it.
        let escaped = self.app_id.replace('"', "\"\"");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS \"{escaped}\"; SET search_path TO \"{escaped}\";"
            ))
            .await
            .map_err(pg_err)?;

        // A warm start skips the DDL batch: `ALTER TABLE` and `CREATE INDEX`
        // take table locks even when nothing changes, and could deadlock with
        // runners already working in this schema.
        let current_ddl: Option<i32> = if client
            .query_one(
                "SELECT to_regclass('rustvello_schema_version') IS NOT NULL",
                &[],
            )
            .await?
            .get(0)
        {
            client
                .query_opt("SELECT version FROM rustvello_schema_version", &[])
                .await?
                .map(|row| row.get(0))
        } else {
            None
        };
        if current_ddl != Some(SCHEMA_DDL_VERSION) {
            self.apply_schema_ddl(&client).await?;
        }
        client.execute("INSERT INTO runtime_profile (version,lease_ms,max_queue_rows,max_payload_bytes) VALUES (1,$1,$2,$3) ON CONFLICT DO NOTHING",
            &[&(self.options.delivery_lease_ms as i64), &i64::from(self.options.max_queue_rows), &i64::from(self.options.max_payload_bytes)]).await?;
        let profile = client
            .query_one(
                "SELECT version,lease_ms,max_queue_rows,max_payload_bytes FROM runtime_profile",
                &[],
            )
            .await?;
        if profile.get::<_, i32>(0) != 1
            || profile.get::<_, i64>(1) != self.options.delivery_lease_ms as i64
            || profile.get::<_, i64>(2) != i64::from(self.options.max_queue_rows)
            || profile.get::<_, i64>(3) != i64::from(self.options.max_payload_bytes)
        {
            return Err(configuration(
                "Postgres persisted runtime admission/lease profile differs",
            ));
        }
        client.commit().await?;
        Ok(())
    }

    /// Create or migrate every table and index of the app schema (additive only).
    async fn apply_schema_ddl(
        &self,
        client: &crate::bounded::Transaction<'_>,
    ) -> RustvelloResult<()> {
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
                workflow_depth INTEGER,
                traceparent TEXT,
                tracestate TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_invocations_task
                ON invocations(task_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_call
                ON invocations(call_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_status
                ON invocations(status);
            CREATE INDEX IF NOT EXISTS idx_invocations_workflow
                ON invocations(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_invocations_workflow_page
                ON invocations(workflow_id, invocation_id);
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
            ALTER TABLE runner_heartbeats ADD COLUMN IF NOT EXISTS can_run_atomic_service BOOLEAN NOT NULL DEFAULT FALSE;

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
            CREATE INDEX IF NOT EXISTS idx_workflow_runs_page
                ON workflow_runs(workflow_type, workflow_id);

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
                runner_language TEXT NOT NULL DEFAULT 'rust',
                executor_kind TEXT NOT NULL DEFAULT 'tokio',
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
            ALTER TABLE runner_contexts ADD COLUMN IF NOT EXISTS runner_language TEXT NOT NULL DEFAULT 'rust';
            ALTER TABLE runner_contexts ADD COLUMN IF NOT EXISTS executor_kind TEXT NOT NULL DEFAULT 'tokio';
            ALTER TABLE invocations ADD COLUMN IF NOT EXISTS traceparent TEXT;
            ALTER TABLE invocations ADD COLUMN IF NOT EXISTS tracestate TEXT;
            ALTER TABLE invocations ADD COLUMN IF NOT EXISTS workflow_parent_id TEXT;
            ALTER TABLE broker_queue ADD COLUMN IF NOT EXISTS reserved_until TIMESTAMPTZ;
            -- Trigger outbox: pre-0.6 runs keep NULL and are never re-published.
            ALTER TABLE trg_trigger_runs ADD COLUMN IF NOT EXISTS planned_invocation_id TEXT;
            CREATE INDEX IF NOT EXISTS idx_trg_runs_pending ON trg_trigger_runs(claimed_at)
                WHERE triggered_invocation_id IS NULL AND planned_invocation_id IS NOT NULL;
            CREATE TABLE IF NOT EXISTS submission_publications (
                invocation_id TEXT PRIMARY KEY, identity_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_profile (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                version INTEGER NOT NULL, lease_ms BIGINT NOT NULL,
                max_queue_rows BIGINT NOT NULL, max_payload_bytes BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_history_runner
                ON history(runner_id);
            -- Marks this DDL version as applied; see SCHEMA_DDL_VERSION.
            CREATE TABLE IF NOT EXISTS rustvello_schema_version (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                version INTEGER NOT NULL
            );
            ",
            )
            .await
            .map_err(pg_err)?;
        client
            .execute(
                "INSERT INTO rustvello_schema_version (version) VALUES ($1)
                 ON CONFLICT (singleton) DO UPDATE SET version = EXCLUDED.version",
                &[&SCHEMA_DDL_VERSION],
            )
            .await?;
        Ok(())
    }
}

/// Version of the DDL batch in `apply_schema_ddl`; bump it whenever that batch changes.
const SCHEMA_DDL_VERSION: i32 = 2;

pub(crate) fn pg_err(e: RustvelloError) -> RustvelloError {
    e
}

pub(crate) fn parse_status(s: &str) -> RustvelloResult<rustvello_proto::status::InvocationStatus> {
    s.parse::<rustvello_proto::status::InvocationStatus>()
        .map_err(RustvelloError::state_backend)
}

#[cfg(all(test, feature = "tls"))]
mod tls_tests {
    use super::*;

    #[test]
    fn private_tls_policy_is_bounded_redacted_and_host_bound() {
        let tls =
            PostgresTlsOptions::private_ca_pem("database.internal", b"SECRET-CA-CONTENT".to_vec())
                .unwrap();
        assert_eq!(tls.expected_hostname(), "database.internal");
        assert!(tls.uses_private_ca());
        assert!(!format!("{tls:?}").contains("SECRET-CA-CONTENT"));

        let matching: tokio_postgres::Config =
            "host=database.internal hostaddr=127.0.0.1 user=test"
                .parse()
                .unwrap();
        tls.validate_hosts(&matching).unwrap();
        let wrong: tokio_postgres::Config = "host=other.internal user=test".parse().unwrap();
        assert!(tls.validate_hosts(&wrong).is_err());
        assert!(PostgresTlsOptions::private_ca_pem("database.internal", Vec::new()).is_err());
        assert!(PostgresTlsOptions::system_roots("bad host").is_err());
    }

    #[test]
    fn runtime_options_keep_complete_tls_parity() {
        let options = PostgresOptions {
            max_pool_size: 7,
            operation_timeout_ms: 321,
            delivery_lease_ms: 654,
            max_queue_rows: 987,
            max_payload_bytes: 12_345,
        };
        options.validate("tls_profile").unwrap();
        assert_eq!(options.max_pool_size, 7);
        assert_eq!(options.operation_timeout_ms, 321);
        assert_eq!(options.delivery_lease_ms, 654);
        assert_eq!(options.max_queue_rows, 987);
        assert_eq!(options.max_payload_bytes, 12_345);
    }
}
