use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustvello_core::error::{RustvelloError, RustvelloResult};

/// Shared SQLite database connection wrapper with schema initialization.
///
/// Data is isolated by `app_id`: for file-based databases the effective
/// path is `{stem}_{app_id}{ext}` (e.g. `data.db` with app_id `"a1"`
/// opens `data_a1.db`). In-memory databases are inherently isolated
/// because each instance gets its own private database.
pub struct Database {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) domain: Arc<str>,
}

/// WAL commit synchronization. FULL syncs each acknowledged commit; NORMAL can
/// lose recent commits on machine power loss. Neither qualifies the filesystem.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqliteSynchronous {
    Normal,
    #[default]
    Full,
}

#[derive(Clone, Debug)]
pub struct SqliteOptions {
    pub synchronous: SqliteSynchronous,
    pub busy_timeout: Duration,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            synchronous: SqliteSynchronous::Full,
            busy_timeout: Duration::from_secs(5),
        }
    }
}

/// Compute the effective file path by injecting `app_id` before the extension.
///
/// `"path/data.db"` + `"my_app"` → `"path/data_my_app.db"`
fn effective_path(base: impl AsRef<Path>, app_id: &str) -> PathBuf {
    let base = base.as_ref();
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let ext = base
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let parent = base.parent().unwrap_or_else(|| Path::new(""));
    parent.join(format!("{stem}_{app_id}{ext}"))
}

impl Database {
    /// Open or create a SQLite database at the given path.
    ///
    /// The `app_id` is embedded in the filename so that different
    /// applications using the same base path get separate database files.
    /// IDs must be 1..=64 bytes from `[a-z0-9_-]`; uppercase and separators
    /// are rejected before opening a file to prevent filesystem aliases.
    pub fn open(path: impl AsRef<Path>, app_id: &str) -> RustvelloResult<Self> {
        Self::open_with_options(path, app_id, SqliteOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        app_id: &str,
        options: SqliteOptions,
    ) -> RustvelloResult<Self> {
        if options.busy_timeout.is_zero() || options.busy_timeout > Duration::from_secs(60) {
            return Err(RustvelloError::Configuration {
                message: "SQLite busy timeout must be in (0, 60s]".into(),
            });
        }
        // Lowercase avoids app aliases on case-insensitive local filesystems.
        if app_id.is_empty()
            || app_id.len() > 64
            || !app_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
        {
            return Err(RustvelloError::Configuration {
                message:
                    "SQLite app_id must contain 1..=64 lowercase ASCII letters, digits, '_' or '-'"
                        .into(),
            });
        }
        let actual = effective_path(path, app_id);
        let conn = Connection::open(&actual).map_err(|e| {
            RustvelloError::state_backend(format!("failed to open SQLite database: {}", e))
        })?;
        let db = Self {
            conn: Mutex::new(conn),
            domain: format!(
                "file:{:?}:{:?}",
                actual
                    .canonicalize()
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?,
                options.synchronous
            )
            .into(),
        };
        db.initialize_schema(&options)?;
        Ok(db)
    }

    /// Create an in-memory SQLite database (for testing).
    ///
    /// In-memory databases are inherently isolated (each `Connection` owns
    /// a private database), so `app_id` is accepted for API consistency
    /// but does not affect behavior.
    pub fn in_memory() -> RustvelloResult<Self> {
        let conn = Connection::open_in_memory().map_err(|e| {
            RustvelloError::state_backend(format!("failed to open in-memory SQLite: {}", e))
        })?;
        let db = Self {
            conn: Mutex::new(conn),
            domain: {
                static NEXT_MEMORY: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                format!(
                    "memory:{}",
                    NEXT_MEMORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                )
                .into()
            },
        };
        db.initialize_schema(&SqliteOptions::default())?;
        Ok(db)
    }

    /// Read back actual connection settings, not merely the requested preset.
    pub fn synchronization(&self) -> RustvelloResult<(String, u32, u32)> {
        let conn = self.conn.lock().map_err(lock_err)?;
        Ok((
            conn.pragma_query_value(None, "journal_mode", |r| r.get(0))
                .map_err(sql_err)?,
            conn.pragma_query_value(None, "synchronous", |r| r.get(0))
                .map_err(sql_err)?,
            conn.pragma_query_value(None, "busy_timeout", |r| r.get(0))
                .map_err(sql_err)?,
        ))
    }

    fn initialize_schema(&self, options: &SqliteOptions) -> RustvelloResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RustvelloError::state_backend(format!("lock poisoned: {}", e)))?;

        // These are connection-local and must precede every schema/data write.
        conn.busy_timeout(options.busy_timeout).map_err(sql_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| {
                RustvelloError::state_backend(format!("PRAGMA journal_mode failed: {}", e))
            })?;
        conn.pragma_update(
            None,
            "synchronous",
            match options.synchronous {
                SqliteSynchronous::Normal => "NORMAL",
                SqliteSynchronous::Full => "FULL",
            },
        )
        .map_err(|e| RustvelloError::state_backend(format!("PRAGMA synchronous failed: {}", e)))?;

        conn.execute_batch(
            "
            -- Broker queue
            CREATE TABLE IF NOT EXISTS broker_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id TEXT NOT NULL,
                task_id TEXT,
                queue_name TEXT NOT NULL,
                priority REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_broker_queue_created
                ON broker_queue(created_at);
            CREATE INDEX IF NOT EXISTS idx_broker_queue_route
                ON broker_queue(queue_name, priority DESC, id ASC);
            CREATE INDEX IF NOT EXISTS idx_broker_queue_task
                ON broker_queue(queue_name, task_id, priority DESC, id ASC);
            CREATE INDEX IF NOT EXISTS idx_broker_queue_invocation
                ON broker_queue(invocation_id);

            -- Dequeue leases retain the queue row until ownership commits.
            CREATE TABLE IF NOT EXISTS broker_reservations (
                queue_id INTEGER PRIMARY KEY,
                expires_at_ms INTEGER NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS broker_queue_delete_reservation
                AFTER DELETE ON broker_queue
                BEGIN
                    DELETE FROM broker_reservations WHERE queue_id = OLD.id;
                END;

            -- Invocations
            CREATE TABLE IF NOT EXISTS invocations (
                invocation_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                call_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                parent_invocation_id TEXT,
                workflow_id TEXT,
                workflow_type TEXT,
                workflow_depth INTEGER,
                workflow_parent_id TEXT,
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
            CREATE TABLE IF NOT EXISTS submission_publications (
                invocation_id TEXT PRIMARY KEY,
                identity_json TEXT NOT NULL
            );

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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id TEXT NOT NULL,
                status TEXT NOT NULL,
                runner_id TEXT,
                timestamp TEXT NOT NULL,
                message TEXT,
                history_timestamp TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_invocation
                ON history(invocation_id);

            -- Status records (current status with runner ownership)
            CREATE TABLE IF NOT EXISTS status_records (
                invocation_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                runner_id TEXT,
                timestamp TEXT NOT NULL
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
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            -- Runner heartbeats
            CREATE TABLE IF NOT EXISTS runner_heartbeats (
                runner_id TEXT PRIMARY KEY,
                creation_time TEXT NOT NULL,
                last_heartbeat TEXT NOT NULL,
                can_run_atomic_service INTEGER NOT NULL DEFAULT 0,
                last_service_start TEXT,
                last_service_end TEXT
            );

            -- Invocation retry counters
            CREATE TABLE IF NOT EXISTS retries (
                invocation_id TEXT PRIMARY KEY,
                retry_count INTEGER NOT NULL DEFAULT 0
            );

            -- Trigger conditions
            CREATE TABLE IF NOT EXISTS trg_conditions (
                condition_id TEXT PRIMARY KEY,
                condition_type TEXT NOT NULL DEFAULT '',
                event_code TEXT,
                condition_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trg_cond_type
                ON trg_conditions(condition_type);
            CREATE INDEX IF NOT EXISTS idx_trg_cond_event
                ON trg_conditions(condition_type, event_code);

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
                last_execution TEXT NOT NULL
            );

            -- Trigger run claims (dedup)
            CREATE TABLE IF NOT EXISTS trg_trigger_run_claims (
                trigger_run_id TEXT PRIMARY KEY,
                claimed_at TEXT NOT NULL
            );

            -- Durable trigger monitoring records
            CREATE TABLE IF NOT EXISTS trg_events (
                event_id TEXT PRIMARY KEY,
                event_code TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                emitted_by_invocation_id TEXT,
                event_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trg_events_code_time
                ON trg_events(event_code, event_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trg_events_emitter
                ON trg_events(emitted_by_invocation_id, event_timestamp DESC);
            CREATE TABLE IF NOT EXISTS trg_trigger_runs (
                trigger_run_id TEXT PRIMARY KEY,
                claimed_at TEXT NOT NULL,
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

            -- App info storage (opaque JSON per app_id)
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
            CREATE INDEX IF NOT EXISTS idx_wf_sub_inv_workflow
                ON workflow_sub_invocations(workflow_id);

            -- Runner execution contexts
            CREATE TABLE IF NOT EXISTS runner_contexts (
                runner_id TEXT PRIMARY KEY,
                runner_cls TEXT NOT NULL,
                runner_language TEXT NOT NULL DEFAULT 'rust',
                executor_kind TEXT NOT NULL DEFAULT 'tokio',
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                thread_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                parent_runner_id TEXT,
                parent_runner_cls TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runner_contexts_parent
                ON runner_contexts(parent_runner_id);

            -- Auto-purge schedule
            CREATE TABLE IF NOT EXISTS auto_purge_schedule (
                invocation_id TEXT PRIMARY KEY,
                scheduled_at TEXT NOT NULL
            );

            -- Index for time-range queries on history (COALESCE for history_timestamp fallback)
            CREATE INDEX IF NOT EXISTS idx_history_timestamp
                ON history(COALESCE(history_timestamp, timestamp));
            ",
        )
        .map_err(|e| RustvelloError::state_backend(format!("schema init failed: {}", e)))?;

        // Migration: add event_code column for existing databases that lack it.
        // ALTER TABLE … ADD COLUMN is a no-op if the column already exists in
        // SQLite (returns error which we ignore).
        let _ = conn.execute_batch("ALTER TABLE trg_conditions ADD COLUMN event_code TEXT");
        let _ = conn.execute_batch(
            "ALTER TABLE runner_contexts ADD COLUMN runner_language TEXT NOT NULL DEFAULT 'rust'",
        );
        let _ = conn.execute_batch(
            "ALTER TABLE runner_contexts ADD COLUMN executor_kind TEXT NOT NULL DEFAULT 'tokio'",
        );
        // Migration: retain task-aware routing for databases created before the
        // queue stored the task ID. Legacy rows remain global and can still be
        // matched through the invocations table.
        let _ = conn.execute_batch("ALTER TABLE broker_queue ADD COLUMN task_id TEXT");
        let _ = conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_broker_queue_task ON broker_queue(task_id, id)",
        );
        let _ = conn.execute_batch("ALTER TABLE invocations ADD COLUMN traceparent TEXT");
        let _ = conn.execute_batch("ALTER TABLE invocations ADD COLUMN tracestate TEXT");
        // Additive: existing invocation/workflow data is never reset.
        let _ = conn.execute_batch("ALTER TABLE invocations ADD COLUMN workflow_parent_id TEXT");

        Ok(())
    }
}

pub(crate) fn lock_err(e: impl std::fmt::Display) -> RustvelloError {
    RustvelloError::state_backend(format!("lock poisoned: {}", e))
}

pub(crate) fn sql_err(e: rusqlite::Error) -> RustvelloError {
    RustvelloError::state_backend(format!("SQLite error: {}", e))
}

/// Run a blocking closure on the tokio blocking thread pool.
///
/// All SQLite operations must go through this helper to avoid blocking the
/// tokio worker threads (see NB_10 Finding 3).
pub(crate) async fn blocking<F, T>(f: F) -> RustvelloResult<T>
where
    F: FnOnce() -> RustvelloResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| RustvelloError::Internal {
            message: format!("spawn_blocking join error: {e}"),
        })?
}

pub(crate) fn parse_status(s: &str) -> RustvelloResult<rustvello_proto::status::InvocationStatus> {
    s.parse::<rustvello_proto::status::InvocationStatus>()
        .map_err(RustvelloError::state_backend)
}

pub(crate) fn parse_timestamp(s: &str) -> RustvelloResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| RustvelloError::state_backend(format!("invalid timestamp in database: {e}")))
}
