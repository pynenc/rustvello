mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;

use std::sync::Arc;

use crate::db::Database;

/// PostgreSQL-backed orchestrator implementation.
pub struct PostgresOrchestrator {
    pub(crate) db: Arc<Database>,
}

impl PostgresOrchestrator {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}
