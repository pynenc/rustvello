mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::db::Database;

/// SQLite-backed orchestrator implementation.
pub struct SqliteOrchestrator {
    db: Arc<Database>,
}

impl SqliteOrchestrator {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}
