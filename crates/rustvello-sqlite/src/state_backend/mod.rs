mod core;
mod query;
mod runner;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::db::Database;

/// SQLite-backed state backend implementation.
pub struct SqliteStateBackend {
    db: Arc<Database>,
}

impl SqliteStateBackend {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}
