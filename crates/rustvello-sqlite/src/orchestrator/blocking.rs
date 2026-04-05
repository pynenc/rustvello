use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorBlocking;
use rustvello_proto::identifiers::InvocationId;

use crate::db::{blocking, lock_err, sql_err};

use super::SqliteOrchestrator;

#[async_trait]
impl OrchestratorBlocking for SqliteOrchestrator {
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let waiter = waiter.clone();
        let waited_on = waited_on.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO waiting_for (waiter_id, waited_on_id) VALUES (?1, ?2)",
                rusqlite::params![waiter.as_str(), waited_on.as_str()],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let waited_on = waited_on.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;

            let mut stmt = conn
                .prepare("SELECT waiter_id FROM waiting_for WHERE waited_on_id = ?1")
                .map_err(sql_err)?;

            let ids: Vec<InvocationId> = stmt
                .query_map([waited_on.as_str()], |row| {
                    let id: String = row.get(0)?;
                    Ok(InvocationId::from_string(id))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            Ok(ids)
        })
        .await
    }

    async fn release_waiters(
        &self,
        completed: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let completed = completed.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            let mut stmt = tx
                .prepare("SELECT waiter_id FROM waiting_for WHERE waited_on_id = ?1")
                .map_err(sql_err)?;

            let waiters: Vec<InvocationId> = stmt
                .query_map([completed.as_str()], |row| {
                    let id: String = row.get(0)?;
                    Ok(InvocationId::from_string(id))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            drop(stmt);
            tx.execute(
                "DELETE FROM waiting_for WHERE waited_on_id = ?1",
                [completed.as_str()],
            )
            .map_err(sql_err)?;

            tx.commit().map_err(sql_err)?;
            Ok(waiters)
        })
        .await
    }
}
