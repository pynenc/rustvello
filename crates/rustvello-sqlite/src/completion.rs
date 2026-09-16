//! Fence native runner payload writes using the SQLite status/owner transaction.

use std::sync::Arc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use crate::db::{blocking, lock_err, parse_status, sql_err, Database};

pub(crate) async fn write_owned<F>(
    db: Arc<Database>,
    invocation_id: InvocationId,
    runner_id: RunnerId,
    terminal: InvocationStatus,
    write: F,
) -> RustvelloResult<()>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<usize> + Send + 'static,
{
    blocking(move || {
        let conn = db.conn.lock().map_err(lock_err)?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)
                .map_err(sql_err)?;
        let (status, owner): (String, Option<String>) = tx
            .query_row(
                "SELECT status, runner_id FROM status_records WHERE invocation_id = ?1",
                [invocation_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sql_err)?;
        let status = parse_status(&status)?;
        if status != InvocationStatus::Running || owner.as_deref() != Some(runner_id.as_str()) {
            return Err(RustvelloError::OwnershipViolation {
                invocation_id,
                from_status: status,
                to_status: terminal,
                current_owner: owner.unwrap_or_default(),
                attempted_owner: runner_id.to_string(),
                reason: "completion payload requires current Running ownership".into(),
            });
        }
        write(&tx).map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    })
    .await
}
