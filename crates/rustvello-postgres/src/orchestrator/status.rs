//! PostgreSQL-backed [`OrchestratorStatus`] implementation.

use async_trait::async_trait;
use chrono::Utc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::OrchestratorStatus;
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::PostgresOrchestrator;
use crate::db::{parse_status, pg_err};

#[async_trait]
impl OrchestratorStatus for PostgresOrchestrator {
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId> {
        let invocation_id = InvocationId::new();
        let now = Utc::now();
        let status_str = InvocationStatus::Registered.to_string();
        let task_id_str = call.task_id.to_string();
        let call_id_str = call.call_id.to_string();

        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        tx.execute(
                "INSERT INTO invocations (invocation_id, task_id, call_id, status, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &invocation_id.as_str(),
                    &task_id_str,
                    &call_id_str,
                    &status_str,
                    &now,
                    &now,
                ],
            )
            .await
            .map_err(pg_err)?;

        tx.execute(
            "INSERT INTO status_records (invocation_id, status, runner_id, timestamp)
                 VALUES ($1, $2, NULL, $3)
                 ON CONFLICT (invocation_id) DO UPDATE SET status = $2, timestamp = $3",
            &[&invocation_id.as_str(), &status_str, &now],
        )
        .await
        .map_err(pg_err)?;

        tx.commit().await.map_err(pg_err)?;

        Ok(invocation_id)
    }

    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let client = self.db.conn().await?;

        let row = client
            .query_opt(
                "SELECT status, runner_id, timestamp FROM status_records WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?
            .ok_or_else(|| RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            })?;

        let status_str: String = row.get(0);
        let runner_id_opt: Option<String> = row.get(1);
        let timestamp: chrono::DateTime<Utc> = row.get(2);

        Ok(InvocationStatusRecord {
            status: parse_status(&status_str)?,
            runner_id: runner_id_opt.map(RunnerId::from_string),
            timestamp,
        })
    }

    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        use rustvello_proto::status::status_record_transition;

        let mut client = self.db.conn().await?;

        // Use a transaction to atomically check-and-set status.
        let tx = client.transaction().await.map_err(pg_err)?;

        let row = tx
            .query_opt(
                "SELECT status, runner_id, timestamp FROM status_records WHERE invocation_id = $1 FOR UPDATE",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?
            .ok_or_else(|| RustvelloError::InvocationNotFound { invocation_id: invocation_id.clone() })?;

        let current_status_str: String = row.get(0);
        let current_runner_id_str: Option<String> = row.get(1);
        let current_ts: chrono::DateTime<Utc> = row.get(2);
        let current_status = parse_status(&current_status_str)?;
        let current_record = InvocationStatusRecord {
            status: current_status,
            runner_id: current_runner_id_str.map(RunnerId::from_string),
            timestamp: current_ts,
        };

        let new_record = status_record_transition(Some(&current_record), status, runner_id)
            .map_err(|e| {
                rustvello_core::error::status_machine_error_to_rustvello(
                    e,
                    invocation_id,
                    current_status,
                )
            })?;

        let status_str = status.to_string();
        let runner_id_str = new_record
            .runner_id
            .as_ref()
            .map(|r| r.as_str().to_string());

        tx.execute(
            "UPDATE status_records SET status = $1, runner_id = $2, timestamp = $3 WHERE invocation_id = $4",
            &[&status_str, &runner_id_str as &(dyn tokio_postgres::types::ToSql + Sync), &new_record.timestamp, &invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;

        tx.execute(
            "UPDATE invocations SET status = $1, updated_at = $2 WHERE invocation_id = $3",
            &[&status_str, &new_record.timestamp, &invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;

        tx.commit().await.map_err(pg_err)?;

        Ok(new_record)
    }

    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let now = Utc::now();
        let status = InvocationStatus::Registered;
        let status_str = status.to_string();
        let task_id_str = call.task_id.to_string();
        let call_id_str = call.call_id.to_string();
        let runner_id_str = runner_id.map(|r| r.as_str().to_string());

        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        tx.execute(
            "INSERT INTO invocations (invocation_id, task_id, call_id, status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (invocation_id) DO NOTHING",
            &[&invocation_id.as_str(), &task_id_str, &call_id_str, &status_str, &now, &now],
        )
        .await
        .map_err(pg_err)?;

        tx.execute(
            "INSERT INTO status_records (invocation_id, status, runner_id, timestamp)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (invocation_id) DO UPDATE SET status = $2, runner_id = $3, timestamp = $4",
            &[
                &invocation_id.as_str(),
                &status_str,
                &runner_id_str as &(dyn tokio_postgres::types::ToSql + Sync),
                &now,
            ],
        )
        .await
        .map_err(pg_err)?;

        tx.commit().await.map_err(pg_err)?;

        Ok(InvocationStatusRecord {
            status,
            runner_id: runner_id.cloned(),
            timestamp: now,
        })
    }

    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32> {
        let client = self.db.conn().await?;
        let row = client
            .query_one(
                "INSERT INTO retries (invocation_id, count) VALUES ($1, 1)
                 ON CONFLICT (invocation_id) DO UPDATE SET count = retries.count + 1
                 RETURNING count",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        let count: i32 = row.get(0);
        Ok(u32::try_from(count).unwrap_or(0))
    }

    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32> {
        let client = self.db.conn().await?;
        let row = client
            .query_opt(
                "SELECT count FROM retries WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map_or(0, |r| u32::try_from(r.get::<_, i32>(0)).unwrap_or(0)))
    }

    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;
        tx.execute(
            "DELETE FROM cc_arg_pairs WHERE invocation_id = $1",
            &[&invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;
        tx.execute(
            "DELETE FROM waiting_for WHERE waiter_id = $1 OR waited_on_id = $1",
            &[&invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;
        tx.execute(
            "DELETE FROM retries WHERE invocation_id = $1",
            &[&invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;
        tx.execute(
            "DELETE FROM status_records WHERE invocation_id = $1",
            &[&invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;
        tx.execute(
            "DELETE FROM invocations WHERE invocation_id = $1",
            &[&invocation_id.as_str()],
        )
        .await
        .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;
        tx.execute("DELETE FROM cc_arg_pairs", &[])
            .await
            .map_err(pg_err)?;
        tx.execute("DELETE FROM waiting_for", &[])
            .await
            .map_err(pg_err)?;
        tx.execute("DELETE FROM retries", &[])
            .await
            .map_err(pg_err)?;
        tx.execute("DELETE FROM runner_heartbeats", &[])
            .await
            .map_err(pg_err)?;
        tx.execute("DELETE FROM status_records", &[])
            .await
            .map_err(pg_err)?;
        tx.execute("DELETE FROM invocations", &[])
            .await
            .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn schedule_auto_purge(&self, _invocation_id: &InvocationId) -> RustvelloResult<()> {
        Err(RustvelloError::NotSupported {
            backend: "Postgres".into(),
            method: "schedule_auto_purge".into(),
        })
    }

    async fn run_auto_purge(&self, _max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>> {
        Err(RustvelloError::NotSupported {
            backend: "Postgres".into(),
            method: "run_auto_purge".into(),
        })
    }
}
