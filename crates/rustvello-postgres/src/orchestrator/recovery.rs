use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{
    ActiveRunnerInfo, AtomicServiceExecution, OrchestratorRecovery,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId};

use super::PostgresOrchestrator;
use crate::db::pg_err;

#[async_trait]
impl OrchestratorRecovery for PostgresOrchestrator {
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        _can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        let now = Utc::now();

        client
            .execute(
                "INSERT INTO runner_heartbeats (runner_id, last_heartbeat) VALUES ($1, $2)
                 ON CONFLICT (runner_id) DO UPDATE SET last_heartbeat = $2",
                &[&runner_id.as_str(), &now],
            )
            .await
            .map_err(pg_err)?;

        Ok(())
    }

    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(max_pending_seconds).unwrap_or(i64::MAX));

        let rows = client
            .query(
                "SELECT invocation_id FROM status_records
                 WHERE status = 'PENDING' AND timestamp < $1",
                &[&threshold],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let threshold = Utc::now()
            - chrono::Duration::seconds(
                i64::try_from(runner_dead_after_seconds).unwrap_or(i64::MAX),
            );

        let rows = client
            .query(
                "SELECT sr.invocation_id FROM status_records sr
                 LEFT JOIN runner_heartbeats rh ON sr.runner_id = rh.runner_id
                 WHERE sr.status = 'RUNNING'
                   AND (rh.last_heartbeat IS NULL OR rh.last_heartbeat < $1)",
                &[&threshold],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>> {
        let client = self.db.conn().await?;
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let rows = client
            .query(
                "SELECT runner_id FROM runner_heartbeats WHERE last_heartbeat >= $1",
                &[&threshold],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| RunnerId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        _can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>> {
        let client = self.db.conn().await?;
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let rows = client
            .query(
                "SELECT runner_id, last_heartbeat FROM runner_heartbeats WHERE last_heartbeat >= $1",
                &[&threshold],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let ts: DateTime<Utc> = r.get(1);
                ActiveRunnerInfo {
                    runner_id: RunnerId::from_string(r.get::<_, String>(0)),
                    creation_time: ts,
                    last_heartbeat: ts,
                    can_run_atomic_service: true,
                    last_service_start: None,
                    last_service_end: None,
                }
            })
            .collect())
    }

    async fn record_atomic_service_execution(
        &self,
        _runner_id: &RunnerId,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> RustvelloResult<()> {
        Ok(())
    }

    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>> {
        Ok(Vec::new())
    }
}
