//! Atomic task publication in one PostgreSQL transaction domain.

use crate::{
    bounded::Transaction,
    db::{parse_status, Database},
    failpoints::boundary,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustvello_core::{
    error::{status_machine_error_to_rustvello, RustvelloError, RustvelloResult},
    publication::{
        PublicationChange, PublicationDomain, PublicationRoute, RuntimePublication,
        SubmissionPublication,
    },
};
use rustvello_proto::{
    identifiers::{InvocationId, RunnerId},
    status::{status_record_transition, InvocationStatus, InvocationStatusRecord},
};
use std::sync::Arc;

pub(crate) struct PostgresPublication {
    pub(crate) db: Arc<Database>,
}

fn invalid(message: &str) -> RustvelloError {
    RustvelloError::Configuration {
        message: message.into(),
    }
}

pub(crate) async fn publish(
    tx: &Transaction<'_>,
    id: &str,
    task: Option<&str>,
    route: &PublicationRoute,
    max_queue: u32,
) -> RustvelloResult<()> {
    rustvello_core::broker::validate_routing(&route.queue, route.priority)?;
    if route.queue.len() > 256 {
        return Err(invalid("queue name exceeds 256 bytes"));
    }
    // Serializes admission counts, not execution. No external I/O under this lock.
    tx.query_one("SELECT singleton FROM runtime_profile FOR UPDATE", &[])
        .await?;
    tx.execute("DELETE FROM broker_queue WHERE invocation_id=$1", &[&id])
        .await?;
    let count: i64 = tx
        .query_one("SELECT COUNT(*) FROM broker_queue", &[])
        .await?
        .get(0);
    if count >= i64::from(max_queue) {
        return Err(invalid("Postgres queue admission capacity reached"));
    }
    tx.execute(
        "INSERT INTO broker_queue (invocation_id,task_id,queue_name,priority) VALUES ($1,$2,$3,$4)",
        &[&id, &task, &route.queue, &route.priority],
    )
    .await?;
    Ok(())
}

async fn now(tx: &Transaction<'_>) -> RustvelloResult<DateTime<Utc>> {
    Ok(tx.query_one("SELECT clock_timestamp()", &[]).await?.get(0))
}

pub(crate) async fn current(
    tx: &Transaction<'_>,
    id: &InvocationId,
) -> RustvelloResult<InvocationStatusRecord> {
    let row = tx.query_opt("SELECT status,runner_id,timestamp FROM status_records WHERE invocation_id=$1 FOR UPDATE", &[&id.as_str()]).await?
        .ok_or_else(|| RustvelloError::InvocationNotFound { invocation_id: id.clone() })?;
    Ok(InvocationStatusRecord {
        status: parse_status(row.get::<_, &str>(0))?,
        runner_id: row.get::<_, Option<String>>(1).map(RunnerId::from_string),
        timestamp: row.get(2),
    })
}

async fn history(
    tx: &Transaction<'_>,
    id: &str,
    record: &InvocationStatusRecord,
    runner: &RunnerId,
) -> RustvelloResult<()> {
    tx.execute("INSERT INTO history (invocation_id,status,runner_id,timestamp,history_timestamp) VALUES ($1,$2,$3,$4,$4)",
        &[&id,&record.status.to_string(),&runner.as_str(),&record.timestamp]).await?;
    Ok(())
}

async fn transition(
    tx: &Transaction<'_>,
    id: &InvocationId,
    runner: &RunnerId,
    old: &InvocationStatusRecord,
    status: InvocationStatus,
) -> RustvelloResult<InvocationStatusRecord> {
    let mut record = status_record_transition(Some(old), status, Some(runner))
        .map_err(|e| status_machine_error_to_rustvello(e, id, old.status))?;
    record.timestamp = now(tx).await?;
    tx.execute(
        "UPDATE status_records SET status=$1,runner_id=$2,timestamp=$3 WHERE invocation_id=$4",
        &[
            &status.to_string(),
            &record.runner_id.as_ref().map(RunnerId::as_str),
            &record.timestamp,
            &id.as_str(),
        ],
    )
    .await?;
    tx.execute(
        "UPDATE invocations SET status=$1,updated_at=$2 WHERE invocation_id=$3",
        &[&status.to_string(), &record.timestamp, &id.as_str()],
    )
    .await?;
    history(tx, id.as_str(), &record, runner).await?;
    if status == InvocationStatus::Pending || status.is_terminal() {
        tx.execute(
            "DELETE FROM broker_queue WHERE invocation_id=$1",
            &[&id.as_str()],
        )
        .await?;
    }
    Ok(record)
}

#[async_trait]
impl RuntimePublication for PostgresPublication {
    fn domain(&self) -> PublicationDomain {
        Arc::clone(&self.db.domain)
    }

    async fn begin_execution(
        &self,
        id: &InvocationId,
        runner: &RunnerId,
        retries: u32,
        incoming: &rustvello_proto::invocation::TraceContextCarrier,
    ) -> RustvelloResult<rustvello_proto::invocation::ExecutionAttemptIdentity> {
        use rustvello_core::execution::{next_execution_identity, IDENTITY_KEY};
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await?;
        let old = current(&tx, id).await?;
        if old.status != InvocationStatus::Running || old.runner_id.as_ref() != Some(runner) {
            return Err(invalid(
                "execution identity requires current Running ownership",
            ));
        }
        let encoded = tx
            .query_opt(
                "SELECT data_value FROM workflow_data WHERE workflow_id=$1 AND data_key=$2",
                &[&id.as_str(), &IDENTITY_KEY],
            )
            .await?;
        let previous = encoded
            .map(|row| {
                serde_json::from_str::<rustvello_proto::invocation::ExecutionAttemptIdentity>(
                    row.get::<_, &str>(0),
                )
            })
            .transpose()
            .map_err(|_| invalid("invalid persisted execution identity"))?;
        let identity = next_execution_identity(previous, retries, incoming)?;
        let value =
            serde_json::to_string(&identity).map_err(|_| invalid("invalid execution identity"))?;
        tx.execute("INSERT INTO workflow_data (workflow_id,data_key,data_value) VALUES ($1,$2,$3) ON CONFLICT (workflow_id,data_key) DO UPDATE SET data_value=$3",
            &[&id.as_str(),&IDENTITY_KEY,&value]).await?;
        boundary("execution.before_commit", id.as_str()).await?;
        tx.commit().await?;
        boundary("execution.after_commit", id.as_str()).await?;
        Ok(identity)
    }

    async fn submit(&self, s: SubmissionPublication) -> RustvelloResult<bool> {
        let inv = &s.invocation;
        if inv.task_id != s.call.task_id || inv.call_id != s.call.call_id {
            return Err(invalid("invocation and call identities differ"));
        }
        let identity = serde_json::to_string(&serde_json::json!({
            "call": s.call, "parent": inv.parent_invocation_id, "workflow": inv.workflow,
            "trace": inv.trace_context, "queue": s.route.queue, "priority": s.route.priority,
            "workflow_root": s.workflow_root, "cc": s.cc_arguments,
        }))
        .map_err(|_| invalid("invalid submission"))?;
        if identity.len() > self.db.options.max_payload_bytes as usize {
            return Err(invalid("submission exceeds payload limit"));
        }
        let id = inv.invocation_id.as_str();
        boundary("submit.before_begin", id).await?;
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await?;
        // A row cannot be locked before first insert. This also fences concurrent replay.
        tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':submission:' || $1, 0))", &[&id]).await?;
        if let Some(row) = tx
            .query_opt(
                "SELECT identity_json FROM submission_publications WHERE invocation_id=$1",
                &[&id],
            )
            .await?
        {
            let existing: &str = row.get(0);
            if existing.is_empty() {
                return Err(invalid("submission ID was removed; use a fresh ID"));
            }
            if existing != identity {
                return Err(invalid(
                    "submission ID already accepted with different content or lineage",
                ));
            }
            return Ok(false);
        }
        let occupied: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM invocations WHERE invocation_id=$1 UNION ALL SELECT 1 FROM history WHERE invocation_id=$1 UNION ALL SELECT 1 FROM results WHERE invocation_id=$1 UNION ALL SELECT 1 FROM errors WHERE invocation_id=$1 UNION ALL SELECT 1 FROM workflow_data WHERE workflow_id=$1 UNION ALL SELECT 1 FROM workflow_runs WHERE workflow_id=$1)", &[&id]).await?.get(0);
        if occupied {
            return Err(invalid(
                "submission ID belongs to a legacy invocation; use a fresh ID",
            ));
        }
        let timestamp = now(&tx).await?;
        let wf = inv.workflow.as_ref();
        tx.execute("INSERT INTO invocations (invocation_id,task_id,call_id,status,created_at,updated_at,parent_invocation_id,workflow_id,workflow_type,workflow_depth,traceparent,tracestate,workflow_parent_id) VALUES ($1,$2,$3,'REGISTERED',$4,$4,$5,$6,$7,$8,$9,$10,$11)",
            &[&id,&inv.task_id.to_string(),&inv.call_id.to_string(),&timestamp,
                &inv.parent_invocation_id.as_ref().map(InvocationId::as_str),&wf.map(|w| w.workflow_id.as_str()),&wf.map(|w| w.workflow_type.to_string()),&wf.map(|w| w.depth as i32),
                &inv.trace_context.traceparent,&inv.trace_context.tracestate,&wf.and_then(|w| w.parent_id.as_ref()).map(InvocationId::as_str)]).await?;
        tx.execute("INSERT INTO status_records (invocation_id,status,runner_id,timestamp) VALUES ($1,'REGISTERED',$2,$3)", &[&id,&s.runner_id.as_str(),&timestamp]).await?;
        boundary("submit.control", id).await?;
        let args = serde_json::to_string(&s.call.serialized_arguments.0)
            .map_err(|_| invalid("invalid arguments"))?;
        tx.execute("INSERT INTO calls (call_id,task_id,serialized_arguments) VALUES ($1,$2,$3) ON CONFLICT (call_id) DO NOTHING", &[&s.call.call_id.to_string(),&s.call.task_id.to_string(),&args]).await?;
        boundary("submit.call", id).await?;
        if s.workflow_root {
            if let Some(w) = wf {
                tx.execute("INSERT INTO workflow_runs (workflow_id,workflow_type,parent_workflow_id,depth) VALUES ($1,$2,$3,$4)", &[&w.workflow_id.as_str(),&w.workflow_type.to_string(),&w.parent_id.as_ref().map(InvocationId::as_str),&(w.depth as i32)]).await?;
            }
        }
        boundary("submit.workflow", id).await?;
        if let Some(c) = &s.runner_context {
            tx.execute("INSERT INTO runner_contexts (runner_id,runner_cls,runner_language,executor_kind,pid,hostname,thread_id,started_at,parent_runner_id,parent_runner_cls) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (runner_id) DO NOTHING",
                &[&c.runner_id,&c.runner_cls,&c.runner_language.to_string(),&c.executor_kind.to_string(),&(c.pid as i32),&c.hostname,&(c.thread_id as i64),&c.started_at,&c.parent_runner_id,&c.parent_runner_cls]).await?;
        }
        history(
            &tx,
            id,
            &InvocationStatusRecord {
                status: InvocationStatus::Registered,
                runner_id: Some(s.runner_id.clone()),
                timestamp,
            },
            &s.runner_id,
        )
        .await?;
        boundary("submit.history", id).await?;
        if let Some(args) = &s.cc_arguments {
            for (key, value) in args.cc_arg_pairs() {
                tx.execute("INSERT INTO cc_arg_pairs (invocation_id,task_id,arg_key,arg_value) VALUES ($1,$2,$3,$4)", &[&id,&inv.task_id.to_string(),&key,&value]).await?;
            }
        }
        publish(
            &tx,
            id,
            Some(&inv.task_id.to_string()),
            &s.route,
            self.db.options.max_queue_rows,
        )
        .await?;
        boundary("submit.queue", id).await?;
        tx.execute(
            "INSERT INTO submission_publications (invocation_id,identity_json) VALUES ($1,$2)",
            &[&id, &identity],
        )
        .await?;
        boundary("submit.before_commit", id).await?;
        tx.commit().await?;
        boundary("submit.after_commit", id).await?;
        Ok(true)
    }

    async fn change(
        &self,
        id: &InvocationId,
        runner: &RunnerId,
        change: PublicationChange,
        auto_purge: bool,
    ) -> RustvelloResult<Option<InvocationStatusRecord>> {
        let operation = match &change {
            PublicationChange::Recover { .. } => "recover".into(),
            PublicationChange::Retry(_) => "retry".into(),
            PublicationChange::Reroute(_) | PublicationChange::ConcurrencyReroute(_) => {
                "reroute".into()
            }
            PublicationChange::Success(_) | PublicationChange::Failure(_) => "complete".into(),
            PublicationChange::Status(status) => format!("status.{status}"),
        };
        boundary(&format!("{operation}.before_begin"), id.as_str()).await?;
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await?;
        let mut old = current(&tx, id).await?;
        let route = match &change {
            PublicationChange::Recover {
                status,
                stale_after_seconds,
                route,
            } => {
                let expected = match status {
                    InvocationStatus::PendingRecovery => InvocationStatus::Pending,
                    InvocationStatus::RunningRecovery => InvocationStatus::Running,
                    _ => return Err(invalid("invalid recovery status")),
                };
                if old.status != expected {
                    return Ok(None);
                }
                let cutoff = now(&tx).await?
                    - chrono::Duration::seconds((*stale_after_seconds).min(31_536_000) as i64);
                let heartbeat = tx
                    .query_opt(
                        "SELECT last_heartbeat FROM runner_heartbeats WHERE runner_id=$1",
                        &[&old.runner_id.as_ref().map(RunnerId::as_str)],
                    )
                    .await?;
                let last_seen = if expected == InvocationStatus::Pending {
                    Some(old.timestamp)
                } else {
                    heartbeat.map(|row| row.get::<_, DateTime<Utc>>(0))
                };
                if last_seen.is_some_and(|t| t >= cutoff) {
                    return Ok(None);
                }
                old = transition(&tx, id, runner, &old, *status).await?;
                boundary("recover.recovery_status", id.as_str()).await?;
                Some(route)
            }
            PublicationChange::ConcurrencyReroute(route) => {
                old = transition(
                    &tx,
                    id,
                    runner,
                    &old,
                    InvocationStatus::ConcurrencyControlled,
                )
                .await?;
                boundary("reroute.concurrency_status", id.as_str()).await?;
                Some(route)
            }
            PublicationChange::Retry(route) | PublicationChange::Reroute(route) => Some(route),
            _ => None,
        };
        let status = match &change {
            PublicationChange::Status(s) => *s,
            PublicationChange::Retry(_) => InvocationStatus::Retry,
            PublicationChange::Success(_) => InvocationStatus::Success,
            PublicationChange::Failure(_) => InvocationStatus::Failed,
            _ => InvocationStatus::Rerouted,
        };
        if matches!(
            change,
            PublicationChange::Success(_) | PublicationChange::Failure(_)
        ) && (old.status != InvocationStatus::Running || old.runner_id.as_ref() != Some(runner))
        {
            return Err(RustvelloError::OwnershipViolation {
                invocation_id: id.clone(),
                from_status: old.status,
                to_status: status,
                current_owner: old
                    .runner_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                attempted_owner: runner.to_string(),
                reason: "completion requires current Running ownership".into(),
            });
        }
        let record = transition(&tx, id, runner, &old, status).await?;
        boundary(&format!("{operation}.status_history"), id.as_str()).await?;
        match &change {
            PublicationChange::Retry(_) => {
                tx.execute("INSERT INTO retries (invocation_id,count) VALUES ($1,1) ON CONFLICT (invocation_id) DO UPDATE SET count=retries.count+1", &[&id.as_str()]).await?;
                boundary("retry.counter", id.as_str()).await?;
            }
            PublicationChange::Success(result) => {
                if result.len() > self.db.options.max_payload_bytes as usize {
                    return Err(invalid("result exceeds payload limit"));
                }
                tx.execute("INSERT INTO results (invocation_id,result) VALUES ($1,$2) ON CONFLICT (invocation_id) DO UPDATE SET result=$2", &[&id.as_str(),result]).await?;
                boundary("complete.payload", id.as_str()).await?;
            }
            PublicationChange::Failure(error) => {
                if error.error_type.len()
                    + error.message.len()
                    + error.traceback.as_ref().map_or(0, String::len)
                    > self.db.options.max_payload_bytes as usize
                {
                    return Err(invalid("error exceeds payload limit"));
                }
                tx.execute("INSERT INTO errors (invocation_id,error_type,message,traceback) VALUES ($1,$2,$3,$4) ON CONFLICT (invocation_id) DO UPDATE SET error_type=$2,message=$3,traceback=$4", &[&id.as_str(),&error.error_type,&error.message,&error.traceback]).await?;
                boundary("complete.payload", id.as_str()).await?;
            }
            _ => {}
        }
        if let Some(route) = route {
            let task: String = tx
                .query_one(
                    "SELECT task_id FROM invocations WHERE invocation_id=$1",
                    &[&id.as_str()],
                )
                .await?
                .get(0);
            publish(
                &tx,
                id.as_str(),
                Some(&task),
                route,
                self.db.options.max_queue_rows,
            )
            .await?;
            tx.execute(
                "DELETE FROM cc_arg_pairs WHERE invocation_id=$1",
                &[&id.as_str()],
            )
            .await?;
            boundary(&format!("{operation}.queue"), id.as_str()).await?;
        }
        if status.is_terminal() {
            tx.execute(
                "DELETE FROM waiting_for WHERE waited_on_id=$1",
                &[&id.as_str()],
            )
            .await?;
            tx.execute(
                "DELETE FROM cc_arg_pairs WHERE invocation_id=$1",
                &[&id.as_str()],
            )
            .await?;
            if auto_purge {
                tx.execute("INSERT INTO auto_purge_schedule (invocation_id,scheduled_at) VALUES ($1,$2) ON CONFLICT (invocation_id) DO UPDATE SET scheduled_at=$2", &[&id.as_str(),&record.timestamp]).await?;
            }
            boundary("complete.terminal_effects", id.as_str()).await?;
        }
        boundary(&format!("{operation}.before_commit"), id.as_str()).await?;
        tx.commit().await?;
        boundary(&format!("{operation}.after_commit"), id.as_str()).await?;
        Ok(Some(record))
    }
}
