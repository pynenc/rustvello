//! Same-database publication transactions. No awaits or external I/O inside a transaction.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::{params, OptionalExtension, Transaction};
use rustvello_core::error::{status_machine_error_to_rustvello, RustvelloError, RustvelloResult};
use rustvello_core::publication::{
    PublicationChange, PublicationDomain, PublicationRoute, RuntimePublication,
    SubmissionPublication,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{status_record_transition, InvocationStatus, InvocationStatusRecord};

use crate::db::{blocking, lock_err, parse_status, parse_timestamp, sql_err, Database};
use crate::failpoints::boundary;

pub(crate) struct SqlitePublication {
    db: Arc<Database>,
}

impl SqlitePublication {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn configuration(message: &str) -> RustvelloError {
    RustvelloError::Configuration {
        message: message.into(),
    }
}

fn validate_route(route: &PublicationRoute) -> RustvelloResult<()> {
    if route.queue.is_empty() || !route.priority.is_finite() {
        return Err(configuration(
            "publication requires a nonempty queue and finite priority",
        ));
    }
    Ok(())
}

fn publish(
    tx: &Transaction<'_>,
    id: &str,
    task: &str,
    route: &PublicationRoute,
) -> RustvelloResult<()> {
    validate_route(route)?;
    // Replace any old reservation/delivery without changing the invocation identity.
    tx.execute("DELETE FROM broker_queue WHERE invocation_id = ?1", [id])
        .map_err(sql_err)?;
    tx.execute("INSERT INTO broker_queue (invocation_id, task_id, queue_name, priority) VALUES (?1, ?2, ?3, ?4)",
        params![id, task, route.queue, route.priority]).map_err(sql_err)?;
    Ok(())
}

fn history(
    tx: &Transaction<'_>,
    id: &str,
    record: &InvocationStatusRecord,
    runner: &RunnerId,
) -> RustvelloResult<()> {
    tx.execute("INSERT INTO history (invocation_id, status, runner_id, timestamp, history_timestamp) VALUES (?1, ?2, ?3, ?4, ?4)",
        params![id, record.status.to_string(), runner.as_str(), record.timestamp.to_rfc3339()]).map_err(sql_err)?;
    Ok(())
}

fn current(tx: &Transaction<'_>, id: &InvocationId) -> RustvelloResult<InvocationStatusRecord> {
    let row: Option<(String, Option<String>, String)> = tx
        .query_row(
            "SELECT status, runner_id, timestamp FROM status_records WHERE invocation_id = ?1",
            [id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql_err)?;
    let (status, runner, timestamp) = row.ok_or_else(|| RustvelloError::InvocationNotFound {
        invocation_id: id.clone(),
    })?;
    Ok(InvocationStatusRecord {
        status: parse_status(&status)?,
        runner_id: runner.map(RunnerId::from_string),
        timestamp: parse_timestamp(&timestamp)?,
    })
}

fn transition(
    tx: &Transaction<'_>,
    id: &InvocationId,
    runner: &RunnerId,
    old: &InvocationStatusRecord,
    status: InvocationStatus,
) -> RustvelloResult<InvocationStatusRecord> {
    let record = status_record_transition(Some(old), status, Some(runner))
        .map_err(|e| status_machine_error_to_rustvello(e, id, old.status))?;
    tx.execute(
        "UPDATE status_records SET status=?1, runner_id=?2, timestamp=?3 WHERE invocation_id=?4",
        params![
            status.to_string(),
            record.runner_id.as_ref().map(RunnerId::as_str),
            record.timestamp.to_rfc3339(),
            id.as_str()
        ],
    )
    .map_err(sql_err)?;
    tx.execute(
        "UPDATE invocations SET status=?1, updated_at=?2 WHERE invocation_id=?3",
        params![
            status.to_string(),
            record.timestamp.to_rfc3339(),
            id.as_str()
        ],
    )
    .map_err(sql_err)?;
    history(tx, id.as_str(), &record, runner)?;
    if status == InvocationStatus::Pending || status.is_terminal() {
        tx.execute(
            "DELETE FROM broker_queue WHERE invocation_id=?1",
            [id.as_str()],
        )
        .map_err(sql_err)?;
    }
    Ok(record)
}

#[async_trait]
impl RuntimePublication for SqlitePublication {
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
        let db = Arc::clone(&self.db);
        let (id, runner, incoming) = (id.clone(), runner.clone(), incoming.clone());
        blocking(move || {
            use rustvello_core::execution::{next_execution_identity, IDENTITY_KEY};
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate).map_err(sql_err)?;
            let old = current(&tx, &id)?;
            if old.status != InvocationStatus::Running || old.runner_id.as_ref() != Some(&runner) {
                return Err(configuration("execution identity requires current Running ownership"));
            }
            let encoded: Option<String> = tx.query_row("SELECT data_value FROM workflow_data WHERE workflow_id=?1 AND data_key=?2", params![id.as_str(),IDENTITY_KEY], |r| r.get(0)).optional().map_err(sql_err)?;
            let previous = encoded.map(|s| serde_json::from_str(&s)).transpose().map_err(|e| configuration(&e.to_string()))?;
            let identity = next_execution_identity(previous, retries, &incoming)?;
            let value = serde_json::to_string(&identity).map_err(|e| configuration(&e.to_string()))?;
            tx.execute("INSERT OR REPLACE INTO workflow_data (workflow_id,data_key,data_value) VALUES (?1,?2,?3)", params![id.as_str(),IDENTITY_KEY,value]).map_err(sql_err)?;
            boundary("execution.before_commit", id.as_str())?;
            tx.commit().map_err(sql_err)?;
            boundary("execution.after_commit", id.as_str())?;
            Ok(identity)
        }).await
    }

    async fn submit(&self, s: SubmissionPublication) -> RustvelloResult<bool> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            validate_route(&s.route)?;
            let inv = &s.invocation;
            if inv.task_id != s.call.task_id || inv.call_id != s.call.call_id {
                return Err(configuration("invocation and call identities differ"));
            }
            let identity = serde_json::to_string(&serde_json::json!({
                "call": s.call, "parent": inv.parent_invocation_id, "workflow": inv.workflow,
                "trace": inv.trace_context, "queue": s.route.queue, "priority": s.route.priority,
                "workflow_root": s.workflow_root, "cc": s.cc_arguments,
            })).map_err(|e| configuration(&e.to_string()))?;
            let id = inv.invocation_id.as_str();
            boundary("submit.before_begin", id)?;
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate).map_err(sql_err)?;
            let existing: Option<String> = tx.query_row("SELECT identity_json FROM submission_publications WHERE invocation_id=?1", [id], |r| r.get(0)).optional().map_err(sql_err)?;
            if let Some(existing) = existing {
                if existing.is_empty() { return Err(configuration("submission ID was removed; use a fresh ID")); }
                if existing != identity { return Err(configuration("submission ID already accepted with different content or lineage")); }
                return Ok(false);
            }
            let occupied: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM invocations WHERE invocation_id=?1 UNION ALL SELECT 1 FROM history WHERE invocation_id=?1 UNION ALL SELECT 1 FROM results WHERE invocation_id=?1 UNION ALL SELECT 1 FROM errors WHERE invocation_id=?1 UNION ALL SELECT 1 FROM workflow_data WHERE workflow_id=?1 UNION ALL SELECT 1 FROM workflow_runs WHERE workflow_id=?1)", [id], |r| r.get(0)).map_err(sql_err)?;
            if occupied { return Err(configuration("submission ID belongs to a legacy invocation; use a fresh ID")); }
            let now = Utc::now();
            let timestamp = now.to_rfc3339();
            let wf = inv.workflow.as_ref();
            tx.execute("INSERT INTO invocations (invocation_id,task_id,call_id,status,created_at,updated_at,parent_invocation_id,workflow_id,workflow_type,workflow_depth,traceparent,tracestate) VALUES (?1,?2,?3,'REGISTERED',?4,?4,?5,?6,?7,?8,?9,?10)",
                params![id, inv.task_id.to_string(), inv.call_id.to_string(), timestamp,
                    inv.parent_invocation_id.as_ref().map(InvocationId::as_str), wf.map(|w| w.workflow_id.as_str()), wf.map(|w| w.workflow_type.to_string()), wf.map(|w| w.depth), inv.trace_context.traceparent, inv.trace_context.tracestate]).map_err(sql_err)?;
            tx.execute("INSERT INTO status_records (invocation_id,status,runner_id,timestamp) VALUES (?1,'REGISTERED',?2,?3)", params![id,s.runner_id.as_str(),timestamp]).map_err(sql_err)?;
            boundary("submit.control", id)?;
            tx.execute("UPDATE invocations SET workflow_parent_id=?1 WHERE invocation_id=?2", params![wf.and_then(|w| w.parent_id.as_ref()).map(InvocationId::as_str),id]).map_err(sql_err)?;
            let args = serde_json::to_string(&s.call.serialized_arguments.0).map_err(|e| configuration(&e.to_string()))?;
            tx.execute("INSERT INTO calls (call_id,task_id,serialized_arguments) VALUES (?1,?2,?3) ON CONFLICT(call_id) DO NOTHING", params![s.call.call_id.to_string(),s.call.task_id.to_string(),args]).map_err(sql_err)?;
            boundary("submit.call", id)?;
            if s.workflow_root {
                if let Some(w) = wf {
                    tx.execute("INSERT INTO workflow_runs (workflow_id,workflow_type,parent_workflow_id,depth) VALUES (?1,?2,?3,?4)", params![w.workflow_id.as_str(),w.workflow_type.to_string(),w.parent_id.as_ref().map(InvocationId::as_str),w.depth]).map_err(sql_err)?;
                }
            }
            boundary("submit.workflow", id)?;
            if let Some(c) = &s.runner_context {
                tx.execute("INSERT OR IGNORE INTO runner_contexts (runner_id,runner_cls,runner_language,executor_kind,pid,hostname,thread_id,started_at,parent_runner_id,parent_runner_cls) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)", params![c.runner_id,c.runner_cls,c.runner_language.to_string(),c.executor_kind.to_string(),c.pid,c.hostname,c.thread_id,c.started_at.to_rfc3339(),c.parent_runner_id,c.parent_runner_cls]).map_err(sql_err)?;
            }
            history(&tx, id, &InvocationStatusRecord { status: InvocationStatus::Registered, runner_id: Some(s.runner_id.clone()), timestamp: now }, &s.runner_id)?;
            boundary("submit.history", id)?;
            if let Some(args) = &s.cc_arguments {
                for (key,value) in args.cc_arg_pairs() {
                    tx.execute("INSERT INTO cc_arg_pairs (invocation_id,task_id,arg_key,arg_value) VALUES (?1,?2,?3,?4)", params![id,inv.task_id.to_string(),key,value]).map_err(sql_err)?;
                }
            }
            publish(&tx, id, &inv.task_id.to_string(), &s.route)?;
            boundary("submit.queue", id)?;
            tx.execute("INSERT INTO submission_publications (invocation_id,identity_json) VALUES (?1,?2)", params![id,identity]).map_err(sql_err)?;
            boundary("submit.before_commit", id)?;
            tx.commit().map_err(sql_err)?;
            boundary("submit.after_commit", id)?;
            Ok(true)
        }).await
    }

    async fn change(
        &self,
        id: &InvocationId,
        runner: &RunnerId,
        change: PublicationChange,
        auto_purge: bool,
    ) -> RustvelloResult<Option<InvocationStatusRecord>> {
        let db = Arc::clone(&self.db);
        let id = id.clone();
        let runner = runner.clone();
        blocking(move || {
            let status_operation;
            let operation = match &change {
                PublicationChange::Recover { .. } => "recover",
                PublicationChange::Retry(_) => "retry",
                PublicationChange::Reroute(_) | PublicationChange::ConcurrencyReroute(_) => "reroute",
                PublicationChange::Success(_) | PublicationChange::Failure(_) => "complete",
                PublicationChange::Status(status) => {
                    status_operation = format!("status.{status}");
                    &status_operation
                },
            };
            let point = |stage: &str| boundary(&format!("{operation}.{stage}"), id.as_str());
            point("before_begin")?;
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate).map_err(sql_err)?;
            let mut old = current(&tx, &id)?;
            let route = match &change {
                PublicationChange::Recover { status, stale_after_seconds, route } => {
                    let expected = match status {
                        InvocationStatus::PendingRecovery => InvocationStatus::Pending,
                        InvocationStatus::RunningRecovery => InvocationStatus::Running,
                        _ => return Err(configuration("invalid recovery status")),
                    };
                    if old.status != expected { return Ok(None); }
                    let cutoff = Utc::now() - chrono::Duration::seconds((*stale_after_seconds).min(i64::MAX as u64) as i64);
                    let heartbeat: Option<String> = tx.query_row("SELECT last_heartbeat FROM runner_heartbeats WHERE runner_id=?1", [old.runner_id.as_ref().map(RunnerId::as_str)], |r| r.get(0)).optional().map_err(sql_err)?;
                    let last_seen = if expected == InvocationStatus::Pending { Some(old.timestamp) } else { heartbeat.as_deref().map(parse_timestamp).transpose()? };
                    if last_seen.is_some_and(|t| t >= cutoff) { return Ok(None); }
                    old = transition(&tx, &id, &runner, &old, *status)?;
                    point("recovery_status")?;
                    Some(route)
                }
                PublicationChange::ConcurrencyReroute(route) => {
                    old = transition(&tx, &id, &runner, &old, InvocationStatus::ConcurrencyControlled)?;
                    point("concurrency_status")?;
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
            if matches!(change, PublicationChange::Success(_) | PublicationChange::Failure(_))
                && (old.status != InvocationStatus::Running || old.runner_id.as_ref() != Some(&runner)) {
                return Err(RustvelloError::OwnershipViolation {
                    invocation_id: id.clone(), from_status: old.status, to_status: status,
                    current_owner: old.runner_id.as_ref().map(ToString::to_string).unwrap_or_default(),
                    attempted_owner: runner.to_string(), reason: "completion payload requires current Running ownership".into(),
                });
            }
            // State-machine validation and ownership fencing precede every payload write.
            let record = transition(&tx, &id, &runner, &old, status)?;
            point("status_history")?;
            match &change {
                PublicationChange::Retry(_) => {
                    tx.execute("INSERT INTO retries (invocation_id,retry_count) VALUES (?1,1) ON CONFLICT(invocation_id) DO UPDATE SET retry_count=retry_count+1", [id.as_str()]).map_err(sql_err)?;
                    point("counter")?;
                }
                PublicationChange::Success(result) => {
                    tx.execute("INSERT OR REPLACE INTO results (invocation_id,result) VALUES (?1,?2)", params![id.as_str(),result]).map_err(sql_err)?;
                    point("payload")?;
                }
                PublicationChange::Failure(error) => {
                    tx.execute("INSERT OR REPLACE INTO errors (invocation_id,error_type,message,traceback) VALUES (?1,?2,?3,?4)", params![id.as_str(),error.error_type,error.message,error.traceback]).map_err(sql_err)?;
                    point("payload")?;
                }
                _ => {}
            }
            if let Some(route) = route {
                let task: String = tx.query_row("SELECT task_id FROM invocations WHERE invocation_id=?1", [id.as_str()], |r| r.get(0)).map_err(sql_err)?;
                publish(&tx, id.as_str(), &task, route)?;
                tx.execute("DELETE FROM cc_arg_pairs WHERE invocation_id=?1", [id.as_str()]).map_err(sql_err)?;
                point("queue")?;
            }
            if status.is_terminal() {
                tx.execute("DELETE FROM waiting_for WHERE waited_on_id=?1", [id.as_str()]).map_err(sql_err)?;
                tx.execute("DELETE FROM cc_arg_pairs WHERE invocation_id=?1", [id.as_str()]).map_err(sql_err)?;
                if auto_purge {
                    tx.execute("INSERT OR REPLACE INTO auto_purge_schedule (invocation_id,scheduled_at) VALUES (?1,?2)", params![id.as_str(),record.timestamp.to_rfc3339()]).map_err(sql_err)?;
                }
                point("terminal_effects")?;
            }
            point("before_commit")?;
            tx.commit().map_err(sql_err)?;
            point("after_commit")?;
            Ok(Some(record))
        }).await
    }
}
