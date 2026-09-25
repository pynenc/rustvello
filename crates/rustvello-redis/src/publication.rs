//! Crash-consistent publication for co-located Redis runtime ports.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use redis::AsyncCommands;
use rustvello_core::{
    broker::validate_routing,
    error::{status_machine_error_to_rustvello, RustvelloError, RustvelloResult},
    publication::{
        PublicationChange, PublicationDomain, PublicationRoute, RuntimePublication,
        SubmissionPublication,
    },
};
use rustvello_proto::{
    call::SerializedArguments,
    identifiers::{InvocationId, RunnerId},
    invocation::{ExecutionAttemptIdentity, InvocationDTO, InvocationHistory, TraceContextCarrier},
    status::{status_record_transition, InvocationStatus, InvocationStatusRecord},
};

use crate::connection::{redis_err, RedisPool};

pub(crate) struct RedisPublication {
    pool: Arc<RedisPool>,
}

impl RedisPublication {
    pub(crate) fn new(pool: Arc<RedisPool>) -> Self {
        Self { pool }
    }

    async fn current(
        &self,
        id: &InvocationId,
    ) -> RustvelloResult<(String, InvocationStatusRecord, String, InvocationDTO)> {
        let prefix = self.pool.prefix();
        let mut conn = self.pool.conn().await?;
        let (status_json, invocation_json): (Option<String>, Option<String>) = redis::pipe()
            .get(format!("{prefix}orch:status:{}", id.as_str()))
            .get(format!("{prefix}state:inv:{}", id.as_str()))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let status_json = status_json.ok_or_else(|| RustvelloError::InvocationNotFound {
            invocation_id: id.clone(),
        })?;
        let invocation_json =
            invocation_json.ok_or_else(|| configuration("invocation state is incomplete"))?;
        let status = decode(&status_json, "status record")?;
        let invocation = decode(&invocation_json, "invocation")?;
        Ok((status_json, status, invocation_json, invocation))
    }
}

fn configuration(message: impl Into<String>) -> RustvelloError {
    RustvelloError::Configuration {
        message: message.into(),
    }
}

fn encode<T: serde::Serialize>(value: &T, purpose: &str) -> RustvelloResult<String> {
    serde_json::to_string(value).map_err(|error| RustvelloError::Serialization {
        message: format!("{purpose}: {error}"),
    })
}

fn decode<T: serde::de::DeserializeOwned>(value: &str, purpose: &str) -> RustvelloResult<T> {
    serde_json::from_str(value).map_err(|error| RustvelloError::Serialization {
        message: format!("{purpose}: {error}"),
    })
}

fn history_json(
    id: &InvocationId,
    record: &InvocationStatusRecord,
    runner: &RunnerId,
) -> RustvelloResult<String> {
    encode(
        &InvocationHistory::new(id.clone(), record.clone(), None).with_runner(runner.clone()),
        "invocation history",
    )
}

fn validate_route(route: &PublicationRoute) -> RustvelloResult<()> {
    validate_routing(&route.queue, route.priority)?;
    if route.queue.len() > 256 {
        return Err(configuration("queue name exceeds 256 bytes"));
    }
    Ok(())
}

fn queue_member(sequence: u64, id: &InvocationId) -> String {
    format!(
        "{:019}:{}",
        i64::MAX as u64 - sequence.min(i64::MAX as u64),
        id.as_str()
    )
}

#[async_trait]
impl RuntimePublication for RedisPublication {
    fn domain(&self) -> PublicationDomain {
        Arc::clone(&self.pool.domain)
    }

    async fn begin_execution(
        &self,
        id: &InvocationId,
        runner: &RunnerId,
        retries: u32,
        incoming: &TraceContextCarrier,
    ) -> RustvelloResult<ExecutionAttemptIdentity> {
        use rustvello_core::execution::{next_execution_identity, IDENTITY_KEY};

        for _ in 0..8 {
            let (status_json, status, _, _) = self.current(id).await?;
            if status.status != InvocationStatus::Running
                || status.runner_id.as_ref() != Some(runner)
            {
                return Err(configuration(
                    "execution identity requires current Running ownership",
                ));
            }
            let key = format!("{}state:wf_data:{}", self.pool.prefix(), id.as_str());
            let mut conn = self.pool.conn().await?;
            let prior_json: Option<String> =
                conn.hget(&key, IDENTITY_KEY).await.map_err(redis_err)?;
            let prior = prior_json
                .as_deref()
                .map(|value| decode(value, "execution identity"))
                .transpose()?;
            let identity = next_execution_identity(prior, retries, incoming)?;
            let identity_json = encode(&identity, "execution identity")?;
            let script = redis::Script::new(
                r#"
                if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                local current = redis.call('HGET', KEYS[2], ARGV[2])
                if ARGV[3] == 'missing' then
                    if current then return 0 end
                elseif current ~= ARGV[3] then
                    return 0
                end
                redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
                return 1
                "#,
            );
            let changed: i32 = script
                .key(format!("{}orch:status:{}", self.pool.prefix(), id.as_str()))
                .key(key)
                .arg(&status_json)
                .arg(IDENTITY_KEY)
                .arg(prior_json.as_deref().unwrap_or("missing"))
                .arg(&identity_json)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;
            if changed == 1 {
                return Ok(identity);
            }
        }
        Err(configuration("execution identity changed concurrently"))
    }

    async fn submit(&self, s: SubmissionPublication) -> RustvelloResult<bool> {
        validate_route(&s.route)?;
        let inv = &s.invocation;
        if inv.task_id != s.call.task_id || inv.call_id != s.call.call_id {
            return Err(configuration("invocation and call identities differ"));
        }
        let identity = encode(
            &serde_json::json!({
                "call": s.call,
                "parent": inv.parent_invocation_id,
                "workflow": inv.workflow,
                "trace": inv.trace_context,
                "queue": s.route.queue,
                "priority": s.route.priority,
                "workflow_root": s.workflow_root,
                "cc": s.cc_arguments,
            }),
            "submission identity",
        )?;
        if identity.len() > self.pool.options.max_payload_bytes as usize {
            return Err(configuration("submission exceeds payload limit"));
        }

        let invocation_json = encode(inv, "invocation")?;
        let call_json = encode(&s.call, "call")?;
        let status = InvocationStatusRecord {
            status: InvocationStatus::Registered,
            runner_id: Some(s.runner_id.clone()),
            timestamp: Utc::now(),
        };
        let status_json = encode(&status, "status record")?;
        let history = history_json(&inv.invocation_id, &status, &s.runner_id)?;
        let workflow_id = inv
            .workflow
            .as_ref()
            .map(|workflow| workflow.workflow_id.as_str())
            .unwrap_or_default();
        let workflow_type = inv
            .workflow
            .as_ref()
            .map(|workflow| workflow.workflow_type.to_string())
            .unwrap_or_default();
        let workflow_json = inv
            .workflow
            .as_ref()
            .map(|workflow| encode(workflow, "workflow"))
            .transpose()?
            .unwrap_or_default();
        let parent_id = inv
            .parent_invocation_id
            .as_ref()
            .map(InvocationId::as_str)
            .unwrap_or_default();
        let runner_json = s
            .runner_context
            .as_ref()
            .map(|context| encode(context, "runner context"))
            .transpose()?
            .unwrap_or_default();
        let cc_pairs = s
            .cc_arguments
            .as_ref()
            .map(SerializedArguments::cc_arg_pairs)
            .unwrap_or_default();
        let cc_json = encode(&cc_pairs, "concurrency keys")?;
        let mut conn = self.pool.conn().await?;
        let sequence: u64 = conn
            .incr(format!("{}broker:sequence", self.pool.prefix()), 1_u64)
            .await
            .map_err(redis_err)?;
        let member = queue_member(sequence, &inv.invocation_id);
        let script = redis::Script::new(
            r#"
            local p = ARGV[1]
            local id = ARGV[2]
            local publication = p .. 'publication:submission:' .. id
            local existing = redis.call('GET', publication)
            if existing then
                if existing == '' then return -2 end
                if existing ~= ARGV[3] then return -3 end
                return 0
            end
            local inv_key = p .. 'state:inv:' .. id
            local status_key = p .. 'orch:status:' .. id
            if redis.call('EXISTS', inv_key, status_key, p .. 'state:history:' .. id,
                p .. 'state:result:' .. id, p .. 'state:error:' .. id,
                p .. 'state:wf_data:' .. id) > 0 then return -6 end
            local call_key = p .. 'state:call:' .. ARGV[5]
            local old_call = redis.call('GET', call_key)
            if old_call and old_call ~= ARGV[6] then return -5 end
            local count_key = p .. 'broker:count'
            local count = tonumber(redis.call('GET', count_key) or '0')
            if count >= tonumber(ARGV[13]) then return -4 end

            redis.call('SET', inv_key, ARGV[4])
            redis.call('SET', call_key, ARGV[6])
            redis.call('SET', status_key, ARGV[7])
            redis.call('SADD', p .. 'orch:task_inv:' .. ARGV[10], id)
            redis.call('SADD', p .. 'orch:call_inv:' .. ARGV[5], id)
            redis.call('RPUSH', p .. 'state:history:' .. id, ARGV[8])
            redis.call('ZADD', p .. 'state:history_ts', ARGV[9], ARGV[8])
            if ARGV[15] ~= '' then redis.call('SADD', p .. 'state:wf:' .. ARGV[15], id) end
            if ARGV[16] ~= '' then redis.call('SADD', p .. 'state:child:' .. ARGV[16], id) end
            if ARGV[19] == '1' and ARGV[15] ~= '' then
                redis.call('SADD', p .. 'state:wf_types', ARGV[17])
                redis.call('HSET', p .. 'state:wf_runs:' .. ARGV[17], ARGV[15], ARGV[18])
            end
            if ARGV[21] ~= '' then redis.call('SETNX', p .. 'state:runner:' .. ARGV[20], ARGV[21]) end
            local pairs = cjson.decode(ARGV[22])
            local reverse = p .. 'orch:cc_rev:' .. id
            for _, pair in ipairs(pairs) do
                local key = p .. 'orch:cc:' .. ARGV[10] .. '\31' .. pair[1] .. '\31' .. pair[2]
                redis.call('SADD', key, id)
                redis.call('SADD', reverse, key)
            end
            local queue_key = p .. 'broker:queue:' .. ARGV[11]
            local metadata_key = p .. 'broker:metadata:' .. ARGV[11]
            redis.call('ZADD', queue_key, ARGV[12], ARGV[14])
            redis.call('HSET', metadata_key, ARGV[14], ARGV[10])
            redis.call('HSET', p .. 'broker:queued_by_inv', id, ARGV[11] .. '\31' .. ARGV[14])
            redis.call('SET', count_key, count + 1)
            redis.call('SET', publication, ARGV[3])
            return 1
            "#,
        );
        let code: i32 = script
            .arg(self.pool.prefix())
            .arg(inv.invocation_id.as_str())
            .arg(identity)
            .arg(invocation_json)
            .arg(s.call.call_id.to_string())
            .arg(call_json)
            .arg(status_json)
            .arg(history)
            .arg(status.timestamp.timestamp_millis())
            .arg(inv.task_id.to_string())
            .arg(&s.route.queue)
            .arg(s.route.priority)
            .arg(self.pool.options.max_queue_rows)
            .arg(member)
            .arg(workflow_id)
            .arg(parent_id)
            .arg(workflow_type)
            .arg(workflow_json)
            .arg(if s.workflow_root { "1" } else { "0" })
            .arg(s.runner_id.as_str())
            .arg(runner_json)
            .arg(cc_json)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match code {
            1 => Ok(true),
            0 => Ok(false),
            -2 => Err(configuration("submission ID was removed; use a fresh ID")),
            -3 => Err(configuration(
                "submission ID already accepted with different content or lineage",
            )),
            -4 => Err(configuration("Redis queue admission capacity reached")),
            -5 => Err(configuration("call identity already has different content")),
            -6 => Err(configuration(
                "submission ID belongs to a legacy invocation; use a fresh ID",
            )),
            _ => Err(configuration("unexpected Redis publication response")),
        }
    }

    async fn change(
        &self,
        id: &InvocationId,
        runner: &RunnerId,
        change: PublicationChange,
        auto_purge: bool,
    ) -> RustvelloResult<Option<InvocationStatusRecord>> {
        if matches!(change, PublicationChange::DelayedRetry { .. }) {
            // Not declared via supports_delayed_retry(); never publish it by mistake
            // as an immediate reroute.
            return Err(RustvelloError::NotSupported {
                backend: "redis".to_owned(),
                method: "durable delayed retry publication".to_owned(),
            });
        }
        for _ in 0..8 {
            let (old_json, old, _, mut invocation) = self.current(id).await?;
            let mut heartbeat_key = String::new();
            let mut heartbeat_value = String::new();
            let mut records = Vec::with_capacity(2);
            let route = match &change {
                PublicationChange::Recover {
                    status,
                    stale_after_seconds,
                    route,
                } => {
                    let expected = match status {
                        InvocationStatus::PendingRecovery => InvocationStatus::Pending,
                        InvocationStatus::RunningRecovery => InvocationStatus::Running,
                        _ => return Err(configuration("invalid recovery status")),
                    };
                    if old.status != expected {
                        return Ok(None);
                    }
                    let cutoff = Utc::now()
                        - chrono::Duration::seconds((*stale_after_seconds).min(31_536_000) as i64);
                    let last_seen = if expected == InvocationStatus::Pending {
                        Some(old.timestamp)
                    } else {
                        heartbeat_key = format!(
                            "{}orch:heartbeat:{}",
                            self.pool.prefix(),
                            old.runner_id
                                .as_ref()
                                .map(RunnerId::as_str)
                                .unwrap_or_default()
                        );
                        let mut conn = self.pool.conn().await?;
                        let value: Option<String> =
                            conn.get(&heartbeat_key).await.map_err(redis_err)?;
                        heartbeat_value = value.clone().unwrap_or_else(|| "__missing__".into());
                        value
                            .as_deref()
                            .map(chrono::DateTime::parse_from_rfc3339)
                            .transpose()
                            .map_err(|_| configuration("invalid runner heartbeat"))?
                            .map(|value| value.with_timezone(&Utc))
                    };
                    if last_seen.is_some_and(|timestamp| timestamp >= cutoff) {
                        return Ok(None);
                    }
                    let recovery = status_record_transition(Some(&old), *status, Some(runner))
                        .map_err(|error| {
                            status_machine_error_to_rustvello(error, id, old.status)
                        })?;
                    records.push(recovery.clone());
                    records.push(
                        status_record_transition(
                            Some(&recovery),
                            InvocationStatus::Rerouted,
                            Some(runner),
                        )
                        .map_err(|error| {
                            status_machine_error_to_rustvello(error, id, recovery.status)
                        })?,
                    );
                    Some(route)
                }
                PublicationChange::ConcurrencyReroute(route) => {
                    let controlled = status_record_transition(
                        Some(&old),
                        InvocationStatus::ConcurrencyControlled,
                        Some(runner),
                    )
                    .map_err(|error| status_machine_error_to_rustvello(error, id, old.status))?;
                    records.push(controlled.clone());
                    records.push(
                        status_record_transition(
                            Some(&controlled),
                            InvocationStatus::Rerouted,
                            Some(runner),
                        )
                        .map_err(|error| {
                            status_machine_error_to_rustvello(error, id, controlled.status)
                        })?,
                    );
                    Some(route)
                }
                PublicationChange::Retry(route) | PublicationChange::Reroute(route) => Some(route),
                _ => None,
            };
            let target = match &change {
                PublicationChange::Status(status) => *status,
                PublicationChange::Retry(_) => InvocationStatus::Retry,
                PublicationChange::Success(_) => InvocationStatus::Success,
                PublicationChange::Failure(_) => InvocationStatus::Failed,
                PublicationChange::Recover { .. } | PublicationChange::ConcurrencyReroute(_) => {
                    InvocationStatus::Rerouted
                }
                PublicationChange::Reroute(_) => InvocationStatus::Rerouted,
                PublicationChange::DelayedRetry { .. } => unreachable!("rejected above"),
            };
            if matches!(
                change,
                PublicationChange::Success(_) | PublicationChange::Failure(_)
            ) && (old.status != InvocationStatus::Running
                || old.runner_id.as_ref() != Some(runner))
            {
                return Err(RustvelloError::OwnershipViolation {
                    invocation_id: id.clone(),
                    from_status: old.status,
                    to_status: target,
                    current_owner: old
                        .runner_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    attempted_owner: runner.to_string(),
                    reason: "completion requires current Running ownership".into(),
                });
            }
            if records.is_empty() {
                records.push(
                    status_record_transition(Some(&old), target, Some(runner)).map_err(
                        |error| status_machine_error_to_rustvello(error, id, old.status),
                    )?,
                );
            }
            let final_record = records.last().expect("status transition exists").clone();
            invocation.status = final_record.status;
            invocation.updated_at = final_record.timestamp;
            let invocation_json = encode(&invocation, "invocation")?;
            let status_json = encode(&final_record, "status record")?;
            let histories = records
                .iter()
                .map(|record| history_json(id, record, runner))
                .collect::<RustvelloResult<Vec<_>>>()?;
            let (payload_mode, payload) = match &change {
                PublicationChange::Success(result) => ("result", result.clone()),
                PublicationChange::Failure(error) => ("error", encode(error, "task error")?),
                _ => ("none", String::new()),
            };
            if payload.len() > self.pool.options.max_payload_bytes as usize {
                return Err(configuration("completion payload exceeds limit"));
            }
            if let Some(route) = route {
                validate_route(route)?;
            }
            let task = invocation.task_id.to_string();
            let mut conn = self.pool.conn().await?;
            let sequence: u64 = if route.is_some() {
                conn.incr(format!("{}broker:sequence", self.pool.prefix()), 1_u64)
                    .await
                    .map_err(redis_err)?
            } else {
                0
            };
            let member = queue_member(sequence, id);
            let route_queue = route.map(|value| value.queue.as_str()).unwrap_or_default();
            let route_priority = route.map_or(0.0, |value| value.priority);
            let script = redis::Script::new(
                r#"
                local p, id = ARGV[1], ARGV[2]
                if redis.call('GET', KEYS[1]) ~= ARGV[3] then return 0 end
                if ARGV[22] ~= '' then
                    local heartbeat = redis.call('GET', ARGV[22])
                    if ARGV[23] == '__missing__' then
                        if heartbeat then return -2 end
                    elseif heartbeat ~= ARGV[23] then return -2 end
                end
                local queued = redis.call('HGET', p .. 'broker:queued_by_inv', id)
                local leased = redis.call('HGET', p .. 'broker:leased_by_inv', id)
                local had = queued or leased
                local count_key = p .. 'broker:count'
                local count = tonumber(redis.call('GET', count_key) or '0')
                if ARGV[17] == '1' and not had and count >= tonumber(ARGV[20]) then return -4 end
                if queued then
                    local split = string.find(queued, '\31', 1, true)
                    local queue, member = string.sub(queued, 1, split - 1), string.sub(queued, split + 1)
                    redis.call('ZREM', p .. 'broker:queue:' .. queue, member)
                    redis.call('HDEL', p .. 'broker:metadata:' .. queue, member)
                    redis.call('HDEL', p .. 'broker:queued_by_inv', id)
                end
                if leased then
                    redis.call('ZREM', p .. 'broker:leases', leased)
                    redis.call('HDEL', p .. 'broker:lease_payload', leased)
                    redis.call('HDEL', p .. 'broker:leased_by_inv', id)
                end
                if had and ARGV[17] ~= '1' then count = math.max(0, count - 1) end
                redis.call('SET', KEYS[1], ARGV[4])
                redis.call('SET', p .. 'state:inv:' .. id, ARGV[5])
                redis.call('RPUSH', p .. 'state:history:' .. id, ARGV[6])
                redis.call('ZADD', p .. 'state:history_ts', ARGV[8], ARGV[6])
                if ARGV[7] ~= '' then
                    redis.call('RPUSH', p .. 'state:history:' .. id, ARGV[7])
                    redis.call('ZADD', p .. 'state:history_ts', ARGV[9], ARGV[7])
                end
                if ARGV[10] == 'result' then redis.call('SET', p .. 'state:result:' .. id, ARGV[11]) end
                if ARGV[10] == 'error' then redis.call('SET', p .. 'state:error:' .. id, ARGV[11]) end
                if ARGV[12] == '1' then redis.call('INCR', p .. 'orch:retries:' .. id) end
                if ARGV[17] == '1' then
                    redis.call('ZADD', p .. 'broker:queue:' .. ARGV[18], ARGV[19], ARGV[21])
                    redis.call('HSET', p .. 'broker:metadata:' .. ARGV[18], ARGV[21], ARGV[16])
                    redis.call('HSET', p .. 'broker:queued_by_inv', id, ARGV[18] .. '\31' .. ARGV[21])
                    if not had then count = count + 1 end
                end
                redis.call('SET', count_key, count)
                if ARGV[13] == '1' then
                    redis.call('DEL', p .. 'orch:waiters:' .. id)
                    local reverse = p .. 'orch:cc_rev:' .. id
                    for _, key in ipairs(redis.call('SMEMBERS', reverse)) do redis.call('SREM', key, id) end
                    redis.call('DEL', reverse)
                    if ARGV[14] == '1' then redis.call('SET', p .. 'orch:auto_purge:' .. id, ARGV[15]) end
                end
                return 1
                "#,
            );
            let code: i32 = script
                .key(format!("{}orch:status:{}", self.pool.prefix(), id.as_str()))
                .arg(self.pool.prefix())
                .arg(id.as_str())
                .arg(&old_json)
                .arg(status_json)
                .arg(invocation_json)
                .arg(&histories[0])
                .arg(histories.get(1).map(String::as_str).unwrap_or_default())
                .arg(records[0].timestamp.timestamp_millis())
                .arg(
                    records
                        .get(1)
                        .unwrap_or(&records[0])
                        .timestamp
                        .timestamp_millis(),
                )
                .arg(payload_mode)
                .arg(payload)
                .arg(if matches!(change, PublicationChange::Retry(_)) {
                    "1"
                } else {
                    "0"
                })
                .arg(if final_record.status.is_terminal() {
                    "1"
                } else {
                    "0"
                })
                .arg(if auto_purge { "1" } else { "0" })
                .arg(final_record.timestamp.to_rfc3339())
                .arg(task)
                .arg(if route.is_some() { "1" } else { "0" })
                .arg(route_queue)
                .arg(route_priority)
                .arg(self.pool.options.max_queue_rows)
                .arg(member)
                .arg(heartbeat_key)
                .arg(heartbeat_value)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;
            match code {
                1 => return Ok(Some(final_record)),
                0 if matches!(change, PublicationChange::Recover { .. }) => return Ok(None),
                0 => continue,
                -2 => return Ok(None),
                -4 => return Err(configuration("Redis queue admission capacity reached")),
                _ => return Err(configuration("unexpected Redis transition response")),
            }
        }
        Err(configuration("status changed concurrently"))
    }
}
