use std::sync::Arc;

use chrono::Utc;

use rustvello_core::error::RustvelloError;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::{deserialize_status_record, serialize_status_record, RedisOrchestrator};
use crate::connection::RedisPool;

fn test_pool() -> Arc<RedisPool> {
    Arc::new(RedisPool::new("redis://localhost/", "test_app").unwrap())
}

#[test]
fn status_key_format() {
    let orch = RedisOrchestrator::new(test_pool());
    let inv_id = InvocationId::new();
    let key = orch.status_key(&inv_id);
    assert!(key.starts_with(&orch.status_prefix));
    assert!(key.ends_with(&inv_id.to_string()));
}

#[test]
fn serialize_deserialize_status_record_roundtrip() {
    let record = InvocationStatusRecord {
        status: InvocationStatus::Pending,
        timestamp: Utc::now(),
        runner_id: Some(RunnerId::new()),
    };
    let json = serialize_status_record(&record).unwrap();
    let decoded = deserialize_status_record(&json).unwrap();
    assert_eq!(decoded.status, record.status);
    assert!(decoded.runner_id.is_some());
}

#[test]
fn deserialize_bad_json_returns_error() {
    let result = deserialize_status_record("not json");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        RustvelloError::Serialization { .. }
    ));
}

#[test]
fn key_prefixes_are_distinct() {
    let orch = RedisOrchestrator::new(test_pool());
    let prefixes = [
        &orch.status_prefix,
        &orch.task_inv_prefix,
        &orch.call_inv_prefix,
        &orch.waiters_prefix,
        &orch.cc_prefix,
        &orch.heartbeat_prefix,
    ];
    for (i, a) in prefixes.iter().enumerate() {
        for (j, b) in prefixes.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "prefixes at {i} and {j} collide");
            }
        }
    }
}
