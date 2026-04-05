use chrono::Utc;
use rustvello_core::error::RustvelloError;
use rustvello_proto::identifiers::RunnerId;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::{deserialize_record, serialize_record, CC_COL, HEARTBEAT_COL, STATUS_COL, WAITERS_COL};

#[test]
fn serialize_deserialize_record_roundtrip() {
    let record = InvocationStatusRecord {
        status: InvocationStatus::Running,
        timestamp: Utc::now(),
        runner_id: Some(RunnerId::new()),
    };
    let json = serialize_record(&record).unwrap();
    let decoded = deserialize_record(&json).unwrap();
    assert_eq!(decoded.status, record.status);
    assert!(decoded.runner_id.is_some());
}

#[test]
fn deserialize_bad_json_returns_serialization_error() {
    let result = deserialize_record("{invalid}");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        RustvelloError::Serialization { .. }
    ));
}

#[test]
fn collection_names_are_distinct() {
    let cols = [STATUS_COL, WAITERS_COL, CC_COL, HEARTBEAT_COL];
    for (i, a) in cols.iter().enumerate() {
        for (j, b) in cols.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "collection names at {i} and {j} collide");
            }
        }
    }
}
