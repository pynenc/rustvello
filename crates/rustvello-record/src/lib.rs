// crates/rustvello-record/src/lib.rs

mod file_format;
mod file_recorder;
mod file_replayer;
pub mod prelude;

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use file_format::SerializationFormat;
pub use file_recorder::FileRecorder;
pub use file_replayer::FileReplayer;
use rustvello_core::{time, Config, MuseResult};
use rustvello_proto::types::*;

#[async_trait]
pub trait Recorder {
    async fn record(&mut self, event: RecordedEventWithTime) -> MuseResult<()>;
    async fn flush(&mut self) -> MuseResult<()>;
    async fn close(&mut self) -> MuseResult<()>;
}

#[async_trait]
pub trait Replayer {
    async fn next_event(&mut self) -> MuseResult<Option<RecordedEventWithTime>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RecordedEvent {
    MuseConfig(Config),
    ElementRegistration {
        local_elem_id: LocalElementId,
        kind_code: String,
        name: String,
        metadata: HashMap<String, String>,
        parent_id: Option<LocalElementId>,
    },
    SendMetric {
        local_elem_id: LocalElementId,
        metric_code: String,
        value: MetricValue,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordedEventWithTime {
    pub timestamp: i64,
    pub event: RecordedEvent,
}

impl RecordedEventWithTime {
    pub fn new(event: RecordedEvent) -> Self {
        Self {
            timestamp: time::utc_now_i64(),
            event,
        }
    }
}
