//! Core traits and types for the Rustvello distributed task system.
//!
//! This crate defines the abstract interfaces that all backend implementations
//! must satisfy:
//! - [`Broker`] — message routing between producers and runners
//! - [`InvocationControlBackend`] — atomic invocation control-state persistence
//! - [`StateBackend`] — persistence of invocations and results
//! - [`Runner`] — task execution engine
//! - [`ClientDataStore`] — external storage for large serialized values
//!
//! See `rustvello-mem` for in-memory implementations (testing),
//! `rustvello-sqlite` for SQLite (single-host production),
//! `rustvello-redis` for Redis, `rustvello-postgres` for PostgreSQL,
//! `rustvello-mongo` / `rustvello-mongo3` for MongoDB, and
//! `rustvello-rabbitmq` for RabbitMQ.

pub mod broker;
pub mod call;
pub mod client_data_store;
pub mod context;
pub mod error;
pub mod execution;
pub mod failpoints;
pub mod guarantees;
pub mod invocation;
pub mod logging;
pub mod middleware;
pub mod observability;
pub mod orchestrator;
pub mod publication;
pub mod reconnectable;
pub mod runner;
pub mod serializer;
pub mod state_backend;
pub mod task;
pub mod trigger;
pub mod workflow;

pub mod prelude {
    pub use crate::broker::Broker;
    pub use crate::call::Call;
    pub use crate::client_data_store::{ClientDataStore, ClientDataStoreManager};
    pub use crate::context::{
        current_attempt_signal, get_invocation_context, get_runner_context, AttemptSignal,
        InvocationContext, RunnerContext,
    };
    pub use crate::error::{RustvelloError, RustvelloResult};
    pub use crate::invocation::{Invocation, InvocationHandle, SyncInvocation};
    pub use crate::observability::{
        capture_w3c_trace_context, extract_w3c_trace_context, is_valid_w3c_trace_context,
        AsyncExportConfig, AsyncExportStats, BoundedAsyncEmitter, CompositeEmitter, EventEmitter,
        EventLevel, LifecycleEvent, LifecycleExporter, NoopEmitter, TaskAttemptContext,
        TaskLifecycleEvent, TaskLifecycleKind, TraceContextCarrier, WorkerLifecycleEvent,
        WorkerLifecycleKind, WorkerTelemetryContext, LIFECYCLE_CONTEXT_VERSION,
    };
    pub use crate::orchestrator::{
        InvocationControlBackend, OrchestratorBlocking, OrchestratorConcurrency, OrchestratorQuery,
        OrchestratorRecovery, OrchestratorStatus,
    };
    pub use crate::runner::Runner;
    pub use crate::serializer::{SerdeSerializer, Serializer};
    pub use crate::state_backend::StateBackend;
    pub use crate::task::{
        block_on_task_future, CrossLanguageSafe, DynTask, ForeignTask, ForeignTaskProxy, Task,
        TaskDefinition, TaskFuture, TaskModule, TaskRegistry,
    };
    pub use crate::trigger::{TriggerManager, TriggerStore};
    pub use crate::workflow::WorkflowRoot;
    pub use rustvello_proto::prelude::*;
}
