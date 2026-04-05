"""Public package for rustvello — Python bindings for distributed task execution."""

from rustvello.rustvello import (
    AppConfig,
    ConcurrencyControlType,
    InvocationId,
    InvocationResult,
    InvocationStatus,
    RustMemBroker,
    RustMemClientDataStore,
    RustMemOrchestrator,
    RustMemStateBackend,
    RustMemTriggerStore,
    RustMongo3Broker,
    RustMongo3ClientDataStore,
    RustMongo3Orchestrator,
    # MongoDB 3.6+ (legacy driver)
    RustMongo3Pool,
    RustMongo3StateBackend,
    RustMongo3TriggerStore,
    RustMongoBroker,
    RustMongoClientDataStore,
    RustMongoOrchestrator,
    # MongoDB
    RustMongoPool,
    RustMongoStateBackend,
    RustMongoTriggerStore,
    RustPostgresBroker,
    RustPostgresClientDataStore,
    # PostgreSQL
    RustPostgresDatabase,
    RustPostgresOrchestrator,
    RustPostgresStateBackend,
    RustPostgresTriggerStore,
    # RabbitMQ
    RustRabbitmqBroker,
    RustRedisBroker,
    RustRedisClientDataStore,
    RustRedisOrchestrator,
    # Redis
    RustRedisPool,
    RustRedisStateBackend,
    RustRedisTriggerStore,
    RustSqliteBroker,
    RustSqliteClientDataStore,
    # SQLite
    RustSqliteDatabase,
    RustSqliteOrchestrator,
    RustSqliteStateBackend,
    RustSqliteTriggerStore,
    RustTaskRunner,
    RustTaskRunnerBuilder,
    Rustvello,
    TaskConfig,
    TaskId,
    compute_args_id,
    get_current_invocation_id,
    get_current_num_retries,
    get_current_workflow_info,
    get_version,
    init_logging,
    status_from_serde,
    status_to_serde,
)

__version__: str = get_version()

from rustvello.app import App, Invocation, TaskHandle

__all__ = [
    # Standalone DX layer
    "App",
    "Invocation",
    "TaskHandle",
    # Public API — types and configuration
    "AppConfig",
    "ConcurrencyControlType",
    "InvocationId",
    "InvocationResult",
    "InvocationStatus",
    "Rustvello",
    "TaskConfig",
    "TaskId",
    "get_version",
    "init_logging",
    # Utility functions
    "compute_args_id",
    "get_current_invocation_id",
    "get_current_num_retries",
    "get_current_workflow_info",
    "status_from_serde",
    "status_to_serde",
    # Runner
    "RustTaskRunner",
    "RustTaskRunnerBuilder",
    # Memory backend
    "RustMemBroker",
    "RustMemClientDataStore",
    "RustMemOrchestrator",
    "RustMemStateBackend",
    "RustMemTriggerStore",
    # SQLite backend
    "RustSqliteDatabase",
    "RustSqliteBroker",
    "RustSqliteOrchestrator",
    "RustSqliteStateBackend",
    "RustSqliteTriggerStore",
    "RustSqliteClientDataStore",
    # PostgreSQL backend
    "RustPostgresDatabase",
    "RustPostgresBroker",
    "RustPostgresOrchestrator",
    "RustPostgresStateBackend",
    "RustPostgresTriggerStore",
    "RustPostgresClientDataStore",
    # Redis backend
    "RustRedisPool",
    "RustRedisBroker",
    "RustRedisOrchestrator",
    "RustRedisStateBackend",
    "RustRedisTriggerStore",
    "RustRedisClientDataStore",
    # MongoDB backend
    "RustMongoPool",
    "RustMongoBroker",
    "RustMongoOrchestrator",
    "RustMongoStateBackend",
    "RustMongoTriggerStore",
    "RustMongoClientDataStore",
    # MongoDB 3.6+ backend (legacy driver)
    "RustMongo3Pool",
    "RustMongo3Broker",
    "RustMongo3Orchestrator",
    "RustMongo3StateBackend",
    "RustMongo3TriggerStore",
    "RustMongo3ClientDataStore",
    # RabbitMQ backend
    "RustRabbitmqBroker",
]
