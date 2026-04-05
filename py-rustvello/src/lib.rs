use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use rustvello_python::app::PyRustvello;
use rustvello_python::broker::PyMemBroker;
use rustvello_python::client_data_store::PyMemClientDataStore;
use rustvello_python::config::{PyAppConfig, PyTaskConfig};
use rustvello_python::error::register_exceptions;
use rustvello_python::identifiers::{PyInvocationId, PyTaskId};
use rustvello_python::invocation::PyInvocationResult;
use rustvello_python::mongo::{
    PyMongoBroker, PyMongoClientDataStore, PyMongoOrchestrator, PyMongoPool, PyMongoStateBackend,
    PyMongoTriggerStore,
};
use rustvello_python::mongo3::{
    PyMongo3Broker, PyMongo3ClientDataStore, PyMongo3Orchestrator, PyMongo3Pool,
    PyMongo3StateBackend, PyMongo3TriggerStore,
};
use rustvello_python::orchestrator::PyMemOrchestrator;
use rustvello_python::postgres::{
    PyPostgresBroker, PyPostgresClientDataStore, PyPostgresDatabase, PyPostgresOrchestrator,
    PyPostgresStateBackend, PyPostgresTriggerStore,
};
use rustvello_python::rabbitmq::PyRabbitmqBroker;
use rustvello_python::redis::{
    PyRedisBroker, PyRedisClientDataStore, PyRedisOrchestrator, PyRedisPool, PyRedisStateBackend,
    PyRedisTriggerStore,
};
use rustvello_python::runner::{PyTaskRunner, PyTaskRunnerBuilder};
use rustvello_python::sqlite::{
    PySqliteBroker, PySqliteClientDataStore, PySqliteDatabase, PySqliteOrchestrator,
    PySqliteStateBackend, PySqliteTriggerStore,
};
use rustvello_python::state_backend::PyMemStateBackend;
use rustvello_python::status::{
    status_from_serde, status_to_serde, PyConcurrencyControlType, PyInvocationStatus,
};
use rustvello_python::trigger::PyMemTriggerStore;

#[pymodule]
fn rustvello(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Exceptions
    register_exceptions(py, m)?;

    // App
    m.add_class::<PyRustvello>()?;

    // Config
    m.add_class::<PyAppConfig>()?;
    m.add_class::<PyTaskConfig>()?;

    // Identifiers
    m.add_class::<PyTaskId>()?;
    m.add_class::<PyInvocationId>()?;

    // Status
    m.add_class::<PyInvocationStatus>()?;
    m.add_class::<PyConcurrencyControlType>()?;
    m.add_function(wrap_pyfunction!(status_to_serde, m)?)?;
    m.add_function(wrap_pyfunction!(status_from_serde, m)?)?;

    // Invocation result
    m.add_class::<PyInvocationResult>()?;

    // Backend components — Memory
    m.add_class::<PyMemBroker>()?;
    m.add_class::<PyMemOrchestrator>()?;
    m.add_class::<PyMemStateBackend>()?;
    m.add_class::<PyMemTriggerStore>()?;
    m.add_class::<PyMemClientDataStore>()?;

    // Backend components — SQLite
    m.add_class::<PySqliteDatabase>()?;
    m.add_class::<PySqliteBroker>()?;
    m.add_class::<PySqliteOrchestrator>()?;
    m.add_class::<PySqliteStateBackend>()?;
    m.add_class::<PySqliteTriggerStore>()?;
    m.add_class::<PySqliteClientDataStore>()?;

    // Backend components — PostgreSQL
    m.add_class::<PyPostgresDatabase>()?;
    m.add_class::<PyPostgresBroker>()?;
    m.add_class::<PyPostgresOrchestrator>()?;
    m.add_class::<PyPostgresStateBackend>()?;
    m.add_class::<PyPostgresTriggerStore>()?;
    m.add_class::<PyPostgresClientDataStore>()?;

    // Backend components — Redis
    m.add_class::<PyRedisPool>()?;
    m.add_class::<PyRedisBroker>()?;
    m.add_class::<PyRedisOrchestrator>()?;
    m.add_class::<PyRedisStateBackend>()?;
    m.add_class::<PyRedisTriggerStore>()?;
    m.add_class::<PyRedisClientDataStore>()?;

    // Backend components — MongoDB
    m.add_class::<PyMongoPool>()?;
    m.add_class::<PyMongoBroker>()?;
    m.add_class::<PyMongoOrchestrator>()?;
    m.add_class::<PyMongoStateBackend>()?;
    m.add_class::<PyMongoTriggerStore>()?;
    m.add_class::<PyMongoClientDataStore>()?;

    // Backend components — MongoDB 3.6+ (legacy driver)
    m.add_class::<PyMongo3Pool>()?;
    m.add_class::<PyMongo3Broker>()?;
    m.add_class::<PyMongo3Orchestrator>()?;
    m.add_class::<PyMongo3StateBackend>()?;
    m.add_class::<PyMongo3TriggerStore>()?;
    m.add_class::<PyMongo3ClientDataStore>()?;

    // Backend components — RabbitMQ (broker only)
    m.add_class::<PyRabbitmqBroker>()?;

    // Runner
    m.add_class::<PyTaskRunner>()?;
    m.add_class::<PyTaskRunnerBuilder>()?;

    #[pyfunction]
    fn get_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    m.add_function(wrap_pyfunction!(get_version, m)?)?;

    m.add_function(wrap_pyfunction!(
        rustvello_python::utils::get_current_invocation_id,
        m
    )?)?;

    m.add_function(wrap_pyfunction!(
        rustvello_python::utils::get_current_num_retries,
        m
    )?)?;

    m.add_function(wrap_pyfunction!(
        rustvello_python::utils::get_current_workflow_info,
        m
    )?)?;

    // Logging
    m.add_function(wrap_pyfunction!(
        rustvello_python::logging::init_logging,
        m
    )?)?;

    m.add_function(wrap_pyfunction!(
        rustvello_python::utils::compute_args_id,
        m
    )?)?;

    Ok(())
}
