//! Integration tests using the shared test suite.

mod broker_suite {
    use rustvello_mem::broker::MemBroker;
    rustvello_test_suite::broker_suite!(MemBroker::new());
}

mod orchestrator_suite {
    use rustvello_mem::orchestrator::MemOrchestrator;
    rustvello_test_suite::orchestrator_suite!(MemOrchestrator::new());
}

mod state_backend_suite {
    use rustvello_mem::state_backend::MemStateBackend;
    rustvello_test_suite::state_backend_suite!(MemStateBackend::new());
}

mod trigger_suite {
    use rustvello_mem::trigger::MemTriggerStore;
    rustvello_test_suite::trigger_suite!(MemTriggerStore::new());
}

mod client_data_store_suite {
    use rustvello_mem::client_data_store::MemClientDataStore;
    rustvello_test_suite::client_data_store_suite!(MemClientDataStore::new());
}

mod concurrency_suite {
    use rustvello_mem::orchestrator::MemOrchestrator;
    rustvello_test_suite::concurrency_suite!(MemOrchestrator::new());
}

mod lifecycle_suite {
    use std::sync::Arc;

    use rustvello_mem::broker::MemBroker;
    use rustvello_mem::orchestrator::MemOrchestrator;
    use rustvello_mem::state_backend::MemStateBackend;
    use rustvello_test_suite::lifecycle::BackendTriple;

    rustvello_test_suite::lifecycle_suite!({
        BackendTriple {
            broker: Arc::new(MemBroker::new()),
            orchestrator: Arc::new(MemOrchestrator::new()),
            state_backend: Arc::new(MemStateBackend::new()),
        }
    });
}
