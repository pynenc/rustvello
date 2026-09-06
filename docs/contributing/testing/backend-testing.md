# Adding Backend Tests

This guide explains how to add tests when implementing a new backend or
extending an existing one.

## Adding a New Backend

When you create a new backend crate (e.g., `rustvello-dynamodb`) that implements
one or more core traits (`Broker`, `InvocationControlBackend`, `StateBackend`,
`TriggerStore`, `ClientDataStore`), follow these steps to wire up the shared
test suite.

### Step 1: Add the test-suite dependency

In your crate's `Cargo.toml`:

```toml
[dev-dependencies]
rustvello-test-suite = { path = "../rustvello-test-suite" }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

If your backend requires Docker (most external services do), also add:

```toml
[dev-dependencies]
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true, features = ["your_module"] }
```

### Step 2: Create the test file

Create `tests/suite.rs` in your crate:

#### For in-process backends (no Docker)

Use the **sync** suite macros:

```rust
//! Integration tests using the shared test suite.

mod broker_suite {
    use your_crate::YourBroker;
    rustvello_test_suite::broker_suite!(YourBroker::new());
}

mod orchestrator_suite {
    use your_crate::YourOrchestrator;
    rustvello_test_suite::orchestrator_suite!(YourOrchestrator::new());
}

mod state_backend_suite {
    use your_crate::YourStateBackend;
    rustvello_test_suite::state_backend_suite!(YourStateBackend::new());
}

mod trigger_suite {
    use your_crate::YourTriggerStore;
    rustvello_test_suite::trigger_suite!(YourTriggerStore::new());
}

mod client_data_store_suite {
    use your_crate::YourClientDataStore;
    rustvello_test_suite::client_data_store_suite!(YourClientDataStore::new());
}

mod lifecycle_suite {
    use std::sync::Arc;
    use your_crate::*;
    use rustvello_test_suite::lifecycle::BackendTriple;

    rustvello_test_suite::lifecycle_suite!({
        BackendTriple {
            broker: Arc::new(YourBroker::new()),
            orchestrator: Arc::new(YourOrchestrator::new()),
            state_backend: Arc::new(YourStateBackend::new()),
        }
    });
}
```

#### For Docker backends

Use the **async** suite macros with factory functions:

````rust
//! Integration tests using testcontainers.
//!
//! These tests require Docker. Run with:
//! ```bash
//! cargo test -p your-crate -- --ignored
//! ```

use std::sync::Arc;
use your_crate::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::your_module::YourImage;

/// Start a container and return a connected client.
async fn start_container() -> (testcontainers::ContainerAsync<YourImage>, YourClient) {
    let container = YourImage::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(YOUR_PORT).await.unwrap();
    let client = YourClient::connect(host, port).await.unwrap();
    (container, client)
}

async fn make_broker() -> (testcontainers::ContainerAsync<YourImage>, YourBroker) {
    let (c, client) = start_container().await;
    (c, YourBroker::new(client))
}

// ... similar factory functions for each trait implementation

mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}

// ... similar modules for each suite
````

### Step 3: Implement only the traits you support

Not every backend needs to implement all 5 traits. For example,
`rustvello-rabbitmq` only implements `Broker`, so its `tests/suite.rs` only
contains:

```rust
mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}
```

### Step 4: Verify with the validator

Run the all-tests validator to confirm your tests are properly wired:

```bash
cargo test -p rustvello-test-suite
```

If you're a direct consumer (not via macros), add your crate's test file to the
`CONSUMERS` list in `all_tests_validator.rs`.

## Adding a New Shared Test

When you need to test a new trait method or behavior:

### 1. Add the test function

Add `pub async fn test_your_new_behavior(backend: &dyn YourTrait)` to the
appropriate module in `rustvello-test-suite/src/`:

```rust
// In broker.rs
pub async fn test_your_new_behavior(broker: &dyn Broker) {
    // Arrange
    let id = InvocationId::new("test_task", "inv_1");

    // Act
    broker.your_new_method(&id).await.unwrap();

    // Assert
    let result = broker.get_something(&id).await.unwrap();
    assert_eq!(result, expected_value);
}
```

### 2. Add it to both macros

Add the test to both the sync and async macros in the same file:

```rust
// In the broker_suite! macro:
#[tokio::test]
async fn suite_broker_your_new_behavior() {
    let broker = $setup;
    $crate::broker::test_your_new_behavior(&broker).await;
}

// In the async_broker_suite! macro:
#[tokio::test]
#[ignore = "requires Docker"]
async fn suite_broker_your_new_behavior() {
    let (_c, broker) = $setup.await;
    $crate::broker::test_your_new_behavior(&broker).await;
}
```

### 3. Run the validator

```bash
cargo test -p rustvello-test-suite
```

If you added the function but forgot the macro entry, the validator will catch
it and fail with a clear error message listing the missing test.

## Current Backend Coverage

| Backend  | Broker | Orchestrator | StateBackend | TriggerStore | ClientDataStore | Lifecycle |
| -------- | :----: | :----------: | :----------: | :----------: | :-------------: | :-------: |
| mem      |   ✅   |      ✅      |      ✅      |      ✅      |       ✅        |    ✅     |
| sqlite   |   ✅   |      ✅      |      ✅      |      ✅      |       ✅        |    ✅     |
| redis    |   ✅   |      ✅      |      ✅      |      ✅      |       ✅        |    ✅     |
| mongo    |   ✅   |      ✅      |      ✅      |      ✅      |       ✅        |    ✅     |
| postgres |   ✅   |      ✅      |      ✅      |      ✅      |       ✅        |    ✅     |
| rabbitmq |   ✅   |      —       |      —       |      —       |        —        |     —     |

RabbitMQ only implements the `Broker` trait (message queue functionality).

## Naming Conventions

- Test function names: `test_<behavior_under_test>` (e.g., `test_route_and_retrieve`)
- Generated test names via macros: `suite_<trait>_<behavior>` (e.g., `suite_broker_route_and_retrieve`)
- Test modules: `<trait>_suite` (e.g., `broker_suite`, `orchestrator_suite`)
- Factory functions (Docker): `make_<trait_name>()` returning `(ContainerAsync<T>, Backend)`
