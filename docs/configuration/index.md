# Configuration

The `Config` class (in Python) and the `Config` struct (in Rust) are central to initializing and customizing the behavior of the Muse client. They encapsulate all the necessary settings required to interact with the Muse system effectively.

## Overview

The configuration allows you to:

- Specify endpoints for connecting to the Muse system.
- Choose the client type (e.g., `Poet` or `Mock`).
- Define element kinds and metric definitions for registration and reporting.
- Set default timestamp resolutions.
- Enable or disable event recording and specify recording paths.
- Configure retries and intervals for various operations, including initialization, cluster monitoring, and recording flush intervals.

## Configuration Fields

- **Endpoints**: List of endpoint URLs for the Muse client.
- **Client Type**: Determines the type of client (`Poet` or `Mock`).
- **Recording Enabled**: Enables or disables event recording.
- **Recording Path**: File path for recording events (required if recording is enabled).
- **Recording Flush Interval**: Interval for flushing recorded events to storage (optional).
- **Default Resolution**: Default timestamp resolution for metrics.
- **Element Kinds**: List of element kinds to register.
- **Metric Definitions**: List of metric definitions available for reporting.
- **Cluster Monitor Interval**: Interval for cluster monitoring tasks (optional).
- **Initialization Interval**: Interval for performing initialization tasks (optional).
- **Max Registration Retries**: Maximum number of retries for element registration.

## Validation Rules

The configuration settings undergo validation to ensure that:

- **Endpoints**: Must not be empty if using the `Poet` client type.
- **Element Kinds**: Cannot be empty; at least one must be provided.
- **Metric Definitions**: Cannot be empty; at least one must be provided.
- **Recording Path**: Must be specified if recording is enabled.

## Example Usage

Below are examples of how to define the configuration in both Python and Rust.

::::{tab-set}

::: {tab-item} Python

```python
import asyncio
from datetime import timedelta
from rustvello import Muse, Config, ClientType, TimestampResolution
from rustvello.proto import ElementKindRegistration, MetricDefinition

# Define the configuration
config = Config(
    endpoints=["http://localhost:8080"],
    client_type=ClientType.Poet,
    default_resolution=TimestampResolution.Milliseconds,
    element_kinds=[
        ElementKindRegistration("kind_code", "description")
    ],
    metric_definitions=[
        MetricDefinition("metric_code", "description")
    ],
    max_reg_elem_retries=3,
    max_endpoint_retries=None,
    recording_enabled=True,
    recording_path="recording.json",
    recording_flush_interval=timedelta(seconds=1),
    initialization_interval=timedelta(milliseconds=500),
    cluster_monitor_interval=timedelta(seconds=30),
)

# Initialize the Muse client
async def main():
    muse = Muse(config)
    await muse.initialize(timeout=5.0)
    # ... use the muse client

asyncio.run(main())
```

:::

::: {tab-item} Rust

```rust
use rustvello::prelude::*;
use rustvello::config::{ClientType, Config};
use rustvello_proto::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> MuseResult<()> {
    // Define the configuration
    let config = Config::new(
        vec!["http://localhost:8080".to_string()],
        ClientType::Poet,
        true,
        Some("recording.json".to_string()),
        Some(Duration::from_secs(1)),
        TimestampResolution::Milliseconds,
        vec![ElementKindRegistration::new("kind_code", "description")],
        vec![MetricDefinition::new("metric_code", "description")],
        Some(Duration::from_millis(500)),  // Initialization interval
        Some(Duration::from_secs(30)),    // Cluster monitor interval
        3,                                // Max retries for element registration
    )?;

    // Initialize the Muse client
    let mut muse = Muse::new(&config)?;
    muse.initialize(Some(Duration::from_secs(5))).await?;
    // ... use the muse client

    Ok(())
}
```

:::

::::

## Detailed Explanation

### Endpoints

A list of URLs where the Muse client can connect to. At least one endpoint must be provided when using the `Poet` client type.

### Client Type

Determines the type of client to instantiate:

- `Poet`: Communicates with the actual Muse system.
- `Mock`: Uses a mock client for testing purposes without making real network calls.

### Recording Options

- **Recording Enabled**: When set to `True` (Python) or `true` (Rust), the client will record events for later analysis.
- **Recording Path**: Specifies where the recorded events should be saved. This must be provided if recording is enabled.

### Default Resolution

Specifies the default timestamp resolution for metrics (e.g., milliseconds, seconds).

### Element Kinds

A list of `ElementKindRegistration` instances that define the kinds of elements you plan to register with the Muse system.

### Metric Definitions

A list of `MetricDefinition` instances that define the metrics you plan to report.

### Max Registration Retries

Specifies how many times the client should retry registering an element in case of failure.

### New Timing Fields

#### Recording Flush Interval

- Configures the frequency of writing recorded events to storage.
- Optional field. Defaults to one second if not specified.

#### Initialization Interval

- Defines the interval for performing initialization tasks, such as health checks and registrations.
- Optional field. Defaults to one second if not specified.

#### Cluster Monitor Interval

- Specifies the interval at which cluster monitoring tasks are performed.
- Optional field. Defaults to one minute if not specified.

## Validation Behavior (Rust Implementation)

The Rust implementation includes a `validate` method that ensures the configuration is correct before the client is initialized.

Validation checks include:

- **Client Type and Endpoints**: If the client type is `Poet`, there must be at least one endpoint specified.
- **Element Kinds and Metric Definitions**: Both must contain at least one entry.
- **Recording Path**: If recording is enabled, a valid recording path must be provided.

If any of these validations fail, the client initialization will return a `MuseError::Configuration` error.

## Common Errors

- **Missing Endpoints**: Ensure that you provide at least one endpoint when using the `Poet` client.
- **Empty Element Kinds or Metric Definitions**: Provide at least one element kind and one metric definition.
- **Recording Enabled Without Path**: If you enable recording, you must specify a valid path for the recording file.
