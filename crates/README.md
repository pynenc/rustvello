# rustvello Project Structure

```sh
rustvello/
│
├── Cargo.toml                  # Workspace configuration
│
├── crates/
│   ├── rustvello-core/           # Core functionality and traits
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs          # Main library definitions
│   │       ├── recording.rs    # Recording context implementation
│   │       ├── cache.rs        # Caching strategies
│   │       └── error.rs        # Error handling
│   │
│   ├── rustvello-client/         # Poet API client
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── poet_client.rs  # API communication
│   │       └── rate_limiter.rs # Rate limiting logic
│   │
│   ├── rustvello-proto/          # Serialization and protocol
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── metric.rs       # Metric payload definitions
│   │       └── serialization.rs
│   │
│   ├── rustvello-cli/            # Command-line interface
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs         # CLI command implementations
│   │
│   └── rustvello/                # Main library interface
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs          # High-level public API
│
├── py-rustvello/                 # Python bindings
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs              # Python FFI layer
│
└── Cargo.lock
```

## Integration Workflow

1. **Core Functionality (`rustvello-core`)**

   - Defines fundamental traits
   - Provides recording mechanism
   - Implements caching strategies

2. **Client Implementation (`rustvello-client`)**

   - Uses proto types and messages and core traits
   - Handles API communication
   - Implements rate limiting

3. **Protocol Definition (`rustvello-proto`)**

   - Provides serialization
   - Defines data structures and types
   - Ensures consistent data representation

4. **Main Library (`rustvello`)**

   - Coordinates between core, client, and proto
   - Provides high-level public API
   - Manages configuration

5. **CLI Tool (`rustvello-cli`)**

   - Uses core recording mechanisms
   - Provides replay and recording commands

6. **Python Bindings (`py-rustvello`)**

   - Wraps Rust implementation
   - Provides Python-friendly interface

7. **Record Functionalitiy (`rustvello-record`)**
   - Records events (calls to the library in Json or bin)
   - Replays recorded files

## Sample Usage Scenario

```rust
// In a library using rustvello (e.g., k8s metrics collector)
use rustvello::{Muse, RecordingContext};

fn collect_k8s_metrics() {
    // Create Muse instance with configuration
    let muse = Muse::new(&config).expect("Failed to create the Muse");

    // Start recording session
    let _recording = RecordingContext::start_recording();

    // Collect and send metrics
    muse.send_metric("node1", "cpu_usage", 75.5);
    muse.send_metric("node1", "memory_usage", 60.3);

    // Recording automatically saved when context is dropped
}
```

## CLI Workflow

```bash
# Start a recording session
rustvello record --start --output k8s_metrics.json

# Replay a recorded session
rustvello replay k8s_metrics.json --poet-url http://localhost:8080
```
