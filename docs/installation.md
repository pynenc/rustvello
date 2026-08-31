# Installation

Rustvello is available as a Rust crate, a Python wheel (`py-rustvello`), and a CLI binary.

---

## Rust Crate

Add the main library crate to your `Cargo.toml`:

```toml
[dependencies]
rustvello = "0.3.1"
```

Or via Cargo:

```bash
cargo add rustvello
```

The default build includes only the in-memory backend. Enable production backends with
feature flags (see below).

### Feature Flags

| Feature      | Description                                  | Default |
| ------------ | -------------------------------------------- | ------- |
| `mem`        | In-memory backend (dev/testing)              | **Yes** |
| `sqlite`     | SQLite-backed persistent backend             | No      |
| `redis`      | Redis-backed distributed backend             | No      |
| `mongodb`    | MongoDB-backed backend (driver v3)           | No      |
| `mongodb3`   | MongoDB-backed backend (driver v2 — legacy)  | No      |
| `rabbitmq`   | RabbitMQ broker                              | No      |
| `postgres`   | PostgreSQL trigger store                     | No      |
| `prometheus` | Prometheus metrics exporter                  | No      |
| `rayon`      | Rayon thread-pool runner for CPU-bound tasks | No      |
| `full`       | All backends enabled                         | No      |

Examples:

```toml
# SQLite for single-host persistence
rustvello = { version = "0.3.1", features = ["sqlite"] }

# Redis for distributed production
rustvello = { version = "0.3.1", features = ["redis"] }

# Everything
rustvello = { version = "0.3.1", features = ["full"] }
```

---

## Individual Crates

For finer-grained control, depend on individual crates directly:

| Crate                  | Description                                            |
| ---------------------- | ------------------------------------------------------ |
| `rustvello-proto`      | DTOs, identifiers, status FSM, config types            |
| `rustvello-core`       | Core traits (`Broker`, `Orchestrator`, `StateBackend`) |
| `rustvello-macros`     | `#[rustvello::task]` proc macro                        |
| `rustvello-mem`        | In-memory backend implementations                      |
| `rustvello-sqlite`     | SQLite backend implementations                         |
| `rustvello-redis`      | Redis backend implementations                          |
| `rustvello-mongo`      | MongoDB backend implementations (driver v3)            |
| `rustvello-mongo3`     | MongoDB backend implementations (driver v2 — legacy)   |
| `rustvello-rabbitmq`   | RabbitMQ broker implementation                         |
| `rustvello-postgres`   | PostgreSQL trigger store implementation                |
| `rustvello-prometheus` | Prometheus `EventEmitter` implementation               |
| `rustvello-monitoring` | Axum web dashboard (SVG timelines, log explorer)       |
| `rustvello-test-suite` | Macro-generated backend compliance test suite          |
| `rustvello-python`     | PyO3 `#[pyclass]` wrappers (Rust → Python bridge)      |
| `rustvello-cli`        | CLI binary (`rustvello` command)                       |

---

## Python (`rustvello` wheel)

The `rustvello` Python package ships pre-compiled wheels (Python 3.12+) and exposes
the Rust backends to Python. It can be used standalone or as a backend for
[pynenc](https://docs.pynenc.org).

Pynenc does **not** depend on rustvello automatically. The base install is pure Python:

```bash
pip install pynenc
```

Add rustvello to get the Rust-powered backends (mem, sqlite, redis, postgres, mongodb,
mongodb3, rabbitmq — all in one wheel):

```bash
pip install pynenc pynenc-rustvello
```

Then select a backend with `PynencBuilder`:

```python
from pynenc import PynencBuilder

# In-memory (dev / testing)
app = PynencBuilder().app_id("my-app").rustvello(backend="mem").build()

# SQLite (single-node persistent)
app = PynencBuilder().app_id("my-app").rustvello(backend="sqlite").build()

# Redis (distributed)
app = PynencBuilder().app_id("my-app").rustvello(
    backend="redis", redis_url="redis://localhost:6379"
).build()
```

### Backend Selection Matrix

| Use Case               | `backend=`                | Persistence | Distributed |
| ---------------------- | ------------------------- | ----------- | ----------- |
| Development            | `"mem"` (default)         | No          | No          |
| Single-host persistent | `"sqlite"`                | Yes         | No          |
| Distributed production | `"redis"` or `"postgres"` | Yes         | Yes         |
| Document-oriented      | `"mongo"`                 | Yes         | Yes         |
| Document-oriented (v2) | `"mongo3"`                | Yes         | Yes         |

### Direct rustvello Install

If you need the Rust bindings without the pynenc SDK:

```bash
pip install rustvello
```

### Building from source

If pre-built wheels are unavailable for your platform:

```bash
# Install maturin
pip install maturin

# Build and install from source
cd py-rustvello
maturin develop --release
```

---

## CLI

The `rustvello` CLI is installed separately:

```bash
cargo install rustvello-cli
```

This places the `rustvello` binary on your `PATH`. See {doc}`cli/index` for usage.

---

## Monitoring Dashboard

The monitoring dashboard is part of `rustvello-monitoring` and is embedded in binaries
that include it. No separate installation is needed — enable the `monitoring` feature
or depend on `rustvello-monitoring` directly, then call `start_monitor()` from your
application code. See {doc}`monitoring/index` for details.

---

## Development Setup

For contributing or building from source, see the {doc}`contributing/index` guide.

```bash
git clone https://github.com/pynenc/rustvello.git
cd rustvello
make install
```
