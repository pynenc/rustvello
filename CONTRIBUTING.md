# Contributing to Rustvello

Contributions are welcome! Every little bit helps, and credit will always be given.

---

## Repository Overview

Rustvello is the **Rust core** of the [pynenc](https://github.com/pynenc/pynenc) distributed
task orchestration framework. It implements:

- **Task registration** and compile-time auto-discovery via proc macros
- **Invocation lifecycle** with an 11-state finite state machine
- **Broker queueing**, **orchestration**, and **concurrency control**
- **Workflow tracking** with parent-child invocation chains
- **Trigger system** (cron, status, event, result, exception conditions)
- **Recovery & heartbeat** for crashed runner detection
- **Monitoring dashboard** (Axum + Askama + HTMX)
- **Python bindings** via PyO3 so pynenc can use Rust backends as drop-in replacements

---

## Workspace Structure

This is a **multi-crate Rust workspace** with Python bindings:

```text
rustvello/
├── Cargo.toml                 # Workspace root
├── Makefile                   # Top-level targets (install, check, test, build, docs)
├── crates/
│   ├── rustvello-proto/       # Pure data types (DTOs, identifiers, config, status FSM)
│   ├── rustvello-core/        # Trait definitions + business logic managers
│   ├── rustvello-mem/         # In-memory backend implementations (dev/testing)
│   ├── rustvello-sqlite/      # SQLite backend implementations (single-node)
│   ├── rustvello-redis/       # Redis backend (WIP)
│   ├── rustvello-postgres/    # PostgreSQL backend (WIP)
│   ├── rustvello-mongo/       # MongoDB backend (WIP)
│   ├── rustvello-rabbitmq/    # RabbitMQ backend (WIP)
│   ├── rustvello-prometheus/  # Prometheus metrics exporter
│   ├── rustvello-macros/      # #[rustvello::task] proc-macro
│   ├── rustvello/             # Main library (app, builder, runner, trigger builder)
│   ├── rustvello-cli/         # CLI binary (run, status, list, purge, info)
│   ├── rustvello-monitoring/  # Web monitoring dashboard (Axum + Askama + HTMX)
│   ├── rustvello-python/      # PyO3 #[pyclass] wrappers
│   ├── rustvello-test-suite/  # Shared backend compliance tests (macro-based)
│   └── Makefile               # Rust-specific targets (fmt, clippy, bench, deny)
├── py-rustvello/              # Python wheel (cdylib + pynenc bridge classes)
├── examples/
│   └── monitoring_server/     # Run a monitoring server for browser debugging
├── docs/                      # Sphinx documentation
└── .github/                   # CI/CD workflows
```

### Crate Dependency Order

```text
rustvello-proto → rustvello-core → rustvello-mem / rustvello-sqlite / ...
                                 → rustvello-macros
                                 → rustvello (main library)
                                 → rustvello-monitoring
                                 → rustvello-cli
                                 → rustvello-python → py-rustvello
```

### Feature Flags

| Flag            | Description          |
| --------------- | -------------------- |
| `mem` (default) | In-memory backends   |
| `sqlite`        | SQLite backends      |
| `redis`         | Redis backends       |
| `mongodb`       | MongoDB backends     |
| `rabbitmq`      | RabbitMQ backends    |
| `prometheus`    | Prometheus metrics   |
| `postgres`      | PostgreSQL backends  |
| `full`          | All backends enabled |

---

## Development Setup

### Prerequisites

- **Rust 1.85+** (`rustup` recommended)
- **Python 3.9+**
- **[uv](https://docs.astral.sh/uv/)** — Python package manager
- **[maturin](https://www.maturin.rs/)** — for building PyO3 wheels
- **Node.js** — for commitlint (installed via pre-commit)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/pynenc/rustvello.git
cd rustvello

# Install all dependencies, build the Python extension, and set up pre-commit hooks
make install

# Run the full quality + test suite
make check && make test
```

### Make Targets (Root)

| Target             | Description                                                        |
| ------------------ | ------------------------------------------------------------------ |
| `make install`     | Install deps, build Python extension, setup pre-commit             |
| `make check`       | Run all linting via pre-commit (clippy, rustfmt, ruff, mypy, etc.) |
| `make test`        | Run Rust + Python tests                                            |
| `make test-rust`   | Run Rust tests only                                                |
| `make test-python` | Run Python tests only (pytest)                                     |
| `make build`       | Build Python wheel + sdist                                         |
| `make build-rust`  | Build all Rust crates (excludes py-rustvello)                      |
| `make develop`     | Build and install py-rustvello in develop mode                     |
| `make docs-serve`  | Build and serve documentation locally at `http://localhost:8000`   |

### Make Targets (crates/)

| Target                  | Description                        |
| ----------------------- | ---------------------------------- |
| `make -C crates fmt`    | Run `cargo fmt`                    |
| `make -C crates clippy` | Run clippy with all features       |
| `make -C crates test`   | Run Rust tests (2 threads)         |
| `make -C crates bench`  | Run Criterion benchmarks           |
| `make -C crates deny`   | Run cargo-deny supply-chain checks |
| `make -C crates semver` | Check semver compatibility         |

---

## Coding Guidelines

### Rust

- **MSRV**: 1.85, edition 2021
- **Formatting**: `cargo fmt --all` — enforced by pre-commit
- **Linting**: `cargo clippy --all-targets -- -D warnings` — no warnings allowed
- **Unsafe code**: denied workspace-wide (`unsafe_code = "deny"` in workspace lints)
- **Error handling**: use `RustvelloResult<T>` / `RustvelloError` — never `unwrap()` in library code
- **Async**: all backend traits use `#[async_trait]` with `Arc<dyn Trait>` dispatch
- **Tests**: `#[tokio::test]` for async tests, placed in `#[cfg(test)] mod tests` or dedicated test files under `tests/`
- **New tasks**: use `#[rustvello::task]` proc macro for compile-time registration

### Python

- **Formatting**: ruff format — enforced by pre-commit
- **Linting**: ruff + mypy — enforced by pre-commit
- **Style**: follow existing pynenc conventions

### General

- Keep PRs focused on a single concern
- Add tests for all new functionality
- Update documentation for user-facing changes

---

## Running Tests

### Rust Tests

```bash
# All Rust tests (default features)
cargo test --workspace --exclude rustvello-python --exclude py-rustvello

# All Rust tests (all features)
cargo test -p rustvello --all-features

# Specific crate
cargo test -p rustvello-core
cargo test -p rustvello-mem
cargo test -p rustvello-monitoring

# Shared test suite (backend compliance)
cargo test -p rustvello-mem --test suite
cargo test -p rustvello-sqlite --test suite
```

### Python Tests

```bash
# Requires building the Python extension first
make develop

# Run all Python tests
uv run pytest py-rustvello/ --cov

# Run pynenc unit tests
uv run pytest pynenc_tests/
```

### Both

```bash
make test
```

---

## Monitoring Dashboard: Running a Live Server for Debugging

The `rustvello-monitoring` crate provides a browser-based dashboard for inspecting
invocations, runners, workflows, and task timelines. To run it with test data:

```bash
# Run the monitoring server example
cargo run -p monitoring-server-example
```

This starts:

- A **TaskRunner** processing invocations in the background
- The **monitoring dashboard** at `http://127.0.0.1:8000`

Open your browser to explore the dashboard. Press `Ctrl-C` to stop.

### How It Works

The example in `examples/monitoring_server/` demonstrates:

1. Building a `RustvelloApp` with in-memory backends
2. Registering tasks and seeding test invocations
3. Constructing `AppInstance` from the app's backends
4. Starting `start_monitor()` alongside a `TaskRunner`

You can modify the example to test specific scenarios (concurrency control, triggers,
workflow patterns, etc.) and observe them in the dashboard.

### Custom Monitoring Setup

```rust
use rustvello_monitoring::{AppInstance, MonitorConfig, start_monitor};

// Build AppInstance from your app's backends
let instance = AppInstance {
    app_id: "my-app".to_string(),
    config: app_config,
    broker: app.broker(),
    orchestrator: app.orchestrator(),
    state_backend: app.state_backend(),
    client_data_store: app.client_data_store(),
    task_ids: vec![/* your task IDs */],
};

let mut apps = HashMap::new();
apps.insert(instance.app_id.clone(), instance);

// Start the monitoring server (blocks)
start_monitor(apps, "my-app", MonitorConfig::default()).await?;
```

### Monitoring Integration Tests

The `rustvello-monitoring` crate has a full integration test suite that starts a
**real HTTP server** on a free port, seeds data into in-memory backends, and makes
HTTP requests to every dashboard page. This mirrors the pynmon integration test
pattern.

#### Running the tests

```bash
# Run all monitoring integration tests
cargo test -p rustvello-monitoring --test monitoring_dashboard

# Run a specific test
cargo test -p rustvello-monitoring --test monitoring_dashboard test_invocations_timeline
```

#### Hierarchical timeline test

The `test_hierarchical_timeline` test is the most comprehensive monitoring test.
It mirrors pynmon's `test_invocations_timeline_multi_runner.py`, exercising:

- **3 hierarchical tasks**: `grandparent_task` → `parent_task` → `child_task`
- **2 concurrent `TaskRunner` instances** sharing the same in-memory backends
- **51 invocations** matching pynmon's test structure:
  familyA(2) + familyB(3) + familyC(4) + familyD(1) + familyE(2)
  = 5 grandparents + 12 parents + 34 children
- **Full dashboard verification**: timeline SVG, invocation list filters,
  detail pages with history (Registered → Running → Success), family tree,
  workflow depth, `parent_invocation_id`, and `runner_id` in history entries

This is the best test for validating the monitoring dashboard end-to-end:

```bash
# Run the hierarchical test
cargo test -p rustvello-monitoring \
    --test monitoring_dashboard test_hierarchical_timeline

# Run with KEEP_ALIVE to explore in a browser
RUSTVELLO_MONITOR_KEEP_ALIVE=1 cargo test -p rustvello-monitoring \
    --test monitoring_dashboard test_hierarchical_timeline -- --nocapture
```

When running with `KEEP_ALIVE`, the test will print the server URL
(e.g. `http://127.0.0.1:52861`). Open it in your browser to explore:

- **Invocations** — filter by task type (`grandparent_task`, `parent_task`,
  `child_task`), see status badges, retries, call IDs
- **Timeline** — SVG with lanes for each runner, cross-highlight on hover,
  click any invocation to see inline details
- **Detail pages** — full invocation info with status timeline showing
  runner IDs, parent invocation links, arguments, family tree
- **Family tree** — click a grandparent to see the full parent → child chain

#### Browser debugging with KEEP_ALIVE

When developing or debugging the dashboard, you can keep the monitoring server
running after the test completes so you can explore it in your browser. There are
two ways:

**1. Environment variable (recommended for ad-hoc debugging):**

```bash
RUSTVELLO_MONITOR_KEEP_ALIVE=1 cargo test -p rustvello-monitoring \
    --test monitoring_dashboard test_invocations_timeline -- --nocapture
```

**2. Source constant (for sustained development):**

Edit `crates/rustvello-monitoring/tests/monitoring_dashboard.rs` and set:

```rust
const KEEP_ALIVE: bool = true;
```

In both cases the test will print the server URL (e.g. `http://127.0.0.1:52861`)
and block until you press Ctrl-C. Open the URL in your browser to explore the
dashboard with real test data.

#### Architecture

The test infrastructure lives in two files:

| File                            | Purpose                                                                                                                                                                                              |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/common/mod.rs`           | Shared helpers: `create_test_app()`, `seed_invocations()`, `create_hierarchical_test_app()`, `seed_hierarchical_invocations()`, `submit_with_parent()`, `start_test_server()`, `should_keep_alive()` |
| `tests/monitoring_dashboard.rs` | 20 integration tests covering every dashboard route                                                                                                                                                  |

Key design decisions that mirror pynmon's test approach:

- **Free port discovery** — binds to `127.0.0.1:0` so the OS assigns a random
  free port. Tests never collide, even when running in parallel.
- **Real HTTP server** — starts a genuine Axum server (not `tower::ServiceExt::oneshot`),
  so the tests exercise the full middleware stack including CSRF validation, body
  limits, and static file serving.
- **In-memory backends** — uses `rustvello-mem` for zero-setup, fast tests.
  No database files to clean up.
- **Shared `Arc` backends** — the runner and monitoring server share the same
  `Arc<dyn Broker>`, `Arc<dyn StateBackend>`, etc., so data seeded via `app.submit()`
  is immediately visible in the dashboard.
- **KEEP_ALIVE** — identical to pynmon's debugging pattern, controlled by both a
  source constant and an environment variable.

#### Writing new monitoring tests

1. Use `create_test_app("unique-name")` to build an in-memory app with a sample task.
2. Optionally call `seed_invocations(&setup.app, n)` to populate test data.
3. Call `start_test_server(setup)` to get a `TestServer` with a `.url` field.
4. Make HTTP requests with `reqwest::Client`.
5. Call `handle_keep_alive(server)` at the end (or `server.shutdown()`).

```rust
#[tokio::test]
async fn test_my_new_feature() {
    let setup = create_test_app("test-my-feature");
    seed_invocations(&setup.app, 3).await.unwrap();
    let server = start_test_server(setup).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/my-endpoint", server.url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    handle_keep_alive(server).await;
}
```

---

## Commit and PR Guidelines

We use **Conventional Commits** for all commit messages and PR titles.

### Format

```text
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type       | Description                                             |
| ---------- | ------------------------------------------------------- |
| `feat`     | New features                                            |
| `fix`      | Bug fixes                                               |
| `docs`     | Documentation only changes                              |
| `style`    | Formatting, no code change                              |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf`     | Performance improvements                                |
| `test`     | Adding or correcting tests                              |
| `build`    | Build system or dependency changes                      |
| `ci`       | CI configuration changes                                |
| `chore`    | Maintenance tasks                                       |
| `revert`   | Revert a previous commit                                |

### Examples

```text
feat(broker): add language-aware invocation routing
fix(monitoring): escape status names in SVG tooltips
docs(readme): update installation instructions
refactor(core): consolidate error handling in orchestrator
test(suite): add shared backend compliance tests
```

### Pre-commit Hooks

Pre-commit is configured to run automatically on commit. The hooks include:

- **Rust**: `cargo fmt`, `cargo clippy`
- **Python**: ruff lint, ruff format, mypy
- **Markdown**: markdownlint
- **Commit messages**: commitlint (conventional commits)
- **General**: trailing whitespace, end-of-file fixer, TOML/YAML validation, typos

To install the commit-msg hook (required for commitlint):

```bash
uv run pre-commit install --hook-type commit-msg
```

---

## Workflow

1. **Fork** the repo on GitHub
2. **Clone** your fork: `git clone git@github.com:YOUR_NAME/rustvello.git`
3. **Navigate**: `cd rustvello`
4. **Install**: `make install`
5. **Branch**: `git checkout -b feat/my-feature` (use conventional commit prefix)
6. Make changes
7. **Run checks**: `make check`
8. **Run tests**: `make test`
9. **Commit**: `git commit -m "feat(scope): description"`
10. **Push**: `git push origin feat/my-feature`
11. **Submit a PR** on GitHub — title must follow conventional commit format

---

## Pull Request Guidelines

1. **Follow Conventional Commit format** for PR title
2. **Include tests** for new functionality
3. **Update documentation** for user-facing changes
4. **Run `make check && make test`** before submitting
5. **Link related issues** using `Closes #123` or `Fixes #456`
6. **Apply appropriate labels** (SemVer: major/minor/patch + type: feature/fix/etc.)

---

## Labels

Labels are automatically synced from `.github/labels.yml` and auto-applied based on PR
titles. Key labels:

| Category   | Labels                                                                                |
| ---------- | ------------------------------------------------------------------------------------- |
| **SemVer** | `major`, `minor`, `patch`                                                             |
| **Type**   | `feature`, `fix`, `docs`, `chore`, `refactor`, `security`, `dependencies`, `breaking` |
| **Other**  | `testing`, `deprecated`, `removed`, `skip-changelog`                                  |

---

## Types of Contributions

### Report Bugs

Report bugs at <https://github.com/pynenc/rustvello/issues>. Include:

- Operating system and version
- Rust and Python versions (`rustc --version`, `python --version`)
- Steps to reproduce the bug

### Fix Bugs

Look through GitHub issues tagged "bug" + "help wanted".

### Implement Features

Look through GitHub issues tagged "enhancement" + "help wanted".

### Write Documentation

Documentation improvements are always welcome — rustdoc comments, examples, guides.
