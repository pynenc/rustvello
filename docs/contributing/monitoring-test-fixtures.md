# Monitoring Test Fixtures

These commands keep the dashboard fixtures easy to refresh while monitoring
changes are under review. Most tests shut the server down automatically; add
`KEEP_ALIVE=1` when you want to open the printed URL in a browser.

`-- --nocapture` is the Rust test harness flag that shows stdout and stderr
from the test. It does not hide logs. `RUST_LOG` decides which tracing events
are emitted.

## Cross-Language Timeline Fixture

Small fixture for validating task language, runner language, executor badges,
status history, family tree links, and timeline rendering.

```bash
KEEP_ALIVE=1 RUST_LOG=rustvello=debug,rustvello_monitoring=debug \
  cargo test -p rustvello-monitoring --test monitoring_dashboard \
  test_timeline_renders_complete_invocation_history -- --nocapture
```

Open the printed `/invocations/timeline` URL for the SVG timeline and `/logs`
for the Log Explorer.

## Large Cross-Language Load Fixture

Generates several hundred Rust and Python-language workflow roots through
triggers and direct submissions. The roots call across language boundaries,
route CPU work to dedicated Rayon queues, route IO-like Python work to IO
queues, and run multiple Rust/Python runner groups with different concurrency
levels. The test also emits live trigger events after the runners start and
waits for at least one atomic-service execution, so `/atomic-service` and Log
Explorer have real management-loop data to inspect.

```bash
make monitoring-load
```

Equivalent direct command:

```bash
KEEP_ALIVE=1 RUST_LOG=rustvello=debug,rustvello_monitoring=debug \
  cargo test -p rustvello-monitoring --test monitoring_load \
  monitoring_cross_language_load_fixture -- --ignored --nocapture
```

Open the printed `time_range=auto` timeline URL first; it zooms around the
generated load instead of showing a mostly empty recent-time window.

Use this fixture when improving:

- timeline lane layout with Rust, Python, Tokio, and Rayon workers
- task occupancy by runtime, active-worker lines, and legend interactions
- Log Explorer parsing and cross-highlighting
- trigger-run and event detail navigation
- workflow and family-tree views with mixed-language children
- atomic-service execution visibility

## Focused Backend And Workflow Commands

Cross-language contract tests without monitoring:

```bash
cargo test -p rustvello --test cross_language_tests -- --nocapture
```

Runner lifecycle, heartbeat, shutdown, and recovery behavior:

```bash
cargo test -p rustvello --test runner_hardening_tests -- --nocapture
```

Workflow identity propagation and child invocation tracking:

```bash
cargo test -p rustvello --test workflow_tests -- --nocapture
```

Trigger scheduling and evidence storage in the shared test suite:

```bash
cargo test -p rustvello --test trigger_tests -- --nocapture
cargo test -p rustvello-mem trigger -- --nocapture
```

RabbitMQ language-queue routing with a real broker:

```bash
cargo test -p rustvello-rabbitmq --test suite language -- --ignored --nocapture
```

Run full dashboard coverage before shipping monitoring changes:

```bash
cargo test -p rustvello-monitoring --test monitoring_dashboard -- --nocapture
```
