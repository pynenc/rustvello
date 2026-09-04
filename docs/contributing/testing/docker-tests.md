# Docker-Based Tests

Backend tests for Redis, MongoDB, PostgreSQL, and RabbitMQ run against real
database instances managed by [testcontainers](https://testcontainers.com/).

## Prerequisites

- **Docker** must be running locally
- No manual container setup is needed — testcontainers handles image pull,
  startup, port mapping, and cleanup automatically

## How It Works

Each Docker-backed test file follows this pattern:

```rust
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

async fn make_broker() -> (testcontainers::ContainerAsync<Redis>, RedisBroker) {
    // 1. Start a fresh container
    let container = Redis::default().start().await.unwrap();

    // 2. Get the mapped port
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let uri = format!("redis://127.0.0.1:{port}/");

    // 3. Create the backend
    let broker = RedisBroker::new(RedisPool::new(&uri).unwrap());

    // 4. Return both — the container guard keeps Docker alive
    (container, broker)
}
```

The `ContainerAsync<T>` guard implements `Drop` — when it goes out of scope,
the container is stopped and removed. Each test function binds it as `_c` to
keep it alive for the test's duration:

```rust
#[tokio::test]
#[ignore = "requires Docker"]
async fn suite_broker_route_and_retrieve() {
    let (_c, broker) = make_broker().await;
    test_route_and_retrieve(&broker).await;
}
```

## Running Docker Tests

Docker tests are annotated `#[ignore = "requires Docker"]` and skipped by
default:

```bash
# Run only Docker tests for Redis:
cargo test -p rustvello-redis -- --ignored

# Run ALL Redis tests (unit + Docker):
cargo test -p rustvello-redis -- --include-ignored

# Run Docker tests across all backends:
cargo test --workspace --exclude py-rustvello --exclude rustvello-python -- --ignored

# Run a specific Docker test:
cargo test -p rustvello-redis -- --ignored suite_broker_route_and_retrieve
```

## Backend-Specific Notes

### Redis

- Image: `redis:latest` (via `testcontainers_modules::redis::Redis`)
- Default port: 6379
- Connection: `RedisPool::new(uri)` with `redis://127.0.0.1:{port}/`

### MongoDB (v3 driver)

- Image: `mongo:latest` (via `testcontainers_modules::mongo::Mongo`)
- Default port: 27017
- Connection: `MongoPool::new(uri)` with `mongodb://127.0.0.1:{port}/`

### MongoDB (v2 driver — mongo3)

- Image: `mongo:latest` (via `testcontainers_modules::mongo::Mongo`)
- Default port: 27017
- Connection: `Mongo3Pool::new(uri)` with `mongodb://127.0.0.1:{port}/`
- Uses optimistic CAS for status transitions (no transaction support)

### PostgreSQL

- Image: `postgres:latest` (via `testcontainers_modules::postgres::Postgres`)
- Default port: 5432
- Connection: `Database::connect(conn_string)` with libpq-style connection string
- Default credentials: `user=postgres password=postgres dbname=postgres`
- Auto-migration: The `Database::connect()` method runs schema migrations on
  first connection

### RabbitMQ

- Image: `rabbitmq:latest` (via `testcontainers_modules::rabbitmq::RabbitMq`)
- Default port: 5672
- Only `Broker` trait is tested (RabbitMQ is a message queue, not a database)

## Test Isolation

Each test function starts its own container, so tests are fully isolated and can
run in parallel. This means:

- **No shared state** between tests
- **No cleanup needed** — containers are ephemeral
- **Slower than in-memory** — container startup adds ~1-3 seconds per test

For faster iteration during development, consider running in-memory backend
tests first:

```bash
cargo test -p rustvello-mem   # Fast: in-memory, no Docker
cargo test -p rustvello-redis -- --ignored  # Slower: real Redis
```

## Troubleshooting

### "Cannot connect to the Docker daemon"

Ensure Docker Desktop or `dockerd` is running:

```bash
docker info
```

### Tests hang or time out

Testcontainers waits for the container to be "ready" (port open, health check
passing). If a container image is being pulled for the first time, it may take
longer. Pre-pull images to avoid timeout:

```bash
docker pull redis:latest
docker pull mongo:latest
docker pull postgres:latest
docker pull rabbitmq:latest
```

### Port conflicts

Testcontainers maps container ports to random host ports, so port conflicts
should not occur. If they do, check for stale containers:

```bash
docker ps -a | grep testcontainers
docker container prune
```
