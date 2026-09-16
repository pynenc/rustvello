"""Backend factory for the standalone App.

Maps backend name strings (``"memory"``, ``"sqlite"``, ``"redis"``,
``"postgres"``, ``"mongo"``, ``"mongo3"``) to the corresponding PyO3 component
objects. ``broker="rabbitmq"`` swaps the broker for the RabbitMQ transport.
"""

from __future__ import annotations

from typing import Any

_BACKEND_NAMES = {"memory", "sqlite", "redis", "postgres", "mongo", "mongo3"}
_BROKER_NAMES = {"rabbitmq"}


def create_backends(
    backend: str,
    app_id: str,
    *,
    db_path: str = "",
    sqlite_synchronous: str = "FULL",
    sqlite_busy_timeout_ms: int = 5000,
    redis_url: str = "",
    postgres_url: str = "",
    mongo_url: str = "",
    mongo_db: str = "",
    broker: str | None = None,
    rabbitmq_url: str = "",
    rabbitmq_prefix: str = "",
) -> dict[str, Any]:
    """Instantiate backend component objects for the given backend type.

    Returns a dict with keys:
    ``orchestrator``, ``state_backend``, ``broker``, ``trigger``,
    ``client_data_store``.

    ``broker`` overrides only the broker component (``"rabbitmq"``), keeping the
    other components on *backend*.

    Raises:
        ValueError: If *backend* or *broker* is not a recognised name.
    """
    if backend not in _BACKEND_NAMES:
        raise ValueError(f"Unknown backend {backend!r}. Choose from: {', '.join(sorted(_BACKEND_NAMES))}")
    if broker is not None and broker not in _BROKER_NAMES:
        raise ValueError(f"Unknown broker {broker!r}. Choose from: {', '.join(sorted(_BROKER_NAMES))}")
    backends = _create_component_backends(
        backend,
        app_id,
        db_path=db_path,
        sqlite_synchronous=sqlite_synchronous,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        redis_url=redis_url,
        postgres_url=postgres_url,
        mongo_url=mongo_url,
        mongo_db=mongo_db,
    )
    if broker == "rabbitmq":
        from rustvello.rustvello import RustRabbitmqBroker

        backends["broker"] = RustRabbitmqBroker(rabbitmq_url, rabbitmq_prefix or app_id)
    return backends


def _create_component_backends(
    backend: str,
    app_id: str,
    *,
    db_path: str,
    sqlite_synchronous: str,
    sqlite_busy_timeout_ms: int,
    redis_url: str,
    postgres_url: str,
    mongo_url: str,
    mongo_db: str,
) -> dict[str, Any]:
    if backend == "sqlite":
        from rustvello.rustvello import (
            RustSqliteBroker,
            RustSqliteClientDataStore,
            RustSqliteDatabase,
            RustSqliteOrchestrator,
            RustSqliteStateBackend,
            RustSqliteTriggerStore,
        )

        db = RustSqliteDatabase(db_path, app_id, synchronous=sqlite_synchronous, busy_timeout_ms=sqlite_busy_timeout_ms)
        return {
            "database": db,
            "orchestrator": RustSqliteOrchestrator(db),
            "state_backend": RustSqliteStateBackend(db),
            "broker": RustSqliteBroker(db),
            "trigger": RustSqliteTriggerStore(db),
            "client_data_store": RustSqliteClientDataStore(db),
        }

    if backend == "redis":
        from rustvello.rustvello import (
            RustRedisBroker,
            RustRedisClientDataStore,
            RustRedisOrchestrator,
            RustRedisPool,
            RustRedisStateBackend,
            RustRedisTriggerStore,
        )

        pool = RustRedisPool(redis_url, app_id)
        return {
            "orchestrator": RustRedisOrchestrator(pool),
            "state_backend": RustRedisStateBackend(pool),
            "broker": RustRedisBroker(pool),
            "trigger": RustRedisTriggerStore(pool),
            "client_data_store": RustRedisClientDataStore(pool),
        }

    if backend == "postgres":
        from rustvello.rustvello import (
            RustPostgresBroker,
            RustPostgresClientDataStore,
            RustPostgresDatabase,
            RustPostgresOrchestrator,
            RustPostgresStateBackend,
            RustPostgresTriggerStore,
        )

        pg_db = RustPostgresDatabase(postgres_url, app_id)
        return {
            "orchestrator": RustPostgresOrchestrator(pg_db),
            "state_backend": RustPostgresStateBackend(pg_db),
            "broker": RustPostgresBroker(pg_db),
            "trigger": RustPostgresTriggerStore(pg_db),
            "client_data_store": RustPostgresClientDataStore(pg_db),
        }

    if backend == "mongo":
        from rustvello.rustvello import (
            RustMongoBroker,
            RustMongoClientDataStore,
            RustMongoOrchestrator,
            RustMongoPool,
            RustMongoStateBackend,
            RustMongoTriggerStore,
        )

        mongo_pool = RustMongoPool(mongo_url, mongo_db, app_id)
        return {
            "orchestrator": RustMongoOrchestrator(mongo_pool),
            "state_backend": RustMongoStateBackend(mongo_pool),
            "broker": RustMongoBroker(mongo_pool),
            "trigger": RustMongoTriggerStore(mongo_pool),
            "client_data_store": RustMongoClientDataStore(mongo_pool),
        }

    if backend == "mongo3":
        from rustvello.rustvello import (
            RustMongo3Broker,
            RustMongo3ClientDataStore,
            RustMongo3Orchestrator,
            RustMongo3Pool,
            RustMongo3StateBackend,
            RustMongo3TriggerStore,
        )

        # MongoDB 3.6+ through the legacy (v2) driver
        mongo3_pool = RustMongo3Pool(mongo_url, mongo_db, app_id)
        return {
            "orchestrator": RustMongo3Orchestrator(mongo3_pool),
            "state_backend": RustMongo3StateBackend(mongo3_pool),
            "broker": RustMongo3Broker(mongo3_pool),
            "trigger": RustMongo3TriggerStore(mongo3_pool),
            "client_data_store": RustMongo3ClientDataStore(mongo3_pool),
        }

    # "memory"
    from rustvello.rustvello import (
        RustMemBroker,
        RustMemClientDataStore,
        RustMemOrchestrator,
        RustMemStateBackend,
        RustMemTriggerStore,
    )

    return {
        "orchestrator": RustMemOrchestrator(),
        "state_backend": RustMemStateBackend(),
        "broker": RustMemBroker(),
        "trigger": RustMemTriggerStore(),
        "client_data_store": RustMemClientDataStore(),
    }
