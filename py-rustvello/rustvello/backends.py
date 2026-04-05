"""Backend factory for the standalone App.

Maps backend name strings (``"memory"``, ``"sqlite"``, ``"redis"``,
``"postgres"``, ``"mongo"``) to the corresponding PyO3 component objects.
"""

from __future__ import annotations

from typing import Any

_BACKEND_NAMES = {"memory", "sqlite", "redis", "postgres", "mongo"}


def create_backends(
    backend: str,
    app_id: str,
    *,
    db_path: str = "",
    redis_url: str = "",
    postgres_url: str = "",
    mongo_url: str = "",
    mongo_db: str = "",
) -> dict[str, Any]:
    """Instantiate backend component objects for the given backend type.

    Returns a dict with keys:
    ``orchestrator``, ``state_backend``, ``broker``, ``trigger``,
    ``client_data_store``.

    Raises:
        ValueError: If *backend* is not a recognised name.
    """
    if backend not in _BACKEND_NAMES:
        raise ValueError(f"Unknown backend {backend!r}. " f"Choose from: {', '.join(sorted(_BACKEND_NAMES))}")

    if backend == "sqlite":
        from rustvello.rustvello import (
            RustSqliteBroker,
            RustSqliteClientDataStore,
            RustSqliteDatabase,
            RustSqliteOrchestrator,
            RustSqliteStateBackend,
            RustSqliteTriggerStore,
        )

        db = RustSqliteDatabase(db_path, app_id)
        return {
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

    # "memory" — should never reach here (handled by caller), but be safe
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
