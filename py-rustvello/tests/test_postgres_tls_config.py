"""Public Python PostgreSQL option and TLS admission checks."""

import os
from pathlib import Path

import pytest

import rustvello.rustvello as rv
from rustvello import RustPostgresDatabase


def test_postgres_python_binding_rejects_invalid_complete_options_before_io() -> None:
    with pytest.raises(rv.ConfigurationError, match="options exceed"):
        RustPostgresDatabase(
            "host=127.0.0.1 port=1 user=unused",
            "python_tls",
            max_pool_size=0,
        )


def test_postgres_python_binding_requires_hostname_for_private_ca() -> None:
    with pytest.raises(ValueError, match="requires tls_hostname"):
        RustPostgresDatabase(
            "host=127.0.0.1 port=1 user=unused",
            "python_tls",
            tls_ca_pem=b"not-used",
        )


def test_postgres_python_binding_validates_tls_hostname_before_io() -> None:
    with pytest.raises(rv.ConfigurationError, match="invalid PostgreSQL TLS hostname"):
        RustPostgresDatabase(
            "host=127.0.0.1 port=1 user=unused",
            "python_tls",
            tls_hostname="bad hostname",
        )


@pytest.mark.skipif(
    "RUSTVELLO_POSTGRES_TLS_HOSTNAME" not in os.environ,
    reason="isolated LC-07-S fixture not configured",
)
def test_postgres_python_binding_connects_with_private_ca_and_all_options() -> None:
    RustPostgresDatabase(
        os.environ["RUSTVELLO_POSTGRES_DSN"],
        "python_tls_options",
        max_pool_size=3,
        operation_timeout_ms=1_500,
        delivery_lease_ms=7_000,
        max_queue_rows=321,
        max_payload_bytes=65_536,
        tls_hostname=os.environ["RUSTVELLO_POSTGRES_TLS_HOSTNAME"],
        tls_ca_pem=Path(os.environ["RUSTVELLO_POSTGRES_TLS_CA"]).read_bytes(),
    )
