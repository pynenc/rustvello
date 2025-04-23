"""Tests for Muse initialization timeout."""

import pytest

from common import get_client_type_from_env
from rustvello import (
    Config,
    ElementKindRegistration,
    MetricDefinition,
    Muse,
    TimestampResolution,
)
from rustvello.exceptions import MuseInitializationTimeoutError


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.integration
async def test_muse_initialization_timeout() -> None:
    """Test Muse initialization timeout behavior."""
    element_kind = ElementKindRegistration(
        "server",
        "Server",
        "A server element kind",
    )
    metric_definition = MetricDefinition(
        "cpu_usage",
        "CPU Usage",
        "The CPU usage of a server",
    )

    # Create config with unreachable endpoint to test timeout
    config = Config(
        endpoints=["http://unreachable:9999"],
        client_type=get_client_type_from_env(),
        recording_enabled=False,
        recording_path=None,
        default_resolution=TimestampResolution.Seconds,
        element_kinds=[element_kind],
        metric_definitions=[metric_definition],
        max_reg_elem_retries=1,  # Set low retry count for faster test
        max_endpoint_retries=None,
    )

    muse = Muse(config)

    with pytest.raises(MuseInitializationTimeoutError):
        await muse.initialize(timeout=0.25)

    assert (
        not muse.is_initialized
    ), "Muse should not initialize with unreachable endpoint"
