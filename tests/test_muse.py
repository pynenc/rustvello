"""General tests for Muse."""

import rustvello
import pytest

from common import get_client_type_from_env


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.integration
async def test_muse() -> None:
    """Test basic Muse functionality."""
    element_kind = rustvello.ElementKindRegistration(
        "EK1",
        "ElementKind1",
        "Test Element Kind",
    )
    metric_definition = rustvello.MetricDefinition(
        "M1",
        "Metric1",
        "Test Metric",
    )
    default_resolution = rustvello.TimestampResolution.Seconds
    config = rustvello.Config(
        endpoints=["http://localhost:8000"],
        client_type=get_client_type_from_env(),
        recording_enabled=False,
        recording_path=None,
        default_resolution=default_resolution,
        element_kinds=[element_kind],
        metric_definitions=[metric_definition],
        max_reg_elem_retries=5,
        max_endpoint_retries=None,
    )

    muse = rustvello.Muse(config)

    assert muse.finest_resolution == default_resolution

    assert not muse.is_initialized
    await muse.initialize(10)
    assert muse.is_initialized
