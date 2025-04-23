"""Tests for metric functionality."""

import asyncio

import pytest

from common import MuseTestContext
from rustvello import MetricQuery


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.integration
async def test_send_and_receive_metric() -> None:
    """Test sending and receiving a metric."""
    ctx = await MuseTestContext.create()
    local_elem_id = await ctx.register_test_element()

    # Send metric
    await ctx.muse.send_metric(local_elem_id, "cpu_usage", 42.0)

    # Wait for the metric to be processed
    await asyncio.sleep(1)

    # Query the metrics
    query = MetricQuery()
    metrics = await ctx.muse.get_metrics(query)

    # Validate the received metrics
    assert any(
        metric.values == [42.0] for metric in metrics
    ), "Metric was not correctly received"
