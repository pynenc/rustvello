"""Tests for element registration."""

import pytest

from common import MuseTestContext


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.integration
async def test_element_registration() -> None:
    """Test the element registration functionality."""
    ctx = await MuseTestContext.create()
    local_elem_id = await ctx.register_test_element()

    # Retrieve the remote ElementId using the Muse client
    remote_element_id = ctx.muse.get_remote_element_id(local_elem_id)

    # Ensure the element is registered and the ElementId is valid
    assert remote_element_id is not None, "Failed to retrieve remote ElementId"
