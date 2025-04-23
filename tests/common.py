"""Common utilities for tests."""

from __future__ import annotations

import asyncio
import os
import time

from datetime import timedelta
from typing import Optional

from rustvello import (
    ClientType,
    Config,
    ElementKindRegistration,
    MetricDefinition,
    Muse,
    TimestampResolution,
)


def get_client_type_from_env() -> ClientType:
    """Retrieve the ClientType from the environment variable `rustvello_CLIENT_TYPE`.

    Defaults to `Mock` if the variable is not set or invalid.

    :return:  The client type to use.

    """
    client_type_str = os.getenv("rustvello_CLIENT_TYPE", "Mock").lower()
    if client_type_str == "poet":
        return ClientType.Poet
    return ClientType.Mock


class MuseTestContext:
    """Context for Muse test cases."""

    muse: Muse
    endpoint: str

    def __init__(self, muse: Muse, endpoint: str) -> None:
        """Initialize the MuseTestContext instance."""
        self.muse = muse
        self.endpoint = endpoint

    @classmethod
    async def create(
        cls: type[MuseTestContext], client_type: Optional[ClientType] = None
    ) -> MuseTestContext:
        """Create a new MuseTestContext.

        :param Optional[ClientType] client_type:
            The client type to use for testing.

        :return:  A new MuseTestContext instance.

        """
        client_type = client_type or get_client_type_from_env()
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

        config = Config(
            endpoints=["http://localhost:8000"],
            client_type=client_type,
            recording_enabled=False,
            recording_path=None,
            default_resolution=TimestampResolution.Milliseconds,
            element_kinds=[element_kind],
            metric_definitions=[metric_definition],
            max_reg_elem_retries=3,
            max_endpoint_retries=10,
            initialization_interval=timedelta(milliseconds=1),
            cluster_monitor_interval=timedelta(milliseconds=1),
        )

        muse = await Muse.create(config, timeout=10.0)
        endpoint = "http://localhost:8000"
        return cls(muse, endpoint)

    async def register_test_element(self) -> str:
        """Register a test element with the Muse system.

        This method registers an element with default parameters suitable for testing.

        :return:
            The local element ID of the registered test element.
        """
        local_elem_id = await self.muse.register_element(
            "server",
            "TestServer",
            {},
            None,
        )

        # Wait until the element is registered
        start_time = time.time()
        while (
            not self.muse.get_remote_element_id(local_elem_id)
            and time.time() - start_time < 5
        ):
            await asyncio.sleep(0.1)

        return local_elem_id
