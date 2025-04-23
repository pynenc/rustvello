"""Core functionality for Muse."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from rustvello.rustvello import PyMuse, TimestampResolution
from rustvello.proto import MetricPayload

if TYPE_CHECKING:
    from rustvello.config import Config
    from rustvello.proto import MetricQuery


class Muse:
    """The main class for interacting with the Muse system in Python.

    :param Config config:
        The configuration object for initializing Muse.

    ```python
    # Example usage:
    from rustvello import Muse, Config

    config = Config(...)
    muse = Muse(config)
    await muse.initialize(timeout=5.0)
    ```
    """

    _muse: PyMuse

    def __init__(self, config: Config) -> None:
        """Initialize the Muse instance."""
        self._muse = PyMuse(config._config)

    async def initialize(self, timeout: Optional[float] = None) -> None:
        """Initialize the Muse client and starts background tasks.

        :param Optional[float] timeout:
            Optional timeout in seconds for the initialization process.

        :raises MuseInitializationTimeoutError:
            If initialization times out.
        """
        await self._muse.initialize(timeout)

    @classmethod
    async def create(
        cls: type[Muse], config: Config, timeout: Optional[float] = None
    ) -> Muse:
        """Create and initialize a Muse instance.

        :param Config config:
            The configuration object.
        :param Optional[float] timeout:
            Optional timeout in seconds for initialization.

        :return:
            An initialized Muse instance.
        """
        instance = cls(config)
        await instance.initialize(timeout)
        return instance

    @property
    def is_initialized(self) -> bool:
        """Check whether the Muse client is initialized.

        :return:
            `True` if initialized, `False` otherwise.
        """
        return self._muse.is_initialized

    @property
    def finest_resolution(self) -> TimestampResolution:
        """Get the finest resolution stored by the Poet.

        :return:
            The current TimestampResolution stored in the poet.
        """
        return self._muse.finest_resolution

    def get_remote_element_id(self, local_elem_id: str) -> Optional[int]:
        """Retrieve the remote Element ID associated with a local Element ID.

        :param str local_elem_id:
            The local Element ID to query.

        :return:
            The corresponding remote Element ID, or `None` if not registered.
        """
        return self._muse.get_remote_element_id(local_elem_id)

    async def register_element(
        self,
        kind_code: str,
        name: str,
        metadata: dict[str, str],
        parent_id: Optional[str] = None,
    ) -> str:
        """Register a new element with the Muse system.

        :param str kind_code:
            The kind code of the element.
        :param str name:
            The name of the element.
        :param dict[str, str] metadata:
            Metadata associated with the element.
        :param Optional[int] parent_id:
            The parent element ID, if any.

        :return:
            The local element ID assigned to the registered element.

        :raises MuseError:
            If registration fails.
        """
        local_elem_id = await self._muse.register_element(
            kind_code,
            name,
            metadata,
            parent_id,
        )
        return local_elem_id

    async def send_metric(
        self,
        local_elem_id: str,
        metric_code: str,
        value: float,
    ) -> None:
        """Send a metric value associated with an element.

        :param int local_elem_id:
            The local ID of the element.
        :param str metric_code:
            The code identifying the metric.
        :param float value:
            The value of the metric to send.

        :raises MuseError:
            If sending the metric fails.
        """
        await self._muse.send_metric(
            local_elem_id,
            metric_code,
            value,
        )

    async def get_metrics(self, query: MetricQuery) -> list[MetricPayload]:
        """Retrieve metrics based on a query.

        :param MetricQuery query:
            The query specifying the criteria for retrieving metrics.

        :return:
            A list of MetricPayload matching the query.

        :raises MuseError:
            If retrieving the metrics fails.
        """
        raw_metrics = await self._muse.get_metrics(query._metric_query)
        return [MetricPayload.from_py_metric_payload(metric) for metric in raw_metrics]

    async def replay(self, replay_path: str) -> None:
        """Replays events from a recording file.

        :param str replay_path:
            The file path to the recording.

        :raises MuseError:
            If replaying the events fails.
        """
        await self._muse.replay(replay_path)
