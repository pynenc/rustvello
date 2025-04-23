"""Protocol definitions for Metrics messages."""

from __future__ import annotations

from typing import Optional

from rustvello.rustvello import PyMetricDefinition, PyMetricPayload, PyMetricQuery


class MetricDefinition:
    """Definition of a metric."""

    _metric_def: PyMetricDefinition

    def __init__(self, code: str, name: str, description: str) -> None:
        """Initialize MetricDefinition."""
        self._metric_def = PyMetricDefinition(code, name, description)


class MetricPayload:
    """Definition of a metric payloadi."""

    _metric_payload: PyMetricPayload

    def __init__(
        self,
        time: int,
        element_id: int,
        metric_ids: list[int],
        values: list[Optional[float]],
    ) -> None:
        """Initialize MetricPayload."""
        self._metric_payload = PyMetricPayload(time, element_id, metric_ids, values)

    @classmethod
    def from_py_metric_payload(
        cls: type[MetricPayload], py_metric_payload: PyMetricPayload
    ) -> MetricPayload:
        """Create a MetricPayload from a PyMetricPayload.

        :param PyMetricPayload py_metric_payload:
            The PyMetricPayload to convert.

        :return:
            A corresponding MetricPayload instance.
        """
        return cls(
            time=py_metric_payload.time,
            element_id=py_metric_payload.element_id,
            metric_ids=py_metric_payload.metric_ids,
            values=py_metric_payload.values,
        )

    @property
    def time(self) -> int:
        """Retrieve the timestamp of the metric payload.

        :return:
            The time in milliseconds since epoch.
        """
        return self._metric_payload.time

    @property
    def element_id(self) -> int:
        """Retrieve the element ID associated with the metric payload.

        :return:
            The element ID as an integer.
        """
        return self._metric_payload.element_id

    @property
    def metric_ids(self) -> list[int]:
        """Retrieve the list of metric IDs.

        :return:
            A list of integers representing metric IDs.
        """
        return self._metric_payload.metric_ids

    @property
    def values(self) -> list[Optional[float]]:
        """Retrieve the list of metric values.

        :return:
            A list of floats or None for each metric value.
        """
        return self._metric_payload.values


class MetricQuery:
    """Definition of a metric."""

    _metric_query: PyMetricQuery

    def __init__(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        element_id: Optional[int] = None,
        parent_id: Optional[int] = None,
        metric_id: Optional[float] = None,
    ) -> None:
        """Initialize MetricQuery."""
        self._metric_query = PyMetricQuery(
            start_time, end_time, element_id, parent_id, metric_id
        )
