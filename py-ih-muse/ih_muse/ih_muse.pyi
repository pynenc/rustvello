# rustvello/rustvello.pyi

from datetime import timedelta
from typing import Any, Optional

__version__: str

class PyMuse:
    is_initialized: bool
    finest_resolution: TimestampResolution
    async def initialize(self, timeout: Optional[float] = None) -> None: ...
    def get_remote_element_id(self, local_elem_id: str) -> Optional[int]: ...
    async def register_element(
        self,
        kind_code: str,
        name: str,
        metadata: dict[str, str],
        parent_id: Optional[str] = None,
    ) -> str: ...
    async def send_metric(
        self,
        local_elem_id: str,
        metric_code: str,
        value: float,
    ) -> None: ...
    async def get_metrics(self, query: PyMetricQuery) -> list[PyMetricPayload]: ...
    async def replay(self, replay_path: str) -> None: ...
    def __init__(self, config: PyConfig) -> None: ...

class PyConfig:
    def __init__(
        self,
        endpoints: list[str],
        client_type: ClientType,
        default_resolution: TimestampResolution,
        element_kinds: list[Any],  # PyElementKindRegistration
        metric_definitions: list[Any],  # PyMetricDefinition
        max_reg_elem_retries: int,
        max_endpoint_retries: Optional[int],
        recording_enabled: bool,  # noqa: FBT001
        recording_path: Optional[str] = None,
        recording_flush_interval: Optional[timedelta] = None,
        initialization_interval: Optional[timedelta] = None,
        cluster_monitor_interval: Optional[timedelta] = None,
    ) -> None: ...

class ClientType:
    Poet: ClientType
    Mock: ClientType

class TimestampResolution:
    Years: TimestampResolution
    Months: TimestampResolution
    Weeks: TimestampResolution
    Days: TimestampResolution
    Hours: TimestampResolution
    Minutes: TimestampResolution
    Seconds: TimestampResolution
    Milliseconds: TimestampResolution
    Microseconds: TimestampResolution

    def to_timedelta(self) -> timedelta: ...

class PyMetricDefinition:
    def __init__(self, code: str, name: str, description: str) -> None: ...

class PyMetricPayload:
    time: int
    element_id: int
    metric_ids: list[int]
    values: list[Optional[float]]
    def __init__(
        self,
        time: int,
        element_id: int,
        metric_ids: list[int],
        values: list[Optional[float]],
    ) -> None: ...

class PyMetricQuery:
    def __init__(
        self,
        start_time: Optional[int],
        end_time: Optional[int],
        element_id: Optional[int],
        parent_id: Optional[int],
        metric_id: Optional[float],
    ) -> None: ...

class PyElementKindRegistration:
    def __init__(
        self, code: str, name: str, description: str, parent_code: Optional[str] = None
    ) -> None: ...
