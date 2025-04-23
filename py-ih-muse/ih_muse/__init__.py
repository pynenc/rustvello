"""Public package for rustvello."""

from rustvello._utils.muse_version import get_muse_version as _get_muse_version
from rustvello.config import ClientType, Config
from rustvello.muse import Muse
from rustvello.proto import (
    ElementKindRegistration,
    MetricDefinition,
    MetricPayload,
    MetricQuery,
    TimestampResolution,
)

__version__: str = _get_muse_version()
del _get_muse_version

__all__ = [
    "ClientType",
    "Config",
    "ElementKindRegistration",
    "MetricDefinition",
    "MetricPayload",
    "MetricQuery",
    "Muse",
    "TimestampResolution",
]
