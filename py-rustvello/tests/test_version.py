"""Tests for package version and Python compatibility metadata."""

import re
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - only used on Python 3.10 and older
    import tomli as tomllib
from pathlib import Path

import rustvello

PROJECT_FILE = Path(__file__).parents[1] / "pyproject.toml"


def project_metadata() -> dict[str, object]:
    return tomllib.loads(PROJECT_FILE.read_text(encoding="utf-8"))["project"]


def test_get_version_returns_string() -> None:
    version = rustvello.get_version()
    assert isinstance(version, str)
    assert len(version) > 0


def test_version_attribute_matches() -> None:
    assert rustvello.__version__ == rustvello.get_version()


def test_extension_version_matches_workspace_manifest() -> None:
    manifest = (Path(__file__).parents[2] / "Cargo.toml").read_text(encoding="utf-8")
    workspace_package = manifest.split("[workspace.package]", maxsplit=1)[1]
    match = re.search(r'^version\s*=\s*"([^"]+)"', workspace_package, re.MULTILINE)

    assert match is not None, "workspace package version should be declared"
    assert rustvello.get_version() == match.group(1)


def test_current_python_is_declared_supported() -> None:
    metadata = project_metadata()
    classifiers = metadata["classifiers"]
    current = f"{sys.version_info.major}.{sys.version_info.minor}"

    assert f"Programming Language :: Python :: {current}" in classifiers


def test_python_metadata_declares_the_supported_range() -> None:
    metadata = project_metadata()
    requires_python = metadata["requires-python"]

    assert requires_python == ">=3.9,<4.0"
    assert {
        classifier.rsplit(" :: ", maxsplit=1)[-1]
        for classifier in metadata["classifiers"]
        if classifier.startswith("Programming Language :: Python :: 3.")
    } >= {"3.9", "3.10", "3.11", "3.12", "3.13"}
