"""Read the supported Python versions from the package metadata."""

from __future__ import annotations

import argparse
import json
import re

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - only used on Python 3.10 and older
    import tomli as tomllib

from pathlib import Path

PROJECT_FILE = Path(__file__).resolve().parents[1] / "py-rustvello" / "pyproject.toml"
PYTHON_CLASSIFIER = re.compile(r"^Programming Language :: Python :: (\d+\.\d+)$")
VERSION_BOUND = re.compile(r"(?P<operator>>=|>|<=|<)\s*(?P<version>\d+(?:\.\d+)?)")


class MetadataError(ValueError):
    """Raised when Python compatibility metadata is incomplete."""


def _version_tuple(version: str) -> tuple[int, int]:
    major, minor = version.split(".", maxsplit=1)
    return int(major), int(minor)


def supported_versions(project_file: Path = PROJECT_FILE) -> list[str]:
    """Return the Python versions listed by compatible project classifiers."""
    metadata = tomllib.loads(project_file.read_text(encoding="utf-8"))
    project = metadata["project"]
    requires_python = project["requires-python"]
    bounds = {
        match.group("operator"): _version_tuple(match.group("version"))
        for match in VERSION_BOUND.finditer(requires_python)
    }

    lower_operator = ">=" if ">=" in bounds else ">"
    upper_operator = "<" if "<" in bounds else "<="
    if lower_operator not in bounds or upper_operator not in bounds:
        message = (
            "requires-python must declare a lower and upper version bound: "
            f"{requires_python!r}"
        )
        raise MetadataError(message)
    lower_bound = bounds[lower_operator]
    upper_bound = bounds[upper_operator]

    def is_compatible(version: str) -> bool:
        value = _version_tuple(version)
        lower_ok = (
            value >= lower_bound if lower_operator == ">=" else value > lower_bound
        )
        upper_ok = (
            value < upper_bound if upper_operator == "<" else value <= upper_bound
        )
        return lower_ok and upper_ok

    versions = {
        match.group(1)
        for classifier in project.get("classifiers", [])
        if (match := PYTHON_CLASSIFIER.match(classifier))
    }
    compatible = sorted(
        (version for version in versions if is_compatible(version)),
        key=_version_tuple,
    )
    if not compatible:
        message = "pyproject.toml declares no compatible Python classifiers"
        raise MetadataError(message)
    return compatible


def main() -> None:
    """Print the supported Python versions in the requested format."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format",
        choices=("json", "space"),
        default="space",
        help="Output format for the supported versions.",
    )
    args = parser.parse_args()
    versions = supported_versions()
    if args.format == "json":
        print(json.dumps(versions))
    else:
        print(" ".join(versions))


if __name__ == "__main__":
    main()
