#!/usr/bin/env python3
"""Patch workspace Cargo.toml version for dev/pre-release builds.

Usage: python scripts/patch_cargo_version.py <new-version>

Updates:
  1. [workspace.package] version
  2. All workspace dependency version specs (entries with both `version` and `path`)
     to `=<new-version>` so Cargo's semver resolver accepts pre-release versions.
"""

import re
import sys

from pathlib import Path

CARGO_TOML = Path(__file__).resolve().parent.parent / "Cargo.toml"


def patch(new_version: str) -> None:
    """Rewrite version strings in the workspace Cargo.toml."""
    text = CARGO_TOML.read_text()

    # 1. Patch the workspace package version (first bare `version = "..."` line)
    text = re.sub(
        r'^(version\s*=\s*)"[^"]*"',
        rf'\1"{new_version}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )

    # 2. Patch workspace dependency versions: lines with both `version` and `path`
    text, n = re.subn(
        r'(version\s*=\s*)"[^"]*"(\s*,\s*path\s*=)',
        rf'\1"={new_version}"\2',
        text,
    )

    CARGO_TOML.write_text(text)
    print(
        f"Patched Cargo.toml: workspace version"
        f" + {n} dependency version(s) -> {new_version}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <new-version>", file=sys.stderr)
        sys.exit(1)
    patch(sys.argv[1])
