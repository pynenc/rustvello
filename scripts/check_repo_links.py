#!/usr/bin/env python3
"""Offline check of links that point into this repository.

Two kinds of link are resolved against the working tree, with no network:

- absolute links to this repository's ``main`` branch
  (``github.com/pynenc/rustvello/blob|tree/main/<path>`` and
  ``raw.githubusercontent.com/pynenc/rustvello/main/<path>``): a file added by
  a pull request only exists on ``main`` after the merge, so the online link
  check (lychee, which excludes these URLs) cannot judge them;
- relative Markdown links (``[text](path)``) between files of the repository.

Runs as a pre-commit hook and in ``make links``; exits 1 listing every
broken link. Usage: ``python3 scripts/check_repo_links.py [FILE ...]`` (no
arguments: every tracked Markdown file and ``llms.txt``).
"""

from __future__ import annotations

import re
import subprocess
import sys

from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
OWN = re.compile(
    r"https://(?:github\.com/pynenc/rustvello/(?P<kind>blob|tree)/main/"
    r"|raw\.githubusercontent\.com/pynenc/rustvello/main/)"
    r"(?P<path>[^\s)>\"'`#?]+)"
)
RELATIVE = re.compile(r"\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
SKIP = ("http://", "https://", "mailto:", "#", "{", "<")
IGNORED_DIRS = ("target/", ".venv/", "docs/_build/", "node_modules/", "fuzz/")


def tracked() -> list[Path]:
    """Every tracked Markdown file plus ``llms.txt``."""
    out = subprocess.run(
        ["git", "ls-files", "*.md", "llms.txt"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return [ROOT / name for name in out if not name.startswith(IGNORED_DIRS)]


def _code_free(text: str) -> str:
    """Blank fenced code blocks, keeping line numbers."""
    lines, fenced = [], False
    for line in text.splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            lines.append("")
            continue
        lines.append("" if fenced else line)
    return "\n".join(lines)


def check(path: Path) -> list[str]:
    """Broken links in one file, as ``file:line: link (reason)``."""
    text = _code_free(path.read_text(encoding="utf-8"))
    problems = []
    for number, line in enumerate(text.splitlines(), start=1):
        where = f"{path.relative_to(ROOT)}:{number}"
        for match in OWN.finditer(line):
            target = ROOT / unquote(match["path"]).rstrip("/")
            want_dir = match["kind"] == "tree"
            exists = target.is_dir() if want_dir else target.is_file()
            if not exists:
                problems.append(
                    f"{where}: {match.group(0)} (no such path on this branch)"
                )
        if path.suffix != ".md":
            continue
        for match in RELATIVE.finditer(line):
            target_text = match["target"]
            if target_text.startswith(SKIP):
                continue
            relative = unquote(target_text.split("#", 1)[0])
            if not relative:
                continue
            target = (path.parent / relative).resolve()
            if not target.exists():
                problems.append(f"{where}: ({target_text}) (no such file)")
    return problems


def main(argv: list[str]) -> int:
    """Check the given files (default: all tracked); return the exit status."""
    files = [Path(name).resolve() for name in argv] or tracked()
    problems = [
        problem
        for file in files
        if file.suffix == ".md" or file.name == "llms.txt"
        for problem in check(file)
    ]
    for problem in problems:
        print(problem)
    if problems:
        print(f"{len(problems)} broken repository link(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
