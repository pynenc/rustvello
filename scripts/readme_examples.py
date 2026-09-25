"""Keep README code examples identical to runnable files, and run them.

A README code block that must stay executable is preceded by a marker comment
naming the file it mirrors::

    <!-- readme-example: examples/python/readme_quickstart.py -->
    ```python
    ...
    ```

``check`` fails when a marked block differs from its file; ``sync`` copies the
files into the READMEs. ``run`` executes the files: Python examples with the current
interpreter (which must have the ``rustvello`` wheel installed), Rust examples
through ``cargo run --example``. Every example must exit 0 within the timeout,
so a README that submits work nobody executes fails CI instead of the reader.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
READMES = [ROOT / "README.md", ROOT / "py-rustvello" / "README.md"]
MARKER = re.compile(
    r"<!-- readme-example: (?P<path>\S+) -->\n+```(?P<lang>\w+)\n(?P<body>.*?)^```",
    re.S | re.M,
)
TIMEOUT_SECONDS = 120


def marked_examples() -> list[tuple[Path, Path, str]]:
    """Return ``(readme, example file, README block body)`` for every marked block."""
    return [
        (readme, ROOT / match["path"], match["body"])
        for readme in READMES
        for match in MARKER.finditer(readme.read_text())
    ]


def check() -> int:
    """Fail when a README block and the file it mirrors differ."""
    failures = 0
    examples = marked_examples()
    if not examples:
        print("no <!-- readme-example: ... --> markers found", file=sys.stderr)
        return 1
    for readme, path, body in examples:
        rel = path.relative_to(ROOT)
        if not path.is_file():
            print(f"{readme.name}: {rel} does not exist", file=sys.stderr)
            failures += 1
        elif path.read_text() != body:
            where = readme.relative_to(ROOT)
            print(f"{where}: block differs from {rel}; run `sync`", file=sys.stderr)
            failures += 1
        else:
            print(f"ok  {readme.relative_to(ROOT)} == {rel}")
    return 1 if failures else 0


def sync() -> int:
    """Rewrite every marked README block from the file it mirrors."""
    for readme in READMES:
        text = readme.read_text()

        def replace(match: re.Match[str]) -> str:
            body = (ROOT / match["path"]).read_text()
            marker = f"<!-- readme-example: {match['path']} -->"
            return f"{marker}\n\n```{match['lang']}\n{body}```"

        readme.write_text(MARKER.sub(replace, text))
    return check()


def _run(cmd: list[str], cwd: Path, timeout: float | None = TIMEOUT_SECONDS) -> bool:
    print("$", " ".join(cmd), flush=True)
    try:
        result = subprocess.run(cmd, cwd=cwd, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        print(f"timed out after {TIMEOUT_SECONDS}s", file=sys.stderr)
        return False
    return result.returncode == 0


def _cargo_run(path: Path) -> list[str]:
    """``cargo run`` for ``crates/<pkg>/examples/<name>.rs`` with required features."""
    try:
        import tomllib
    except ModuleNotFoundError:  # Python < 3.11
        import tomli as tomllib

    manifest = tomllib.loads((path.parent.parent / "Cargo.toml").read_text())
    package = manifest["package"]["name"]
    features = next(
        (
            ex.get("required-features", [])
            for ex in manifest.get("example", [])
            if ex["name"] == path.stem
        ),
        [],
    )
    cmd = ["cargo", "run", "-q", "-p", package, "--example", path.stem]
    return cmd + (["--features", ",".join(features)] if features else [])


def run(languages: set[str]) -> int:
    """Execute every example of the selected languages from a scratch directory."""
    failures = 0
    for path in sorted({path for _, path, _ in marked_examples()}):
        rel = path.relative_to(ROOT)
        if path.suffix == ".py" and "python" in languages:
            with tempfile.TemporaryDirectory() as scratch:
                ok = _run([sys.executable, str(path)], Path(scratch))
        elif path.suffix == ".rs" and "rust" in languages:
            # compile first without a deadline; only the run itself is timed
            cmd = _cargo_run(path)
            ok = _run(["cargo", "build", *cmd[2:]], ROOT, timeout=None) and _run(
                cmd, ROOT
            )
        else:
            continue
        print(f"{'ok ' if ok else 'FAIL'} {rel}", flush=True)
        failures += not ok
    return 1 if failures else 0


def main() -> int:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="README blocks match their example files")
    sub.add_parser("sync", help="copy the example files into the README blocks")
    run_parser = sub.add_parser("run", help="execute the example files")
    run_parser.add_argument("--language", choices=["python", "rust"], action="append")
    args = parser.parse_args()
    if args.command == "check":
        return check()
    if args.command == "sync":
        return sync()
    status = check()
    return status or run(set(args.language or ["python", "rust"]))


if __name__ == "__main__":
    sys.exit(main())
