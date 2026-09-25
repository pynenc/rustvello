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

``skill`` is the "fresh agent" check of the agent skill in ``skills/rustvello``:
it copies the skill directory alone to a scratch location and, with the current
interpreter (the built wheel installed, nothing else from the repository), runs
every example and helper script there, checks that the version the skill
requires is the installed one, and checks the Python snippets of ``SKILL.md``
against the installed API (``evals/rustvello_eval/api_surface.py``).
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
SKILL_DIR = ROOT / "skills" / "rustvello"
SKILL = SKILL_DIR / "SKILL.md"
# Documents whose marked blocks must equal their files; only READMES are run by `run`.
DOCUMENTS = [*READMES, SKILL]
# Helper scripts of the skill, run with these arguments by `skill`.
SKILL_SCRIPTS = {"guarantees.py": ["--need", "delayed_retry", "durability"]}
PYTHON_BLOCK = re.compile(r"^```python\n(?P<body>.*?)^```", re.S | re.M)
MARKER = re.compile(
    r"<!-- readme-example: (?P<path>\S+) -->\n+```(?P<lang>\w+)\n(?P<body>.*?)^```",
    re.S | re.M,
)
TIMEOUT_SECONDS = 120


def marked_examples() -> list[tuple[Path, Path, str]]:
    """Return ``(readme, example file, README block body)`` for every marked block."""
    return [
        (readme, ROOT / match["path"], match["body"])
        for readme in DOCUMENTS
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
    for readme in DOCUMENTS:
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
    readme_paths = {path for doc, path, _ in marked_examples() if doc in READMES}
    for path in sorted(readme_paths):
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


def _skill_version_problems() -> list[str]:
    """Check that the skill requires the installed ``major.minor``."""
    import rustvello

    installed = ".".join(rustvello.__version__.split(".")[:2])
    text = SKILL.read_text()
    required = re.search(r'rustvello-version: "(\d+\.\d+)"', text)
    pin = re.search(r"rustvello>=(\d+\.\d+),<", text)
    problems = []
    if not text.startswith("---\nname: rustvello\ndescription: "):
        problems.append("SKILL.md must start with front matter: name, then description")
    for label, match in (("metadata rustvello-version", required), ("pip pin", pin)):
        if match is None or match[1] != installed:
            found = match[1] if match else "none"
            problems.append(
                f"SKILL.md {label} is {found}, installed rustvello is {installed}"
            )
    return problems


def _skill_snippet_problems() -> list[str]:
    """Check that unmarked SKILL.md snippets use only the installed API."""
    sys.path.insert(0, str(ROOT / "evals"))
    from rustvello_eval.api_surface import check_code, introspect

    surface = introspect()
    marked = {body for doc, _, body in marked_examples() if doc == SKILL}
    problems = []
    for match in PYTHON_BLOCK.finditer(SKILL.read_text()):
        if match["body"] in marked:
            continue
        # snippets use names defined elsewhere; give them an app to resolve against
        code = "from rustvello import App\napp = App()\n" + match["body"]
        problems += [f"SKILL.md snippet: {e}" for e in check_code(code, surface).errors]
    return problems


def skill() -> int:
    """Fresh-agent check: run the skill's examples and scripts from a copy of it."""
    import shutil

    status = check()
    problems = _skill_version_problems() + _skill_snippet_problems()
    for problem in problems:
        print(problem, file=sys.stderr)
    failures = len(problems) + status
    with tempfile.TemporaryDirectory() as scratch:
        copy = Path(scratch) / "rustvello"
        shutil.copytree(SKILL_DIR, copy, ignore=shutil.ignore_patterns("__pycache__"))
        runs = [([str(p)], p) for p in sorted((copy / "examples").glob("*.py"))]
        runs += [
            ([str(copy / "scripts" / n), *a], copy / "scripts" / n)
            for n, a in SKILL_SCRIPTS.items()
        ]
        for args, path in runs:
            with tempfile.TemporaryDirectory() as workdir:
                ok = _run([sys.executable, *args], Path(workdir))
            print(f"{'ok ' if ok else 'FAIL'} {path.relative_to(copy)}", flush=True)
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
    sub.add_parser(
        "skill", help="fresh-agent check of the agent skill (needs the wheel)"
    )
    args = parser.parse_args()
    if args.command == "check":
        return check()
    if args.command == "skill":
        return skill()
    if args.command == "sync":
        return sync()
    status = check()
    return status or run(set(args.language or ["python", "rust"]))


if __name__ == "__main__":
    sys.exit(main())
