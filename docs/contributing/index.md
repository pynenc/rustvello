# Contributing to rustvello

Thank you for your interest in contributing! This guide covers how to set up your development environment and submit changes.

```{toctree}
:hidden:
:maxdepth: 2

ide
testing/index
```

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain, 1.78+)
- [Python](https://www.python.org/downloads/) 3.11+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [maturin](https://www.maturin.rs/) (for building Python bindings)
- [make](https://www.gnu.org/software/make/) (build automation)
- [git](https://git-scm.com/)

## Setup

```bash
git clone https://github.com/pynenc/rustvello.git
cd rustvello
make install
```

`make install` will:

1. Install Python dependencies via `uv sync --group dev`
2. Build the Python bindings via `maturin develop`
3. Install pre-commit hooks

## Makefile Targets

| Target              | Description                                   |
| ------------------- | --------------------------------------------- |
| `make install`      | Install all dependencies and pre-commit hooks |
| `make develop`      | Build Python bindings with maturin            |
| `make check`        | Run all lints and checks (Rust + Python)      |
| `make check-rust`   | Run `cargo clippy` and `cargo fmt --check`    |
| `make check-python` | Run `ruff check` and `ruff format --check`    |
| `make test`         | Run all tests (Rust + Python)                 |
| `make test-rust`    | Run Rust tests only                           |
| `make test-python`  | Run Python tests with pytest                  |
| `make build-rust`   | Build all Rust crates                         |
| `make docs-build`   | Build Sphinx documentation                    |
| `make docs-serve`   | Serve docs locally                            |
| `make clean`        | Clean all build artifacts                     |

Run `make help` to see all available targets.

## Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Run `make check` and `make test`
4. Commit using [Conventional Commits](#conventional-commits) format
5. Push and open a pull request

## Conventional Commits

All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```text
<type>(<scope>): <description>
```

| Type       | Description                        |
| ---------- | ---------------------------------- |
| `feat`     | A new feature                      |
| `fix`      | A bug fix                          |
| `docs`     | Documentation changes              |
| `style`    | Formatting, no code change         |
| `refactor` | Code restructuring                 |
| `perf`     | Performance improvement            |
| `test`     | Adding or fixing tests             |
| `build`    | Build system or dependency changes |
| `ci`       | CI configuration changes           |
| `chore`    | Other maintenance tasks            |

## Pull Request Guidelines

- PR title should follow conventional commit format
- Include a clear description of the changes
- Ensure all checks pass (`make check && make test`)
- Reference any related issues

## Project Architecture

See {doc}`/architecture` for an overview of the crate structure and how the components fit together.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
