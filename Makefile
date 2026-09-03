# Interpreter used by the Python compatibility metadata helper.
PYTHON_BIN ?= $(CURDIR)/.venv/bin/python

.PHONY: install
install: ## Install dependencies, build the Python extension, and set up pre-commit hooks
	@echo "🚀 Installing dependencies"
	@uv sync --group dev --no-install-package rustvello
	@echo "🚀 Building and installing py-rustvello in develop mode"
	@uv run maturin develop --release -m py-rustvello/Cargo.toml
	@echo "🚀 Installing pre-commit hooks"
	@uv run pre-commit install

.PHONY: check
check: ## Run all code quality checks (pre-commit)
	@echo "🚀 Running all checks via pre-commit"
	@uv run pre-commit run --all-files

.PHONY: build
build: clean-build ## Build the Python wheel and sdist
	@echo "🚀 Creating wheel file"
	@uvx maturin build --release -m py-rustvello/Cargo.toml --out dist --sdist

.PHONY: build-rust
build-rust: ## Build all Rust crates (excludes py-rustvello cdylib)
	@echo "🚀 Building Rust workspace"
	@cargo build --workspace --exclude py-rustvello

.PHONY: develop
develop: ## Build and install py-rustvello in develop mode
	@echo "🚀 Building and installing package in develop mode"
	@uv run maturin develop --release -m py-rustvello/Cargo.toml

.PHONY: test-python
test-python: develop ## Run Python tests with pytest
	@echo "🚀 Testing Python: Running pytest"
	@uv run pytest py-rustvello/ --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: python-versions
python-versions: ## Print Python versions supported by py-rustvello metadata
	@$(PYTHON_BIN) scripts/python_compatibility.py

.PHONY: test-rust
test-rust: ## Run Rust tests
	@echo "🚀 Testing Rust: Running cargo test"
	@cargo test --workspace --exclude py-rustvello --exclude rustvello-python

.PHONY: test
test: test-rust test-python ## Run all tests (Rust + Python)

.PHONY: test-docker
test-docker: ## Run ignored Docker backend compliance suites
	@cargo test -p rustvello-redis -p rustvello-postgres -p rustvello-mongo -p rustvello-mongo3 -p rustvello-rabbitmq -- --ignored --test-threads=1

.PHONY: test-stress
test-stress: ## Run fast in-memory contention tests
	@cargo test -p rustvello --test concurrency_stress_tests

.PHONY: test-soak
test-soak: ## Run ignored high-volume and SQLite contention tests
	@cargo test -p rustvello --test concurrency_stress_tests -- --ignored --test-threads=1
	@cargo test -p rustvello-sqlite --test stress -- --ignored --test-threads=1

.PHONY: fuzz
fuzz: ## Run fuzz targets for a short duration (CI-friendly, requires nightly)
	@echo "🚀 Running fuzz targets (30s each)"
	@cargo +nightly fuzz run fuzz_json_trigger -- -max_total_time=30
	@cargo +nightly fuzz run fuzz_toml_config -- -max_total_time=30

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@echo "🚀 Removing build artifacts"
	@rm -rf dist build
	@cargo clean

.PHONY: publish-python
publish-python: build ## Publish the Python package to PyPI
	@echo "🚀 Publishing Python package to PyPI"
	@uvx twine upload dist/*

.PHONY: publish-rust
publish-rust: ## Publish Rust crates to crates.io (in dependency order)
	@echo "🚀 Publishing Rust crates to crates.io"
	cargo publish -p rustvello-proto
	sleep 30
	cargo publish -p rustvello-core
	sleep 30
	cargo publish -p rustvello-macros
	cargo publish -p rustvello-mem
	cargo publish -p rustvello-sqlite
	cargo publish -p rustvello-redis
	cargo publish -p rustvello-postgres
	cargo publish -p rustvello-mongo
	cargo publish -p rustvello-rabbitmq
	cargo publish -p rustvello-prometheus
	sleep 30
	cargo publish -p rustvello
	cargo publish -p rustvello-monitoring
	sleep 30
	cargo publish -p rustvello-cli
	cargo publish -p rustvello-test-suite

.PHONY: docs-install
docs-install: ## Install rustvello documentation dependencies (uv docs group)
	@echo "🚀 Installing documentation dependencies"
	@uv sync --group docs

.PHONY: docs-render
docs-render: docs-install ## Render rustvello documentation to docs/_build/html
	@echo "🚀 Building rustvello documentation"
	@rm -rf docs/_build
	@uv run --group docs python -m sphinx -b html docs/ docs/_build/html
	@echo "📖 Rustvello docs built — open docs/_build/html/index.html"

.PHONY: docs
docs: docs-render ## Build rustvello documentation

.PHONY: docs-build
docs-build: docs-render ## Build rustvello documentation (compat alias)

.PHONY: docs-serve
docs-serve: docs-render ## Serve rustvello documentation locally
	@echo "🚀 Serving rustvello documentation at http://localhost:8080"
	@uv run --group docs python -m http.server 8080 --directory docs/_build/html

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
