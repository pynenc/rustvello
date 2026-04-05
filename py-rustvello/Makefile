.PHONY: install
install:
	@echo "Installing dependencies via UV..."
	uv sync --all-extras --all-groups

.PHONY: build
build:
	@echo "Building rustvello wheel with maturin..."
	uv run maturin develop --release

.PHONY: install-pre-commit
install-pre-commit: install
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install

.PHONY: pre-commit
pre-commit:
	@echo "Running pre-commit on all files..."
	uv run pre-commit run --all-files

.PHONY: lint
lint:
	@echo "Running ruff checks..."
	uv run ruff check rustvello/ tests/
	uv run ruff format --check rustvello/ tests/

.PHONY: format
format:
	@echo "Formatting code..."
	uv run ruff check --fix rustvello/ tests/
	uv run ruff format rustvello/ tests/

.PHONY: typecheck
typecheck:
	@echo "Running mypy..."
	uv run mypy rustvello/

.PHONY: clean
clean:
	@echo "Cleaning previous coverage data and HTML reports..."
	rm -f .coverage .coverage.*
	rm -rf htmlcov

.PHONY: test
test: clean
	@echo "Running tests with coverage..."
	uv run coverage run -m pytest tests/ -q
	uv run coverage report

.PHONY: coverage
coverage:
	@echo "Displaying coverage report..."
	uv run coverage report

.PHONY: htmlcov
htmlcov:
	@echo "Generating HTML coverage report..."
	uv run coverage html --show-contexts --title "py-rustvello Coverage"

.PHONY: check
check: lint typecheck test
	@echo "All checks passed."
