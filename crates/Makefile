.DEFAULT_GOAL := help

SHELL=bash
BASE ?= main

.PHONY: fmt
fmt:  ## Run rustfmt and dprint
	cargo fmt --all

.PHONY: check
check:  ## Run cargo check with all features
	cargo check -p rustvello --all-features

.PHONY: clippy
clippy:  ## Run clippy with all features
	cargo clippy --all-targets --all-features -- -D warnings -W clippy::dbg_macro

.PHONY: clippy-default
clippy-default:  ## Run clippy with default features
	cargo clippy --all-targets -- -D warnings -W clippy::dbg_macro

.PHONY: pre-commit
pre-commit: fmt clippy clippy-default  ## Run autoformatting and linting

.PHONY: deny
deny:  ## Run cargo-deny supply-chain checks
	cargo deny check

.PHONY: bench
bench:  ## Run criterion benchmarks
	cargo bench -p rustvello

.PHONY: semver
semver:  ## Check semver compatibility
	cargo semver-checks check-release -p rustvello-proto
	cargo semver-checks check-release -p rustvello-core
	cargo semver-checks check-release -p rustvello

.PHONY: changelog
changelog:  ## Generate CHANGELOG with git-cliff
	git cliff -o ../CHANGELOG.md

.PHONY: test
test:  ## Run tests
	cargo test --all-features \
		-p rustvello \
		-- \
		--test-threads=2

.PHONY: integration-tests
integration-tests:  ## Run integration tests
	cargo test --all-features --test it -p rustvello

.PHONY: test-doc
test-doc:  ## Run doc examples
	cargo test --doc \
	    -p rustvello-core

.PHONY: doctest
doctest:  ## Check that documentation builds
	cargo doc --no-deps -p rustvello-proto
	cargo doc --no-deps -p rustvello-core
	cargo doc --no-deps -p rustvello
	cargo doc --no-deps -p rustvello-cli

.PHONY: publish
publish:  ## Publish rustvello crates (in dependency order)
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

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
