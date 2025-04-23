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
	cargo clippy --all-targets --all-features -- -W clippy::dbg_macro

.PHONY: clippy-default
clippy-default:  ## Run clippy with default features
	cargo clippy --all-targets -- -W clippy::dbg_macro

.PHONY: pre-commit
pre-commit: fmt clippy clippy-default  ## Run autoformatting and linting

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
	cargo doc --no-deps -p rustvello-record
	cargo doc --no-deps -p rustvello-client
	cargo doc --no-deps -p rustvello
	cargo doc --no-deps -p rustvello-cli

.PHONY: publish
publish:  ## Publish rustvello crates
	cargo publish --allow-dirty -p rustvello-proto
	cargo publish --allow-dirty -p rustvello-core
	cargo publish --allow-dirty -p rustvello-record
	cargo publish --allow-dirty -p rustvello-client
	cargo publish --allow-dirty -p rustvello
	cargo publish --allow-dirty -p rustvello-cli

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
