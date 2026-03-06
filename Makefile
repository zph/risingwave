# Ensure rustup + lld are on PATH (matches .envrc)
export PATH := /opt/homebrew/opt/rustup/bin:/opt/homebrew/opt/lld/bin:$(PATH)

.PHONY: help setup check build test test-unit test-e2e test-e2e-clean test-e2e-mongo-parquet run stop psql fmt clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# --- Setup ---

setup: setup-brew-deps setup-rustup setup-direnv ## Install all dev dependencies (rustup, lld, cmake, direnv allow)

setup-brew-deps: ## Install Homebrew dependencies (lld, cmake, mongo-scaffold)
	@command -v lld >/dev/null 2>&1 && echo "lld already available" || brew install lld
	@command -v cmake >/dev/null 2>&1 && echo "cmake already available" || brew install cmake
	@command -v mongo-cluster >/dev/null 2>&1 && echo "mongo-scaffold already available" || brew install --cask zph/mongo-scaffold/mongo-scaffold

setup-rustup: ## Install rustup via Homebrew (provides nightly toolchain per rust-toolchain.toml)
	@command -v rustup >/dev/null 2>&1 && echo "rustup already available" || brew install rustup
	@rustup show active-toolchain 2>/dev/null || rustup default nightly-2025-10-10

setup-direnv: ## Allow direnv for this project (.envrc puts rustup + lld on PATH)
	@command -v direnv >/dev/null 2>&1 && direnv allow . || echo "direnv not found; PATH is set via Makefile fallback"

# --- Build & Check ---

check: ## Run clippy + compile checks (./risedev c)
	./risedev c

build: ## Build the project (./risedev b)
	./risedev b

fmt: ## Format code with cargo fmt
	cargo fmt

# --- Test ---

test: ## Run all connector unit tests for mongodb_oplog
	cargo test -p risingwave_connector mongodb_oplog

test-integration: ## Run integration tests (requires Docker/Podman + mongo-scaffold)
	cargo test -p risingwave_connector mongodb_oplog::integration_tests -- --ignored

test-unit: ## Run unit tests for a crate (usage: make test-unit CRATE=risingwave_connector FILTER=mongodb)
	cargo test -p $(CRATE) $(FILTER)

test-e2e: ## Run E2E SLT tests (usage: make test-e2e SLT='./e2e_test/source_inline/mongodb_oplog/*.slt')
	./risedev slt '$(SLT)'

test-e2e-clean: ## Clean E2E test state before re-run (usage: make test-e2e-clean SLT='./path/to/test.slt')
	./risedev slt-clean '$(SLT)'

test-e2e-mongo-parquet: ## E2E: mongo-oplog source -> S3 Parquet sink (requires risedev + mup)
	uv run e2e_test/s3/mongo_oplog_parquet_sink.py

# --- Run ---

run: ## Start RisingWave instance in background (./risedev d)
	./risedev d

stop: ## Stop RisingWave instance (./risedev k)
	./risedev k

psql: ## Connect to running RisingWave via psql (usage: make psql Q='SELECT 1')
	./risedev psql -c "$(Q)"

# --- Clean ---

clean: ## Clean build artifacts
	cargo clean
