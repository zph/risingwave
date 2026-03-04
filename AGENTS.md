# Coding Agent Instructions for RisingWave

## Project Overview

RisingWave is a Postgres-compatible streaming database that offers the simplest and most cost-effective way to process, analyze, and manage real-time event data — with built-in support for the Apache Iceberg™ open table format.

RisingWave components are developed in Rust and split into several crates:

1. `config` - Default configurations for servers
2. `prost` - Generated protobuf rust code (grpc and message definitions)
3. `stream` - Stream compute engine
4. `batch` - Batch compute engine for queries against materialized views
5. `frontend` - SQL query planner and scheduler
6. `storage` - Cloud native storage engine
7. `meta` - Meta engine
8. `utils` - Independent util crates
9. `cmd` - All binaries, `cmd_all` contains the all-in-one binary `risingwave`
10. `risedevtool` - Developer tool for RisingWave

## Coding Style

- Always write code comments in English.
- Write simple, easy-to-read and easy-to-maintain code.
- Use `cargo fmt` to format the code if needed.
- Follow existing code patterns and conventions in the repository.

## Build, Run, Test

You may need to learn how to build and test RisingWave when implementing features or fixing bugs.

### Build & Check

- Use `./risedev b` to build the project.
- Use `./risedev c` to check if the code follow rust-clippy rules, coding styles, etc.

### Unit Test

- Integration tests and unit tests are valid Rust/Tokio tests, you can locate and run those related in a standard Rust way.
- Parser tests: use `./risedev update-parser-test` to regenerate expected output.
- Planner tests: use `./risedev run-planner-test [name]` to run and `./risedev do-apply-planner-test` (or `./risedev dapt`) to update expected output.

### End-to-End Test

- Use `./risedev d` to run a RisingWave instance in the background via tmux.
  - It builds RisingWave binaries if necessary. The build process can take up to 10 minutes, depending on the incremental build results. Use a large timeout for this step, and be patient.
  - It kills the previous instance, if exists.
  - Optionally pass a profile name (see `risedev.yml`) to choose external services or components. By default it uses `default`.
  - This runs in the background so you do not need a separate terminal.
  - You can connect right after the command finishes.
  - Logs are written to files in `.risingwave/log` folder.
- Use `./risedev k` to stop a RisingWave instance started by `./risedev d`.
- Only when a RisingWave instance is running, you can use `./risedev psql -c "<your query>"` to run SQL queries in RW.
- Only when a RisingWave instance is running, you can use `./risedev slt './path/to/e2e-test-file.slt'` to run end-to-end SLT tests.
  - File globs like `/**/*.slt` is allowed.
  - Failed run may leave some objects in the database that interfere with next run. Use `./risedev slt-clean ./path/to/e2e-test-file.slt` to reset the database before running tests.
- The preferred way to write tests is to write tests in SQLLogicTest format.
- Tests are put in `./e2e_test` folder.

### Sandbox escalation

When sandboxing is enabled, these commands need `require_escalated` because they bind or connect to local TCP sockets:

- `./risedev d` and `./risedev p` (uses local ports and tmux sockets)
- `./risedev psql ...` or direct `psql -h localhost -p 4566 ...` (local TCP connection)
- `./risedev slt './path/to/e2e-test-file.slt'` (connects to local TCP via psql protocol)
- Any command that checks running services via local TCP (for example, health checks or custom SQL clients)

## Validating Changes

### Prerequisites

- **Toolchain**: Project requires nightly Rust (see `rust-toolchain.toml`). Use `rustup` (not Homebrew `rust`).
- **direnv**: `.envrc` prepends `/opt/homebrew/opt/rustup/bin` to PATH so rustup-managed nightly is used.

### Quick Validation Checklist

1. **Compile check** (fastest): `./risedev c` — runs clippy + compile checks
2. **Build**: `./risedev b` — full build
3. **Unit tests for a crate**: `cargo test -p risingwave_connector <test_filter>` — run specific tests
4. **Run instance**: `./risedev d` — starts RisingWave in background (builds if needed, can take ~10 min)
5. **SQL queries**: `./risedev psql -c "<query>"` — requires running instance
6. **E2E SLT tests**: `./risedev slt './path/to/test.slt'` — requires running instance
7. **Stop instance**: `./risedev k`

### Connector-Specific Validation

- **Connector unit tests**: `cargo test -p risingwave_connector mongodb_oplog`
- **Parser tests**: `./risedev update-parser-test` to regenerate expected output
- **Planner tests**: `./risedev run-planner-test [name]` to run, `./risedev dapt` to update expected output
- **Format check**: `cargo fmt` to format code

## Connector Development

See `docs/dev/src/connector/intro.md`.

### MongoDB Version Requirement

- **Minimum MongoDB server version: 3.6** — this is non-negotiable
- Use `mongodb` Rust crate **v2.8.2** (v2.x line), NOT v3.x (which drops 3.6 support)
- All oplog tailing features (tailable cursors, replSetGetStatus, readConcern majority) are available in 3.6+
