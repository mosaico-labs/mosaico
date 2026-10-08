--- 
title: Testing & Development
position: 2
description: "Development environment setup and contribution procedures for the Mosaico project. Covers local orchestration, database schema migration, and the test suite required before submitting changes."
---

This section outlines the standard procedures for contributing to the project. It covers local environment orchestration, database schema management, and the verification suite required to maintain code quality.

## Developing the Daemon

The development lifecycle relies on a containerized infrastructure that includes a preconfigured **PostgreSQL** instance and a **MinIO** object store.

### Environment Setup

Initialize the required services using the provided docker compose file:

```bash
cd docker/devel
docker compose up -d
```

To configure the application for the development environment, initialize your local `.env` file:

```bash
cd mosaicod
cp env.devel .env
```

This configuration exports the `MOSAICOD_DB_URL`, database credentials, storage backend variables, and the `DATABASE_URL` required for compile-time query verification by [sqlx](https://github.com/launchbadge/sqlx).

### Update sqlx Queries Cache

If you modify SQL queries, you **must** refresh the offline metadata cache to allow the project to compile. Run the following command to update the cache:

```bash
cd mosaicod/crates/mosaicod-db
cargo sqlx prepare -- --features postgres
```

### Apply Migrations

As detailed in the [Setup](../daemon/install.md) guide, the build process validates queries against the schema. Ensure your local database is synchronized with the latest migrations:

```bash 
cd mosaicod/crates/mosaicod-db
cargo sqlx migrate run
```

### Execution

You can execute the daemon directly from the source tree:

```bash
cd mosaicod
cargo run -- run [OPTIONS]
```

or compiled with:

```bash
cd mosaicod
cargo build --bin mosaicod --profile [PROFILE]
```

The current available prfiles are:

- `debug`: Default profile, optimized for development with debug symbols and no optimizations.
- `release`: Optimized for production, good amount of optimizations, smaller binary size, and few link-time optimizations but with no debug symbols.
- `docker`: As release but without link-time optimizations to reduce the binary size and speed up compilation. 
- `optimized`: Profile with high optimizations, slow to compile but with the best runtime performance. 

### Tests

Daemon specific test can be executed with:

```bash
cd mosaicod
cargo test
```

### Linting

Ensure compliance with the project's style guidelines and static analysis rules before submitting a pull request

```bash
cd mosaicod
cargo lint
```

## Developing the Python SDK

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) 0.11.23 or newer, then set up the SDK with Python 3.10 or newer:

```bash
cd mosaico-sdk-py
uv sync --locked --extra cli
uv run --locked --extra cli pre-commit install
uv run --locked --extra cli mosaico --help
```

uv creates a local `.venv` and installs the SDK, CLI extra, and development tools from `uv.lock`. Include `--extra cli` when running SDK commands so synchronization keeps the optional CLI dependencies installed. An existing Poetry environment can be replaced by running the same sync command; Poetry is no longer required.

Use `uv add PACKAGE` for runtime dependencies, `uv add --dev PACKAGE` for development tools, and `uv lock --upgrade-package PACKAGE` for a targeted update. Commit both `pyproject.toml` and `uv.lock` when changing dependencies. Build distributable packages with `uv build` from the SDK directory.

The Python documentation has its own environment and lockfile and requires Python 3.11 or newer:

```bash
cd docs/py # From the repository root
uv sync --locked
uv run --locked mkdocs build
```

## Testing

The project includes a comprehensive suite of unit and integration tests to validate functionality and prevent regressions.

The test suite can be executed with:

```bash
./scripts/tests
```

Use `--help` to see available options.

## Development Environment

The project provides a script that generates a disposable development environment. This environment launches a clean `mosaicod` instance without any data, allowing you to test and interact with the daemon without needing to set up your own data.

This is particularly useful when developing client applications or testing the API, as it provides a consistent and isolated environment for experimentation.

The environment can be launched with:

```bash
./scripts/dev_env
```

Use `--help` to see available options.
