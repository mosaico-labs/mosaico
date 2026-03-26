# Testing & Development

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

This configuration exports the `MOSAICOD_DB_URL`, storage backend variables, and the `DATABASE_URL` required for compile-time query verification by [sqlx](https://github.com/launchbadge/sqlx).

### Update sqlx Queries Cache

If you modify SQL queries, you **must** refresh the offline metadata cache to allow the project to compile. Run the following command to update the cache:

```bash
cd mosaicod/crates/mosaicod-db
cargo sqlx prepare -- --features postgres
```

### Apply Migrations

As detailed in the [Setup](../daemon/setup.md) guide, the build process validates queries against the schema. Ensure your local database is synchronized with the latest migrations:

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

## Testing

The project includes a comprehensive suite of unit and integration tests to validate functionality and prevent regressions.

The test suite can be executed with:

```bash
./scripts/test.sh
```

Use `--help` to see available options.

## Development Environment

The project provides a script that generates a disposable development environment. This environment launches a clean `mosaicod` instance without any data, allowing you to test and interact with the daemon without needing to set up your own data.

This is particularly useful when developing client applications or testing the API, as it provides a consistent and isolated environment for experimentation.

The environment can be launched with:

```bash
./scripts/dev_env.sh
```

Use `--help` to see available options.