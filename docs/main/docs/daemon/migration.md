---
title: Migration Guide
sidebar_position: 14
toc_max_heading_level: 2
description: "Step-by-step instructions for upgrading mosaicod between versions."
---

This page collects the steps required to upgrade `mosaicod` from one version to the next. Each section covers a single upgrade path; if you are skipping versions, apply the sections in order.

Database schema migrations are applied automatically when `mosaicod` starts. Each migration runs inside its own transaction: if it fails, its changes are rolled back, your data is left untouched and `mosaicod` refuses to start. Migrations are not reversible, so **back up your database before upgrading** if you want to be able to roll back to a previous version.

## From 0.6 to 0.7

### Database schema changes

This release changes the database schema. The migrations are applied automatically on the first startup of `mosaicod` 0.7.

### Database credentials

Credentials are no longer part of `MOSAICOD_DB_URL`. Move the user and password to the dedicated [environment variables](env.md#dbms):

```diff
- MOSAICOD_DB_URL=postgresql://postgres:password@localhost:5432/mosaico
+ MOSAICOD_DB_URL=postgresql://localhost:5432/mosaico
+ MOSAICOD_DB_USER=postgres
+ MOSAICOD_DB_PASSWORD=password
```

The password can also be read from a file by setting `MOSAICOD_DB_PASSWORD_FILE` in place of `MOSAICOD_DB_PASSWORD`.

### Server flags

The `--tls`, `--gzip` and `--api-key` options of `mosaicod server` have been removed. Use the corresponding environment variables instead:

| 0.6 | 0.7 |
| :--- | :--- |
| `--tls` | `MOSAICOD_TLS_ENABLED=true` |
| `--gzip` | `MOSAICOD_GZIP_ENABLED=true` |
| `--api-key` | `MOSAICOD_API_KEY_ENABLED=true` |

The `--host` and `--port` options are unchanged. See the [CLI reference](cli.md#mosaicod-server) for details.

### Removed environment variables

* `MOSAICOD_TARGET_MESSAGE_SIZE`: no longer configurable. It is now always derived as half of [`MOSAICOD_MAX_GRPC_MESSAGE_SIZE`](env.md#mosaicod-max-grpc-message-size), which must be set between `4 MiB` and `128 MiB`.

### Store optimizer

Version 0.7 introduces the [store optimization routine](store_optimizer.md), which rewrites a topic's small chunks into fewer, larger ones. Topics ingested before the upgrade are optimized as well. It is recommended to run it periodically, alongside the [cleanup routine](cleanup.md):

```bash
mosaicod store-optimizer --time-interval 3600
```

### What's new

* **Config file**: `mosaicod` can now be configured through a [`config.toml` file](config.md), as an alternative to environment variables.
* **`mosaicod ps`**: lists the running `server`, `cleanup` and `store-optimizer` instances. See the [CLI reference](cli.md#mosaicod-ps).
