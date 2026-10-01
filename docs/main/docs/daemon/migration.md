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

### Environment Variables

#### gRPC message size configuration

`MOSAICOD_MAX_GRPC_MESSAGE_SIZE` has been replaced by two independently configurable limits, decoupling the incoming (decode) and outgoing (encode) message size:

```diff
- MOSAICOD_MAX_GRPC_MESSAGE_SIZE=52428800
+ MOSAICOD_GRPC_MAX_DECODE_MESSAGE_SIZE=52428800
+ MOSAICOD_GRPC_TARGET_ENCODE_MESSAGE_SIZE=26214400
```

`MOSAICOD_TARGET_MESSAGE_SIZE` is removed. In 0.6 it was not configurable (always derived as half of `MOSAICOD_MAX_GRPC_MESSAGE_SIZE`); in 0.7 it is replaced by `MOSAICOD_GRPC_TARGET_ENCODE_MESSAGE_SIZE`, which is independently configurable and defaults to `25 MiB`.

The allowed range for the decode limit also widened from `4 MiB`-`128 MiB` to `4 MiB`-`512 MiB`. The corresponding `config.toml` keys are `server.grpc.max_decode_message_size` and `server.grpc.target_encode_message_size`. See the [environment variables reference](env.md#general).

### Actions

#### `info`

The `VERSION` action has been replaced by `INFO`, which, in addition to the version string, reports the server's configured gRPC message size limits:

* `grpc_max_decode_message_size`
* `grpc_max_encode_message_size`
* `grpc_target_encode_message_size`

### Store optimizer

Version 0.7 introduces the [store optimization routine](store_optimizer.md), which rewrites a topic's small chunks into fewer, larger ones. Topics ingested before the upgrade are optimized as well. It is recommended to run it periodically, alongside the [cleanup routine](cleanup.md):

```bash
mosaicod store-optimizer --time-interval 3600
```

### What's new

* **Config file**: `mosaicod` can now be configured through a [`config.toml` file](config.md), as an alternative to environment variables.
* **`mosaicod ps`**: lists the running `server`, `cleanup` and `store-optimizer` instances. See the [CLI reference](cli.md#mosaicod-ps).
