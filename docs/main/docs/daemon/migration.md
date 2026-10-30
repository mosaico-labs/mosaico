---
title: Migration Guide
sidebar_position: 14
toc_max_heading_level: 2
description: "Step-by-step instructions for upgrading mosaicod between versions."
---

import CollapsibleSection from '@site/src/components/CollapsibleSection';

This page collects the steps required to upgrade `mosaicod` from one version to the next. Each section covers a single upgrade path; if you are skipping versions, apply the sections in order.

Database schema migrations are applied automatically when `mosaicod` starts. Each migration runs inside its own transaction: if it fails, its changes are rolled back, your data is left untouched and `mosaicod` refuses to start. Migrations are not reversible, so **back up your database before upgrading** if you want to be able to roll back to a previous version.

<CollapsibleSection>

## From 0.6 to 0.7

### Database schema changes

This release changes the database schema. The new migrations are applied automatically on the first startup of `mosaicod` 0.7, but a database created with `mosaicod` 0.6 or earlier needs a manual fix first.

`mosaicod` 0.7 ships an already released migration, `20251110134258_data_catalog.sql`, with a changed comment. The SQL it runs is the same, but the checksum recorded in the `_sqlx_migrations` table no longer matches. Because of this, `mosaicod` refuses to start and reports a migration error with this cause:

```
migration 20251110134258 was previously applied but has been modified
```

Databases created from scratch with `mosaicod` 0.7 are not affected.

To fix it, run the following script against your database **before** starting `mosaicod` 0.7. It replaces the old checksum with the new one only if the database still holds the 0.6 checksum, so it is safe to run more than once and on databases that don't need it.

```sql title="fix_migration_checksum_0.7.sql"
DO $$
DECLARE
    v_version  CONSTANT bigint := 20251110134258;
    v_old      CONSTANT bytea  := decode('1180e69f10ad0e6de1631831c506f33a8373de04ce382ab39ddb5c4da6134fbd7376974ef99c938380b21cbc1bf4ff79', 'hex');
    v_new      CONSTANT bytea  := decode('9b32757639cc56e0d7badef1cca6366696bb2c4dfdaf7d8bfe59731ea6e6c4ad33f44f8ffa94a979983b26d321525de6', 'hex');
    v_current  bytea;
BEGIN
    IF to_regclass('_sqlx_migrations') IS NULL THEN
        RAISE NOTICE '_sqlx_migrations not found: nothing to fix (fresh database?)';
        RETURN;
    END IF;

    SELECT checksum INTO v_current FROM _sqlx_migrations WHERE version = v_version;

    IF NOT FOUND THEN
        RAISE NOTICE 'migration % not applied yet: nothing to fix', v_version;
    ELSIF v_current = v_new THEN
        RAISE NOTICE 'migration % already has the 0.7 checksum: nothing to fix', v_version;
    ELSIF v_current = v_old THEN
        UPDATE _sqlx_migrations SET checksum = v_new WHERE version = v_version;
        RAISE NOTICE 'migration % checksum updated to 0.7', v_version;
    ELSE
        RAISE EXCEPTION 'migration % has an unknown checksum (%): not touching it', v_version, encode(v_current, 'hex');
    END IF;
END
$$;
```

Save it to a file and run it with `psql`, using the same database and credentials configured for `mosaicod`:

```bash
psql "postgresql://<user>:<password>@<host>:<port>/<database>" -f fix_migration_checksum_0.7.sql
```

The script prints a `NOTICE` describing what it did. If it stops with an `unknown checksum` error, the recorded checksum doesn't match any `mosaicod` release and nothing is changed: don't start `mosaicod` and [open a discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue).

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

</CollapsibleSection>

## From 0.7 to 0.8

### Database schema changes

This release changes the database schema. The migrations are applied automatically on the first startup of `mosaicod` 0.8.

### Flight endpoints

#### `list_flights`

`list_flights` can now return sequences one page at a time, and `Criteria.expression` must be a serialized `ListFlightsCmd`. See [Sequence List](retrieval.md#sequence-list) for the request format and pagination.

An empty or raw `"/"` expression is now rejected with `InvalidArgument`, like any expression that is not a valid `ListFlightsCmd`. To keep listing every sequence:

```diff
- flight_info = list_flights("")
+ cmd = ListFlightsCmd(order_by=SEQUENCE_ORDER_CREATED_AT)
+ flight_info = list_flights(cmd.SerializeToString())
```

Upgrade the Python SDK together with the server. Older SDK versions send an empty expression from `MosaicoClient.list_sequences()`, which a 0.8 server rejects.
