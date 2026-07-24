---
title: CLI Reference
sidebar_position: 8
description: "Complete CLI reference for the mosaicod binary. Documents all subcommands (server, cleanup, key management, and others) with their flags, default values, and usage examples."
---

## mosaicod server

Start the server locally.

```bash
mosaicod server [OPTIONS]
```

### Options

| Option | Default | Description |
| :--- | --- | :--- |
| `--host <HOST>` | `127.0.0.1` |  Specify a host address. |
| `--port <PORT>` | `6726` | Port to listen on. |
| `--tls` | `false` | Enable TLS. When enabled, the following envirnoment variables needs to be set `MOSAICOD_TLS_CERT_FILE` and `MOSAICOD_TLS_PRIVATE_KEY_FILE` | 
| `--gzip` | `false` | Enable gzip compression for both incoming and outgoing messages. |
| `--api-key` | `false` | Require API keys to operate. When enabled the system will require API keys to perform any actions. |

## mosaicod cleanup

Run the store [cleanup routine](cleanup.md), which permanently purges orphaned files from the object store.

```bash
mosaicod cleanup [OPTIONS]
```

### Options

| Option | Default | Description |
| :--- | --- | :--- |
| `--time-interval <TIME_INTERVAL>` | `0` | Minimum interval, in seconds, between a cleanup run and the next one. When set to `0` a single cleanup is performed and then the process terminates. Any value greater than `0` runs the cleanup routine in a loop, sleeping `time_interval` seconds between runs. |
| `--retention-duration <RETENTION_DURATION>` | `86400` | Maximum period, in seconds, an obsolete file is kept in the store before being permanently deleted. Set it to `0` to delete obsolete files right away. |

#### Examples

Perform a single cleanup and exit (one-shot):

```bash
mosaicod cleanup
```

Run the cleanup routine continuously, every hour:

```bash
mosaicod cleanup --time-interval 3600
```

Run the cleanup routine every hour, keeping obsolete files for 24 hours before purging them:

```bash
mosaicod cleanup --time-interval 3600 --retention-duration 86400
```

Perform a single cleanup that deletes obsolete files immediately, without any retention period:

```bash
mosaicod cleanup --retention-duration 0
```

## mosaicod api-key

Manage API keys.

### create

Create a new API key.

```bash
mosaicod api-key create --permissions <PERMISSIONS> [OPTIONS]
```

`<PERMISSIONS>` is one or more of `read`, `write`, `delete`, combined with `|` (e.g. `read` or `read|write`).

| Option | Default | Description |
| :--- | --- | :--- |
| `-d, --description` | | Set a description for the API key to make it easily recognizable. |
| `--expires-in <EXPIRES_IN>` | | Define a time duration, using the ISO8601 format, after which the key in no longer valid (e.g. `P1Y2M3D` 1 year 2 months and 3 days) |
| `--expires-at <EXPIRES_AT>` | | Define a datetime, using the rfc3339 format, after which the key in no longer valid (e.g `2026-03-27T12:20:00Z`) |

#### Examples

Read-only key:

```bash
mosaicod api-key create --permissions "read"
```

Read and write:

```bash
mosaicod api-key create --permissions "read|write"
```

Full access:

```bash
mosaicod api-key create --permissions "read|write|delete"
```

### revoke

Revoke an existing API key.

```bash
mosaicod api-key revoke <FINGERPRINT>
```
The [fingerprint](api_key.md#token-structure) are the last 8 digits of the API key.

### status

Check the status of an API key.

```bash
mosaicod api-key status <FINGERPRINT>
```

The [fingerprint](api_key.md#token-structure) are the last 8 digits of the API key.

### list

List all API keys.

```bash
mosaicod api-key list
```

### purge
Remove API keys in bulk. By default, only expired keys are removed; use the `--all` flag to remove every key, regardless of its expiration status.

```bash
mosaicod api-key purge [OPTIONS]
```

| Option          |  Description                                                        |
| --------------- |  ------------------------------------------------------------------ |
| `-A`, `--all`   |  Remove **all** API keys, including those that have not yet expired. |

#### Examples

Remove only expired keys:

```bash
mosaicod api-key purge
```

Remove all API key:

```bash
mosaicod api-key purge --all
```

or, equivalently:

```bash
mosaicod api-key purge -A
```

:::warning
    Using `--all` is irreversible: every API key will be permanently revoked, including keys currently in use by services or integrations. Make sure you have a way to reissue the keys before running this command.
:::

## Common Options

Each `mosaicod` command shares the following common options:

| Options| Default | Description |
| :--- | --- | :--- |
| `--log-format <LOG_FORMAT>` | `pretty` | Set the log output format. Available values are: `json`, `pretty`, `plain`|
| `--log-level <LOG_LEVEL>` | `warning` | Set the log level. Possible values: warning, info, debug |