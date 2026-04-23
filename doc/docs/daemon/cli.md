# CLI Reference

## mosaicod run

Start the server locally

```bash
mosaicod run [OPTIONS]
```

### Options

| Option | Default | Description |
| :--- | --- | :--- |
| `--host <HOST>` | `127.0.0.1` |  Specify a host address. |
| `--port <PORT>` | `6726` | Port to listen on. |
| `--tls` | `false` | Enable TLS. When enabled, the following envirnoment variables needs to be set `MOSAICOD_TLS_CERT_FILE` and `MOSAICOD_TLS_PRIVATE_KEY_FILE` | 
| `--api-key` | `false` | Require API keys to operate. When enabled the system will require API keys to perform any actions. |

## mosaicod api-key

Manage API keys.

### Subcommands

|Command|Description|
|---|---|
|`create`|Create a new API key with a custom scope|
|`revoke`|Revoke an existing API key|
|`status`|Check the status of an API key|
|`list`|List all API keys|

### mosaicod api-key create

Create a new API key.

```bash
mosaicod api-key create --permission [read|write|delete|manage] [OPTIONS]
```

| Option | Default | Description |
| :--- | --- | :--- |
| `-d, --description` | | Set a description for the API key to make it easily recognizable. |
| `--expires-in <EXPIRES_IN>` | | Define a time duration, using the ISO8601 format, after which the key in no longer valid (e.g. `P1Y2M3D` 1 year 2 months and 3 days) |
| `--expires-at <EXPIRES_AT>` | | Define a datetime, using the rfc3339 format, after which the key in no longer valid (e.g `2026-03-27T12:20:00Z`) |

### mosaicod api-key revoke

Revoke an existing API key.

```bash
mosaicod api-key revoke <FINGERPRINT>
```
The [fingerprint](api_key.md#token-structure) are the last 8 digits of the API key.

### mosaicod api-key status

Check the status of an API key.

```bash
mosaicod api-key status <FINGERPRINT>
```

The [fingerprint](api_key.md#token-structure) are the last 8 digits of the API key.

### mosaicod api-key list

List all API keys.

```bash
mosaicod api-key list
```


## Common Options

Each `mosaicod` command shares the following common options:

| <div style="width:10rem">Options</div>| Default | Description |
| :--- | --- | :--- |
| `--log-format <LOG_FORMAT>` | `pretty` | Set the log output format. Available values are: `json`, `pretty`, `plain`|
| `--log-level <LOG_LEVEL>` | `warning` | Set the log level. Possible values: warning, info, debug |
