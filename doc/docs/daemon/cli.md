# CLI Reference

## mosaicod run

Start the server locally

```bash
mosaicod run [OPTIONS]
```

### Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `--host` | `false` | Listen on all addresses, including LAN and public addresses. |
| `--port <PORT>` | `6726` | Port to listen on. |
| `--local-store <PATH>` | `None` | Enable storage of objects on the local filesystem at the specified directory path. |
| `--tls` | `false` | Enable TLS. When enabled, the following envirnoment variables needs to be set `MOSAICOD_TLS_CERT_FILE`: certificate file path, `MOSAICOD_TLS_PRIVATE_KEY_FILE`: private key file path | 


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
mosaicod api-key create [OPTIONS]
```

| Option | Description |
| :--- | :--- |
| `-d, --description` | Set a description for the API key to make it easily recognizable. |
| `-e, --expires` |  Define a time duration (using the ISO8601 format) after which the API key in no longer valid. |

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

