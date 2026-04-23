---
title: Client
description: Architecture and design of the MosaicoClient.
---

??? question "API Reference"
    [`mosaicolabs.comm.MosaicoClient`][mosaicolabs.comm.MosaicoClient].

The `MosaicoClient` is a resource manager designed to orchestrate distinct **Layers** of communication and processing. 
This layered architecture ensures that high-throughput sensor data does not block critical control operations or application logic.

Creating a new client is done via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] factory method. It is recomended to always use the client inside a `with` context to ensure resources in all layers are cleanly released.

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Logic goes here
    pass
# The connection with the server is closed automatically

```

## Control Layer
A single connection is maintained for metadata operations, schema management, and lifecycle control. This layer handles lightweight tasks such as creating sequences and querying the catalog. By using a dedicated control flow, the client ensures that critical state commands (like `sequence_finalize`) remain responsive and synchronized with the data stream.

## Data Layer
Data ingestion and retrieval are handled through a **unified single-stream connection**. By consolidating data traffic into a single pipe, the SDK reduces network overhead and ensures strict sequential ordering of data packets. This simplified approach eliminates the complexity of connection handshakes and is optimized for stable, predictable throughput in environments where network jitter must be minimized.

## Processing Layer
The SDK operates on a **Synchronous Processing Model**, where serialization and transmission occur within the calling thread’s context. For complex sensor data (like image compression or LIDAR encoding), the SDK performs inline serialization immediately prior to transmission. This "lock-step" execution ensures maximum data integrity and minimizes memory pressure, as it avoids the overhead of background thread synchronization and context switching.

## Security Layer
The Security Layer manages the confidentiality and integrity of the communication channel. It is composed of two primary mechanisms that work in tandem to harden the connection.

### 1. Encryption (TLS)
For deployments over public or shared networks, the client supports [**Transport Layer Security (TLS)**](../daemon/tls.md). Two connection modes are available via SDK, depending on the configuratiom of the Mosaico server:

* **One-way TLS**, server authenticated only
  ```python
  MosaicoClient.connect("localhost", 6726, enable_tls=True)
  ```
* **Two-way TLS**, via providing a certificate file
  ```python
  MosaicoClient.connect("localhost", 6726, tls_cert_path="my/cert/file/path.pem")
  ```

In both modes, the client automatically switches from an insecure channel to an encrypted one via `grpc+tls` protocol. The **One-way TLS** mode is available by setting `enable_tls=True` when calling [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect], and providing no certificate. 
On the other hand, the **Two-way TLS** mode is available by passing the TLS certificate path via `tls_cert_path` parameter when calling [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect]. 
In such a case, there is no need to set the `enable_tls=True` flag.

### 2. Authentication (API Key)
Mosaico uses an [**API Key** system](../daemon/api_key.md) to authorize every operation. When a key is provided, the client automatically attaches your unique credentials to the metadata of every gRPC and Flight call. This ensures that even if your endpoint is public, only requests with a valid, non-revoked key are processed by the server.

The client supports 4 permission levels, each with increasing privileges:

| Permission | Description |
| :--- | :--- |
| [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read] | Read-Only access to resources |
| [`APIKeyPermissionEnum.Write`][mosaicolabs.enum.APIKeyPermissionEnum.Write] | Write access to resources (Create and update sequences)|
| [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete] | Delete access to resources (Delete sequences, sessions and topics)|
| [`APIKeyPermissionEnum.Manage`][mosaicolabs.enum.APIKeyPermissionEnum.Manage] | Full access to resources + Manage API keys (create, retrieve the status, revoke)|


```python
from mosaicolabs import MosaicoClient

# The client handles the handshake and credential injection automatically
with MosaicoClient.connect(
    host="mosaico.production.cluster",
    port=6726,
    api_key="<msco_secret_key>",
) as client:
    print(client.version())
```

!!! warning "API-Key and TLS"
    TLS is not mandatory for connecting via API-keys. It is recommended to enable the support for TLS in the server, to avoid sensitive credential to be sent on unencrypted channels.

### Recommended Patterns

#### Explicit Connection
Ideal for local development or scripts where credentials are managed by a secrets manager.

```python
from mosaicolabs import MosaicoClient

# The client handles the handshake and credential injection automatically
with MosaicoClient.connect(
    host="mosaico.production.cluster",
    port=6726,
    api_key="<msco_secret_key>",
    tls_cert_path="/etc/mosaico/certs/ca.pem"
) as client:
    print(client.version())
```

#### Environment-Based Configuration (`from_env`)
As a best practice for production and containerized environments (Docker/Kubernetes), the `MosaicoClient` supports **Zero-Config Discovery** via [`MosaicoClient.from_env`][mosaicolabs.comm.MosaicoClient.from_env]. This prevents sensitive keys from ever being hardcoded in your source files.

The client automatically searches for:
* `MOSAICO_API_KEY`: Your authentication token.
* `MOSAICO_TLS_CERT_FILE`: The path to your CA certificate.

```python
import os
from mosaicolabs import MosaicoClient

# Standardize your deployment by using environment variables
# export MOSAICO_API_KEY=... 
# export MOSAICO_TLS_CERT_FILE=...

with MosaicoClient.from_env(host="mosaico.internal", port=6726) as client:
    # Security is initialized automatically from the environment
    print(client.version())
```

## Compression

The `MosaicoClient` supports **gRPC-level compression** to reduce bandwidth usage when transmitting data over the network. Compression is applied transparently to all record batches sent through the Data Layer.

Compression is configured via the `compression` parameter of [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect]. It accepts either a [`GRPCCompressionAlgorithm`][mosaicolabs.enum.GRPCCompressionAlgorithm] shorthand (**algorithm only**, no level control) or a [`GRPCCompression`][mosaicolabs.comm.GRPCCompression] dataclass (full control over both **algorithm and level**).

The library supports the following compression algorithms:

| Algorithm | Description |
| :--- | :--- |
| [`GRPCCompressionAlgorithm.Null`][mosaicolabs.enum.GRPCCompressionAlgorithm.Null] | No compression (default). Data is transmitted uncompressed. |
| [`GRPCCompressionAlgorithm.Gzip`][mosaicolabs.enum.GRPCCompressionAlgorithm.Gzip] | Standard GZIP compression. Best for general-purpose use. |
| [`GRPCCompressionAlgorithm.StreamGzip`][mosaicolabs.enum.GRPCCompressionAlgorithm.StreamGzip] | Experimental stream-level GZIP. Applied to the raw gRPC stream rather than individual messages. |

When using an algorithm that supports intensity tuning, an optional [`GRPCCompressionLevel`][mosaicolabs.enum.GRPCCompressionLevel] can be specified to trade off CPU usage against compression ratio.

| Level | Description |
| :--- | :--- |
| [`GRPCCompressionLevel.Low`][mosaicolabs.enum.GRPCCompressionLevel.Low] | Fastest compression, lowest CPU overhead, largest output size. |
| [`GRPCCompressionLevel.Medium`][mosaicolabs.enum.GRPCCompressionLevel.Medium] | Balanced trade-off between speed and compression ratio. |
| [`GRPCCompressionLevel.High`][mosaicolabs.enum.GRPCCompressionLevel.High] | Highest compression ratio, greatest CPU overhead. |

If no level is specified, the underlying gRPC protocol implementation chooses a sensible default for the selected algorithm.

#### Shorthand (algorithm only)

Pass a [`GRPCCompressionAlgorithm`][mosaicolabs.enum.GRPCCompressionAlgorithm] value directly. The compression level is left to the gRPC default.

```python
from mosaicolabs import MosaicoClient
from mosaicolabs.enum import GRPCCompressionAlgorithm

with MosaicoClient.connect(
    "localhost",
    6726,
    compression=GRPCCompressionAlgorithm.Gzip,
) as client:
    # ... Perform operations
```

#### Full control (algorithm + level)

Use the [`GRPCCompression`][mosaicolabs.comm.GRPCCompression] dataclass to explicitly set both the algorithm and the compression level.

```python
from mosaicolabs import MosaicoClient
from mosaicolabs.comm import GRPCCompression
from mosaicolabs.enum import GRPCCompressionAlgorithm, GRPCCompressionLevel

with MosaicoClient.connect(
    "localhost",
    6726,
    compression=GRPCCompression(
        algorithm=GRPCCompressionAlgorithm.Gzip,
        level=GRPCCompressionLevel.High,
    ),
) as client:
    # ... Perform operations
```

!!! tip "When to enable compression"
    Compression is most beneficial when the network link is the bottleneck (e.g., remote deployments over WAN or metered connections). For local or LAN deployments, the CPU overhead of compression may outweigh the bandwidth savings, so the default `GRPCCompressionAlgorithm.Null` is recommended.

!!! warning "StreamGzip is experimental"
    `GRPCCompressionAlgorithm.StreamGzip` applies compression at the raw gRPC stream level. It is currently marked as **experimental** and may not be stable in all environments. Prefer `GRPCCompressionAlgorithm.Gzip` for production use.
