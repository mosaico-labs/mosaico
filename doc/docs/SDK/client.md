---
title: Client
description: Architecture and design of the MosaicoClient.
---

API Reference: [`mosaicolabs.comm.MosaicoClient`][mosaicolabs.comm.MosaicoClient].

The `MosaicoClient` is a resource manager designed to orchestrate three distinct **Layers** of communication and processing. 
This layered architecture ensures that high-throughput sensor data does not block critical control operations or application logic.

Creating a new client is done via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] factory method. It is recomended to always use the client inside a `with` context to ensure resources in all layers are cleanly released.

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Logic goes here
    pass
# Pools and connections are closed automatically

```

## Control Layer

A single, dedicated connection is maintained for metadata operations. 
This layer handles lightweight tasks such as creating sequences, querying the catalog, and managing schema definitions. 
By isolating control traffic, the client ensures that critical commands (like `sequence_finalize`) are never queued behind heavy data transfers.

## Data Layer

For high-bandwidth data ingestion (e.g., uploading 4x 1080p cameras simultaneously), the client maintains a **Connection Pool** of multiple Flight clients. 
The SDK automatically stripes writes across these connections in a round-robin fashion, allowing the application to saturate the available network bandwidth.

## Processing Layer

Serialization of complex sensor data (like compressing images or encoding LIDAR point clouds) is CPU-intensive. 
The SDK uses an **Executor Pool** of background threads to offload these tasks. 
This ensures that while one thread is serializing the *next* batch of data, another thread is already transmitting the *previous* batch over the network.

As a senior architect, it is vital to emphasize that the **Security Layer** is not an "add-on" but a foundational component of the `MosaicoClient` lifecycle. In robotics, where data often moves from edge devices to centralized clusters, this layer ensures that your Physical AI assets remain protected against unauthorized access and intercept.

## Security Layer

The Security Layer manages the confidentiality and integrity of the communication channel. It is composed of two primary mechanisms that work in tandem to harden the connection.

### 1. Encryption (TLS)
For deployments over public or shared networks, the client supports [**Transport Layer Security (TLS)**](../daemon/tls.md). By providing a `tls_cert_path`, the client automatically switches from an insecure channel to an encrypted one. The SDK handles the heavy lifting of reading the certificate bytes and configuring the underlying Flight/gRPC drivers to verify the server's identity and encrypt the data stream.

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
