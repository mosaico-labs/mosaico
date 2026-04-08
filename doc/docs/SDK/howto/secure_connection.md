---
title: Secure Connection
description: Example how-to use TLS & API Key Authentication
---

By following this guide, you will learn how to:

1. **Initialize an Authenticated Session** using the `connect` method.
2. **Handle TLS Certificates** for encrypted communication.
3. **Use Environment Discovery** for production-grade credential management.

## Option 1: One-way (server authenticated) TLS - No certificate

Note that when `enable_tls=True` is provided to the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] method, the SDK automatically switches to an encrypted ([one-way](../client.md#1-encryption-tls)) gRPC channel.

```python
from mosaicolabs import MosaicoClient

# 1. Configuration constants
MOSAICO_HOST = "mosaico.production.yourdomain.com"
MOSAICO_PORT = 6726
# Your secret API key
MY_API_KEY = "msco_vy9lqa7u4lr7w3vimhz5t8bvvc0xbmk2_9c94a86"

# 2. Establish the secure connection
with MosaicoClient.connect(
    host=MOSAICO_HOST,
    port=MOSAICO_PORT,
    api_key=MY_API_KEY,      # Injects Auth Middleware
    enable_tls=True   # Enables one-way TLS encryption
) as client:
    # All operations inside this block are now encrypted and authenticated
    print(f"Connected to version: {client.version()}")
    sequences = client.list_sequences()
```

## Option 2: Connecting with a TLS certificate

Note that when `tls_cert_path` is provided to the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] method, the SDK automatically switches to an encrypted ([two-way](../client.md#1-encryption-tls)) gRPC channel.

```python
from mosaicolabs import MosaicoClient

# 1. Configuration constants
MOSAICO_HOST = "mosaico.production.yourdomain.com"
MOSAICO_PORT = 6726
# The certificate used by the server (CA or self-signed)
CERT_PATH = "/etc/mosaico/certs/server_ca.pem"
# Your secret API key
MY_API_KEY = "msco_vy9lqa7u4lr7w3vimhz5t8bvvc0xbmk2_9c94a86"

# 2. Establish the secure connection
with MosaicoClient.connect(
    host=MOSAICO_HOST,
    port=MOSAICO_PORT,
    api_key=MY_API_KEY,      # Injects Auth Middleware
    tls_cert_path=CERT_PATH   # Enables TLS encryption
) as client:
    # All operations inside this block are now encrypted and authenticated
    print(f"Connected to version: {client.version()}")
    sequences = client.list_sequences()
```
