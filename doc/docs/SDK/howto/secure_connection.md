---
title: Secure Connection
description: Example how-to use TLS & API Key Authentication
---

By following this guide, you will learn how to:

1. **Initialize an Authenticated Session** using the `connect` method.
2. **Handle TLS Certificates** for encrypted communication.
3. **Use Environment Discovery** for production-grade credential management.

## Option 1: Connecting via the `connect()` Factory

The [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] method is the standard way to provide security credentials. Note that when `tls_cert_path` is provided, the SDK automatically switches to an encrypted gRPC channel.

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

## Option 2: Environment Discovery

In actual robotics fleets or CI/CD pipelines, hardcoding keys is a major security risk. The [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] includes a [`from_env()`][mosaicolabs.comm.MosaicoClient.from_env] method that automatically looks for specific environment variables to configure the client.

### 1. Set your environment variables
In your terminal or Docker Compose file, set the following:
```bash
export MOSAICO_API_KEY="msco_your_secret_key_here"
export MOSAICO_TLS_CERT_FILE="/path/to/ca.pem"
```

### 2. Implement the "Zero-Config" Connection
The SDK will now automatically pick up these values:

```python
import os
from mosaicolabs import MosaicoClient

# The host and port are still required, but credentials are discovered
with MosaicoClient.from_env(
        host="mosaico.internal.cluster",
        port=6726
    ) as client:
    # The client is already authenticated via MOSAICO_API_KEY
    # and encrypted via MOSAICO_TLS_CERT_FILE
    pass
```
