---
title: Getting Started
sidebar_position: 2
description: "Quickstart for Mosaico. Covers installing mosaicod, installing a client SDK, and pushing your first typed sensor data in under five minutes."
---

Interacting with Mosaico requires two components: `mosaicod`, which runs server-side and manages all data operations, and a client SDK to communicate with it from your code.

## Mosaico Daemon

`mosaicod` is the engine behind the platform. It handles storage, catalog management, ingestion, and retrieval. No client call will succeed without a running instance.

### Quick install

The fastest way to get a local instance running is the install script served at `get.mosaico.dev`. It only supports Linux and macOS; on Windows, install WSL2 with an Ubuntu distribution and run it inside that instead.

```bash
curl -fsSL https://get.mosaico.dev | sh
```

Once it finishes, you'll have a fully configured environment ready to go:

- the `mosaicod` and `mosaicoctl` commands available
- a manual page with additional information, viewable with `man mosaico`
- optionally, the [Python SDK](#python-sdk) itself

:::info
Run it in dry-run mode first to preview every step without changing anything on disk:

```bash
curl -fsSL https://get.mosaico.dev | sh -s -- --dry-run
```

See every available option with:

```bash
curl -fsSL https://get.mosaico.dev | sh -s -- --help
```

:::

### Manual install

If you'd rather manage the containers yourself, use the following compose file for a quick local setup:

```yaml title="compose.yaml"
services:
  db:
    image: postgres:18
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust

  mosaicod:
    image: ghcr.io/mosaico-labs/mosaicod:latest
    environment:
      MOSAICOD_DB_URL: postgresql://postgres@db:5432/postgres
      MOSAICOD_STORE_ENDPOINT: file:///tmp
      MOSAICOD_STORE_BUCKET: mosaico
    command: run --host 0.0.0.0
    depends_on:
      - db
    ports:
      - "6726:6726"
```

This setup will create a data folder at `/tmp/mosaico` on your machine. You can change this by modifying the `MOSAICOD_STORE_ENDPOINT` variable.

:::note
For more advanced installation options, see the [installation guide](./daemon/install.md).
:::

## Python SDK

The [`mosaicolabs`](https://pypi.org/project/mosaicolabs/) Python SDK is the primary way to interact with the platform. It provides a high-level API for the full data lifecycle, ingesting sensor data, querying catalogs, and streaming data into ML pipelines, without any custom serialization code.

With `mosaicod` running, the SDK is all you need to start working with your data. Install it, point it at your daemon, and you have full programmatic access to the platform: write sequences, query catalogs, and pull data directly into your pipelines. No extra configuration or intermediary services required.

Install it via `pip`:

```bash
pip install mosaicolabs
```

The following example connects to a local daemon instance and lists available sequences:

```py title="lets_get_physical.py"
from mosaicolabs import MosaicoClient

# Connect to the Mosaico server
with MosaicoClient.connect(host="localhost", port=6726) as client:
    # List available sequences
    sequences = client.list_sequences()
    print(f"Connected! Found sequences: {sequences}")
```

Jump straight into the how-to guides on [writing data](learn/writing_single_topic.mdx) and [querying sequences](learn/query_sequences.mdx), or see the [Python SDK documentation](clients/python.md) for the full reference.
