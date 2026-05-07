---
title: Getting Started
sidebar_position: 2
---

Interacting with Mosaico requires two components: `mosaicod`, which runs server-side and manages all data operations, and a **client SDK** to communicate with it from your code.

## Mosaico Daemon

`mosaicod` is the engine behind the platform. It handles storage, catalog management, ingestion, and retrieval. No client call will succeed without a running instance, follow the [installation guide](./daemon/install.md) to get it up and running first.

## Python SDK

The [`mosaicolabs`](https://pypi.org/project/mosaicolabs/) Python SDK is the primary way to interact with the platform. It provides a high-level API for the full data lifecycle, ingesting sensor data, querying catalogs, and streaming data into ML pipelines, without any custom serialization code.

Install it via `pip`:

```bash
pip install mosaicolabs
```

See the [Python SDK documentation](https://docs.mosaico.dev/sdk) for the full reference, or jump straight into the how-to guides on [writing data](https://docs.mosaico.dev/sdk/SDK/howto/serialized_writing_from_csv/) and [querying sequences](https://docs.mosaico.dev/sdk/SDK/howto/query_sequences/).

## LLM-Friendly Docs

Mosaico provides machine-readable documentation in the [`llms.txt`](https://llmstxt.org/) format for use with AI assistants and LLM-powered tooling:

| Resource | Description |
| :--- | :--- |
| [`llms.txt`](https://docs.mosaico.dev/llms.txt) | Concise platform and architecture overview |
| [`llms-full.txt`](https://docs.mosaico.dev/llms-full.txt) | Complete platform and architecture docs |
| [`py/llms.txt`](https://docs.mosaico.dev/sdk/llms/llms.txt) | Concise Python SDK overview |
| [`py/llms-full.txt`](https://docs.mosaico.dev/sdk/llms/llms-full.txt) | Complete Python SDK docs |
