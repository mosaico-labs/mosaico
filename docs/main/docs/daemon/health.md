---
title: Health Checks
sidebar_position: 4
description: "How mosaicod reports liveness and readiness through the gRPC health checking protocol. Covers the two health service names, what each one depends on, and authentication."
---

`mosaicod` serves the [gRPC health checking protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) on the same port as the Arrow Flight service.

## Service Names

| Service | Purpose | `SERVING` | `NOT_SERVING` |
| --- | --- | --- | --- |
| `""` (empty) | Liveness | The server is up. | The server is shutting down. |
| `mosaicod` | Readiness | The last heartbeat reached PostgreSQL. | The last heartbeat failed to reach PostgreSQL, or the server is shutting down. |

**Liveness** never depends on PostgreSQL or the object store, so that an outage of either doesn't restart every replica.

**Readiness** follows the heartbeat that `mosaicod` sends to PostgreSQL every 30 seconds, so it can lag an outage by up to about a minute. It doesn't check the object store.

## Authentication

Health checks never require an API key, even with [API key management](api_key.md) enabled, since probes can't send credentials.
