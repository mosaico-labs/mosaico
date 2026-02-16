---
title: Client Architecture
description: Description of the MosaicoClient class
sidebar:
    order: 2
---

The `MosaicoClient` is a resource manager designed to orchestrate three distinct **Layers** of communication and processing. 
This layered architecture ensures that high-throughput sensor data does not block critical control operations or application logic.

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

**Best Practice:** It is recomended to always use the client inside a `with` context to ensure resources in all layers are cleanly released.

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Logic goes here
    pass
# Pools and connections are closed automatically

```

## Quick reference
API Reference: [`mosaicolabs.comm.MosaicoClient`][mosaicolabs.comm.MosaicoClient].

| Method | Return | Description |
| :--- | :--- | :--- |
| **[`connect(host, port, timeout)`][mosaicolabs.comm.MosaicoClient.connect]** | `MosaicoClient` | Establishes the connection to the server and initializes all data and processing pools. |
| **[`close()`][mosaicolabs.comm.MosaicoClient.close]** | `None` | Manually shuts down all pools and connections. Called automatically by the context manager (if the instance was created in a `with` block). |
| **[`sequence_create(sequence_name, metadata, on_error)`][mosaicolabs.comm.MosaicoClient.sequence_create]** | [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] | Creates a [new writer](handling/writing.md) for uploading data. |
| **[`sequence_handler(sequence_name)`][mosaicolabs.comm.MosaicoClient.sequence_handler]** | [`Optional[SequenceHandler]`][mosaicolabs.handlers.SequenceHandler] | Retrieves a [handler](handling/reading.md) for an existing sequence. The method does not actually download the sequence data-stream. |
| **[`topic_handler(sequence_name, topic_name)`][mosaicolabs.comm.MosaicoClient.topic_handler]** | [`Optional[TopicHandler]`][mosaicolabs.handlers.TopicHandler] | Retrieves a [handler](handling/reading.md) for a specific topic within a sequence. The method does not actually download the topic data-stream. |
| **[`query(*queries, query)`][mosaicolabs.comm.MosaicoClient.query]** | [`Optional[QueryResponse]`][mosaicolabs.models.query.response.QueryResponse] | Executes [queries](query.md) against the platform catalogs. The provided queries are joined in AND condition. The method accepts a variable arguments of query builder objects or a pre-constructed *Query* object.|
| **[`sequence_delete(sequence_name)`][mosaicolabs.comm.MosaicoClient.sequence_delete]** | `None` | Permanently removes a sequence and all its associated data from the server. The operation is allowed only on [unlocked sequences](../index.md#data-lifetime-and-integrity) |
| **[`list_sequences()`][mosaicolabs.comm.MosaicoClient.list_sequences]** | `List[str]` | Retrieves the list of all sequences available on the server. |
| **[`list_sequence_notify(sequence_name)`][mosaicolabs.comm.MosaicoClient.list_sequence_notify]** | `List[Notified]` | Retrieves a list of all notifications available on the server for a specific sequence. |
| **[`list_topic_notify(sequence_name, topic_name)`][mosaicolabs.comm.MosaicoClient.list_topic_notify]** | `List[Notified]` | Retrieves a list of all notifications available on the server for a specific topic. |
| **[`clear_sequence_notify(sequence_name)`][mosaicolabs.comm.MosaicoClient.clear_sequence_notify]** | `None` | Clears the notifications for a specific sequence from the server. |
| **[`clear_topic_notify(sequence_name, topic_name)`][mosaicolabs.comm.MosaicoClient.clear_topic_notify]** | `None` | Clears the notifications for a specific topic from the server. |


