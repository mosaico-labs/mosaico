---
title: Retrieval Channel
description: Streaming data via the Ticket mechanism.
sidebar:
    order: 4
---

Measurement data is accessed through the **Retrieval Channel**, which leverages the Flight `DoGet` endpoint to serve high-performance read operations. 
Unlike a simple file download, this channel provides a sophisticated interface that allows clients to request precise slices of data, which are then dynamically assembled and streamed back as a sequence of optimized Arrow batches.

## The Retrieval Protocol

Accessing data requires specifying the **Locator**, which defines the topic path, and an optional time range in nanoseconds.

The resolution process follows a coordinated sequence of operations designed to minimize latency. Upon receiving a request, the server performs an index lookup within the metadata cache to identify the physical data chunks that intersect with the requested time window. This is followed by a pruning stage, where the system discards any chunks that fall entirely outside the query bounds to avoid redundant I/O. Once the relevant segments are identified, the server initiates streaming, opening the underlying files and delivering the data back to the client in a high-throughput pipeline.

## Smart Batching

The server performs more than just a file dump; it implements smart batching to optimize network performance. This is particularly useful when streaming heterogeneous data, where payloads can range from simple, lightweight time-series to high-resolution 4K images.

Through adaptive sizing, the system analyzes the schema structure and the compression ratio of the stored data to dynamically compute an optimal `RecordBatch` size. This approach maximizes memory and network efficiency, ensuring that network packets are fully utilized while preventing Out-Of-Memory (OOM) errors on the client side that can occur when deserializing massive, monolithic batches.


## Metadata Context Headers

To ensure the client has full context, the data stream is prefixed with a Schema message containing embedded custom metadata. Mosaico injects rich context into this header, allowing the client to reconstruct the full environment.

This includes *user metadata*, preserving original project context like experimental tags or vehicle IDs, and the *ontology tag*, which informs the client exactly what type of sensor data (e.g., `Lidar`, `Camera`) is being received to enable type-safe deserialization.

The *serialization format* instructs the client on how to interpret the Arrow buffers on the wire. The supported formats include:

- `Default`: The standard Arrow columnar layout.
- `Ragged`: Optimized representation for variable-length lists.
- `Image`: An optimized array format specifically for high-resolution visual data.