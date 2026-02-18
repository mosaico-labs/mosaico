---
title: Ingestion Channel
description: High-throughput writing via data streams.
sidebar:
    order: 3
---

Data ingestion in Mosaico is handled by the **Ingestion Channel**, a specialized pathway implemented via the Flight `DoPut` streaming endpoint. 
This channel is explicitly engineered to handle write-heavy workloads, enabling the system to absorb high-bandwidth sensor data—such as 4K video streams or high-frequency Lidar point clouds—without contending with administrative traffic.

## The Ingestion Protocol

Uploading a stream involves a rigorous handshake protocol designed to ensure type safety before any data is committed to storage:

**Command Descriptor.** The client initiates the stream by sending a Flight Descriptor containing a JSON command identifying the target topic (e.g., `run_01/sensors/cam_front`) and providing the authorization key generated during topic creation.

**Schema Negotiation.** The first message of the stream must be the Apache Arrow Schema definition. This establishes the contract for the transmitted data and carries three critical pieces of metadata: *user metadata*, *ontology tag*, and *serialization format*.

User metadata consists of custom, queryable key-value pairs (e.g., `{"vehicle_id": "v1"}`) that tag streams with project-specific attributes. The ontology tag serves as a strict type identifier, ensuring that the arriving data conforms to the registered model (e.g., verifying `Lidar` data matches the `Lidar` ontology). Finally, the serialization format dictates the physical binary layout, supporting `Default` for fixed-schema tables, `Ragged` for variable-length lists, and `Image` for optimized multi-dimensional arrays.

If the schema or metadata mismatches the topic registration, the server immediately rejects the stream to guarantee data integrity.

**Data Transmission.** Once the schema is negotiated and accepted, the client streams a sequence of `RecordBatch` payloads. The server buffers the batches in memory and writes them efficiently to the underlying storage.

**Commit Phase.** Closing the stream signals the server that the upload is complete. The server then flushes all buffers, calculates necessary indices, and atomically commits the data chunks to the repository.

## Chunking & Indexing Strategy

To manage massive datasets efficiently, the backend automatically handles *chunking*. As data flows in, `mosaicod` splits the continuous stream into optimal storage units called *chunks*.

For every chunk written, the server computes and stores *skip indices* in the metadata database containing ontology statistics i.e. type-specific metadata (e.g., coordinate bounding boxes for GPS, value ranges for sensors) that enables the query engine to perform content-based filtering without reading the bulk data.