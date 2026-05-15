---
title: Overview
sidebar_position: 1
description: "Architecture overview of mosaicod, the Mosaico daemon written in Rust. Covers its role as the central data arbiter, the Apache Arrow Flight protocol, the Ontology type system, and how it handles typed storage, ingestion, querying, and retrieval."
---

The **Mosaico Daemon**, a.k.a. `mosaicod`, acts as engine of the data platform. Engineered to be the high-performance arbiter for all data interactions, guaranteeing that every byte of robotics data is strictly typed, atomically stored, and efficiently retrievable.

It functions on a standard client-server model, mediating between your high-level applications and the low-level storage infrastructure.

## Why `mosaicod`?

Robotics produces a data profile that standard infrastructure is rarely equipped to handle. A single system generates dozens of concurrent, asynchronous streams—ranging from high-bandwidth point clouds to sparse IMU readings—each operating at independent frequencies. 
When this data is treated as a generic stream of bytes, the result is usually significant storage bloat and a high barrier to retrieval.

The necessity of mosaicod stems from the requirement to move data management away from the application layer and into a dedicated, persistent daemon. 
By sitting between the producers and the disk, the daemon can apply compression based on the specific data model rather than a generic algorithm, and it can facilitate partial retrieval so that a two-second event can be extracted from a ten-hour log without scanning unrelated data.

Furthermore, robotics development relies on long-term data viability and lineage. 
As software evolves, schemas change; `mosaicod` anchors type enforcement to the specific schema snapshot present at the time of recording, preventing historical data from becoming unreadable. 
It also manages the inherent contention of multiple uncoordinated processes writing to the same sink simultaneously. By centralizing these responsibilities, the system ensures that data integrity, lineage, and schema enforcement are built-in properties of the recording process rather than manual tasks for the developer.

## Architectural Design

`mosaicod` is architected atop the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol, a general-purpose, high-performance client-server framework developed for the exchange of massive datasets. It operates directly on [Apache Arrow](https://arrow.apache.org/) columnar data, enabling efficient transport over [gRPC](https://grpc.io/) without the overhead of serialization.

Unlike traditional REST APIs which serialize data into text-based JSON, Flight is designed specifically for high-throughput data systems. This architectural choice provides Mosaico with three critical advantages:

**Zero-Copy Serialization.** Data is transmitted in the Arrow columnar format, the exact same format used in-memory by modern analytics tools like pandas and Polars. This eliminates the CPU-heavy cost of serializing and deserializing data at every hop.

**Parallelized Transport.** Operations are not bound to a single pipe; data transfer can be striped across multiple connections to saturate available bandwidth.

**Snapshot-Based Schema Enforcement.** Data types are not guessed, nor are they forced into a rigid global model. Instead, the protocol enforces a rigorous schema handshake that validates data against a specific schema snapshot stored with the sequence.

### Resource Addressing

Mosaico treats every entity in the system, whether it's a Sequence or a Topic, as a uniquely addressable resource. These resources are identified by a **Resource Locator**, a uniform logical path that remains consistent across all channels. 

Mosaico uses two types of resource locators:

- A **Sequence Locator** identifies a recording session by its sequence name (e.g., `run_2023_01`).
- A **Topic Locator** identifies a specific data stream using a hierarchical path that includes the sequence name and topic path (e.g., `run_2023_01/sensors/lidar_front`).

### Storage Architecture

`mosaicod` uses an **RDBMS** to perform fast queries on metadata, manage system state such as sequence and topic definitions, and handle the event queue for processing asynchronous tasks like background data processing or notifications. An **object store** (such as S3, MinIO, or local filesystem) provides long-term storage for resilience and durability, holding the bulk sensor data, images, point clouds, and immutable schema snapshots that define data structures.

:::info 
The RDBMS system is not strictly required for data durability.

If the metadata database is corrupted or destroyed, `mosaicod` can rebuild the entire catalog by rescanning the durable object storage. 
This design ensures that while the DBMS is used to create relations between datasets, the store guarantees long-term durability and recovery, protecting your data against catastrophic infrastructure failure.
:::

## The Mosaico Protocol

Arrow Flight provides the transport layer, but it is intentionally generic — it knows nothing about sequences, topics, schemas, or the semantics of a robotics recording. `mosaicod` builds its own protocol on top of it: a structured set of commands and message formats that map the Flight primitives onto Mosaico's data model.

This protocol defines how a client opens a recording session, registers a topic with its schema, streams typed data into a sequence, and later queries or retrieves that data. It handles the schema handshake that ties a write to a specific snapshot, the resource locators that address sequences and topics uniformly, and the metadata exchanges that keep the catalog consistent.

The protocol is designed around the constraints of robotics workloads: high-frequency writes from multiple concurrent producers, reads that may span long time ranges, and the need to mix small structured messages (like joint states) with large binary payloads (like compressed images or point clouds) within the same session.

### SDKs

Because the Mosaico protocol is built on top of Flight and gRPC, interacting with `mosaicod` directly would require assembling low-level RPC calls and manually constructing the message formats the daemon expects. The Mosaico SDKs remove this entirely.

The SDKs speak the protocol natively — they handle connection management, schema registration, the Arrow serialization, and the correct sequencing of operations. From the client's perspective, writing a topic or querying a sequence is a straightforward API call; the protocol details are fully abstracted away.

Currently, Mosaico provides an SDK for **Python**, with additional language support planned. The Python SDK is the primary interface for data ingestion, retrieval, and integration with the broader robotics and data science ecosystem.
