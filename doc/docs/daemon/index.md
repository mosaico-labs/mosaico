---
title: Backend Overview
description: Architecture and design of the mosaicod daemon.
sidebar: 
    order: 1
---

The **Mosaico Daemon**, a.k.a. `mosaicod`, acts as the authoritative kernel of the data platform. Developed in **Rust**, it is engineered to be the high-performance arbiter for all data interactions, guaranteeing that every byte of robotics data is strictly typed, atomically stored, and efficiently retrievable.

It functions on a standard client-server model, mediating between your high-level applications (via the SDKs) and the low-level storage infrastructure.

## Architectural Design

`mosaicod` is architected atop the **Apache Arrow Flight** protocol. Apache Arrow Flight is a general-purpose, high-performance client-server framework developed for the exchange of massive datasets. It operates directly on Apache Arrow columnar data, enabling efficient transport over gRPC without the overhead of serialization.

Unlike traditional REST APIs which serialize data into text-based JSON, Flight is designed specifically for high-throughput data systems. This architectural choice provides Mosaico with three critical advantages:

**Zero-Copy Serialization.** Data is transmitted in the Apache Arrow columnar format, the exact same format used in-memory by modern analytics tools like pandas and Polars. This eliminates the CPU-heavy cost of serializing and deserializing data at every hop.

**Parallelized Transport.** Operations are not bound to a single pipe; data transfer can be striped across multiple TCP connections to saturate available bandwidth.

**Snapshot-Based Schema Enforcement.** Data types are not guessed, nor are they forced into a rigid global model. Instead, the protocol enforces a rigorous schema handshake that validates data against a specific schema snapshot stored with the sequence.

### Resource Addressing

Mosaico treats every entity in the system, whether it's a Sequence or a Topic, as a uniquely addressable resource. These resources are identified by a **Resource Locator**, a uniform logical path that remains consistent across all channels. 

Mosaico uses two types of resource locators:

- A **Sequence Locator** identifies a recording session by its sequence name (e.g., `run_2023_01`).
- A **Topic Locator** identifies a specific data stream using a hierarchical path that includes the sequence name and topic path (e.g., `run_2023_01/sensors/lidar_front`).

### The Three Channels

The server partitions its operations into three specialized channels to ensure that administrative overhead never bottlenecks data throughput. The **Control Channel** acts as the system's administrative control path, handling metadata management and resource orchestration. 

Data flows into the system via the **Ingestion Channel**, a dedicated high-speed lane optimized for writing raw sensor data at wire speed. Conversely, the **Retrieval Channel** is engineered to read and stream requested data slices back to clients. This separation ensures that columnar data retrieval remains efficient and low-latency, even while the system is under heavy write load.

### Snapshot Schema

Robotics data is inherently dynamic, with sensor configurations and message definitions that evolve rapidly across different experimental runs. 

To avoid the overhead of complex schema migrations or the risk of breaking legacy datasets, Mosaico moves away from a rigid global schema in favor of a *schema snapshot* approach. 
By associating a specific snapshot with each individual sequence, `mosaicod` captures the exact blueprint of every topic and sensor message at the moment of creation. 
This ensures that the data remains immutable and self-describing. 
The SDK can reconstruct objects exactly as they were originally defined, guaranteeing that a recording remains perfectly readable even if the project's ontology undergoes significant changes over time.

### Two-Tier Storage Topology

`mosaicod` implements a hierarchical storage architecture, consisting of two distinct layers:

**L1 Hot State (Metadata Cache).** A high-performance **PostgreSQL** database acts as the system's L1 layer. It indexes all structural information—the catalog of sequences, topics, channel definitions, and validation schemas. This layer provides the transactional consistency and query speed required for the Control Channel's interactive operations.

**L2 Cold State (Long Term Storage).** The **Object Store** (e.g., S3, Google Cloud Storage, or MinIO) serves as the persistent L2 layer. This is the system's ground truth, where the bulk sensor data (images, Lidar point clouds) and immutable schema snapshots reside.

This topology decouples compute from retention storage, but more importantly, it enforces a critical resiliency invariant involved with the reconstructability principle:

*The L1 state is entirely transient and can be completely reconstructed effectively caching the L2 state.*

If the metadata database is corrupted or destroyed, `mosaicod` can rebuild the entire catalog by rescanning the durable L2 storage. This design ensures that while the L1 provides performance, the L2 guarantees long-term durability and recovery, protecting your data against catastrophic infrastructure failure.