---
title: Backend Overview
description: Architecture and design of the mosaicod daemon.
sidebar: 
    order: 1
---

The **Mosaico Daemon**, a.k.a. `mosaicod`, acts as engine of the data platform. Developed in **Rust**, it is engineered to be the high-performance arbiter for all data interactions, guaranteeing that every byte of robotics data is strictly typed, atomically stored, and efficiently retrievable.

It functions on a standard client-server model, mediating between your high-level applications (via the SDKs) and the low-level storage infrastructure.

## Architectural Design

`mosaicod` is architected atop the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol. Apache Arrow Flight is a general-purpose, high-performance client-server framework developed for the exchange of massive datasets. It operates directly on [Apache Arrow](https://arrow.apache.org/) columnar data, enabling efficient transport over [gRPC](https://grpc.io/) without the overhead of serialization.

Unlike traditional REST APIs which serialize data into text-based JSON, Flight is designed specifically for high-throughput data systems. This architectural choice provides Mosaico with three critical advantages:

**Zero-Copy Serialization.** Data is transmitted in the Arrow columnar format, the exact same format used in-memory by modern analytics tools like pandas and Polars. This eliminates the CPU-heavy cost of serializing and deserializing data at every hop.

**Parallelized Transport.** Operations are not bound to a single pipe; data transfer can be striped across multiple connections to saturate available bandwidth.

**Snapshot-Based Schema Enforcement.** Data types are not guessed, nor are they forced into a rigid global model. Instead, the protocol enforces a rigorous schema handshake that validates data against a specific schema snapshot stored with the sequence.

### Resource Addressing

Mosaico treats every entity in the system, whether it's a Sequence or a Topic, as a uniquely addressable resource. These resources are identified by a **Resource Locator**, a uniform logical path that remains consistent across all channels. 

Mosaico uses two types of resource locators:

- A **Sequence Locator** identifies a recording session by its sequence name (e.g., `run_2023_01`).
- A **Topic Locator** identifies a specific data stream using a hierarchical path that includes the sequence name and topic path (e.g., `run_2023_01/sensors/lidar_front`).

### Flight Endpoints

The daemon exposes Apache Arrow Flight endpoints that handle various operations using Flight's core methods: `list_flights` and `get_flight_info` for discovery and metadata management, `do_put` for high-speed data ingestion, and `do_get` for efficient data retrieval. 
This design ensures administrative operations don't interfere with data throughput while maintaining low-latency columnar data access.

### Storage Architecture

`mosaicod` uses a database to perform fast queries on metadata, manage system state such as sequence and topic definitions, and handle the event queue for processing asynchronous tasks like background data processing or notifications. An object store (such as S3, MinIO, or local filesystem) provides long-term storage for resilience and durability, holding the bulk sensor data, images, point clouds, and immutable schema snapshots that define data structures.

!!! note "Database Durability and Recovery"
    The database state is entirely transient and can be fully reconstructed from the object store. This also enables importing data from other stores. 
    
    *Currently, there is no way to import data and reconstruct the database, but we are designing the system to enable this feature in future releases.* 

If the metadata database is corrupted or destroyed, `mosaicod` can rebuild the entire catalog by rescanning the durable object storage. This design ensures that while the database provides performance, the store guarantees long-term durability and recovery, protecting your data against catastrophic infrastructure failure.
