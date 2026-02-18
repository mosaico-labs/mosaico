---
title: Writing & Reading Data
description: Handlers, Writers, and Streamers.
---

The **Data Handling** module serves as the high-performance operational core of the Mosaico SDK, providing a unified interface for moving multi-modal sensor data between local applications and the Mosaico Data Platform. Engineered to solve the "Big Data" challenges of robotics and autonomous systems, this module abstracts the complexities of network I/O, asynchronous buffering, and high-precision temporal alignment.

### Asymmetric Architecture

The SDK employs a specialized architecture that separates concerns into **Writers** and **Handlers**, ensuring each layer is optimized for its unique traffic pattern:

* **Ingestion (Writing)**: Designed for low-latency, high-throughput ingestion of 4K video, high-frequency IMU telemetry, and dense point clouds. It utilizes a "Multi-Lane" approach where each sensor stream operates in isolation with dedicated system resources.
* **Discovery & Retrieval (Reading)**: Architected to separate metadata-based resource discovery from high-volume data transmission. This separation allows developers to inspect sequence and topic catalogs—querying metadata and temporal bounds—before committing to a high-bandwidth data stream.

### Memory-Efficient Data Flow

The Mosaico SDK is engineered to handle massive data volumes without exhausting local system resources, enabling the processing of datasets that span terabytes while maintaining a minimal and predictable memory footprint.

* **Smart Batching & Buffering**: Both reading and writing operations are executed in memory-limited batches rather than loading or sending entire sequences at once.
* **Asynchronous Processing**: The SDK offloads CPU-intensive tasks, such as image serialization and network I/O, to background threads within the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient].
* **Automated Lifecycle**: In reading workflows, processed batches are automatically discarded and replaced with new data from the server. In writing workflows, buffers are automatically flushed based on configurable size or record limits.
* **Stream Persistence**: Integrated **Error Policies** allow developers to prioritize either a "clean slate" data state or "recovery" of partial data in the event of an application crash.

