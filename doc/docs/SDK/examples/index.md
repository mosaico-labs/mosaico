---
title: Mosaico SDK Examples
description: Runnable examples of the mosaico python SDK.
---

The Mosaico SDK is designed with a **code-first philosophy**. To help you move from architectural concepts to production implementation, we provide a suite of pedagogical examples.

These examples aren't just snippets; they are **fully executable blueprints** that demonstrate high-performance data handling, zero-copy serialization with Apache Arrow, and deep discovery via our type-safe query engine.

We provide a specialized CLI utility to run these examples without manual configuration. This runner automatically injects connection parameters into the example logic, allowing you to point the samples at your specific Mosaico instance.

### Basic Usage

From your terminal, use the `mosaicolabs.examples` command followed by the name of the example:

```bash
# Run the ROS Ingestion example
mosaicolabs.examples ros_injection
```

### Configuration Options

The CLI supports several global flags to control the execution environment:

| Option | Default | Description |
| :--- | :--- | :--- |
| `--host` | `localhost` | The hostname of your Mosaico Server. |
| `--port` | `6726` | The Flight port of your Mosaico Server. |
| `--log-level` | `INFO` | Set verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

**Example with custom server:**

```bash
mosaicolabs.examples data_inspection --host 192.168.1.50 --port 6276 --log-level DEBUG
```

### Available Blueprints

We recommend exploring the examples in the following order to understand the platform's flow:

#### [ROS Ingestion](./ros_injection.md)

  * **CLI Name**: `ros_injection`
  * **Purpose**: The "Hello World" of Mosaico. This example automates the download of the [NVIDIA R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1) and performs a high-performance injection from `.mcap` files.
  * **Key Concept**: Demonstrates **Adaptation Logic**—translating raw ROS dictionaries into strongly-typed Mosaico Ontology models.

#### [Data Discovery and Inspection](./data_inspection.md)

  * **CLI Name**: `data_inspection`
  * **Purpose**: Learn how to browse the server-side catalog.
  * **Key Concept**: Explains the **Lazy Handler** pattern. You will see how to retrieve sequence metadata (size, timestamps) and topic details without downloading bulk sensor data.

#### [Deep Querying](./query_catalogs.md)

  * **CLI Name**: `query_catalogs`
  * **Purpose**: Move beyond simple time-based retrieval.
  * **Key Concept**: Showcases the **`.Q` Query Proxy**. You will learn to search for specific physical events (e.g., "Find IMU lateral acceleration >= 1.0 m/s^2") across your entire dataset.


### Infrastructure Prerequisite

Before running any example, ensure your Mosaico infrastructure is active. The easiest way to start is using the provided **Quick Start environment**.
Please, refer to the **[daemon Setup](../../daemon/install.md)** for setting up the environment.

### Ready to start?
We recommend beginning with the **[ROS Ingestion Guide](./ros_injection.md)** to populate your local server with high-fidelity robotics data. The other examples will run on the data ingested via the ROS Ingestion example.