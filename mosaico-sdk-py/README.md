<p align="center">
  <img src="https://raw.githubusercontent.com/mosaico-labs/mosaico/main/logo/mono_black.svg" width="300" alt="Mosaico Logo">
</p>

<p align="center">
  <a href="https://pypi.org/project/mosaicolabs/"><img src="https://img.shields.io/pypi/v/mosaicolabs.svg" alt="PyPI version"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10%2B-blue" alt="Python 3.10+"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="Apache 2.0 License"></a>
</p>

# Mosaico SDK

The **Mosaico SDK** is the primary Python interface for the **Mosaico Data Platform**, a high-performance system for ingesting, storing, and querying multi-modal robotics data (Physical AI, Robotics, IoT, and Computer Vision).

It treats robotics data as a first-class citizen instead of a bag of generic numbers: sensor streams are validated, typed, time-synchronized, and queryable — down to the physical value level (e.g. *"find every sequence where lateral acceleration exceeded 4 m/s²"*).

For full documentation, see the [Mosaico Python SDK Documentation](https://docs.mosaico.dev/python-sdk/).

## Key Features

* **Typed Ontology** — [Pydantic](https://docs.pydantic.dev/) models for common sensors (IMU, GPS, Camera, LiDAR, Point Clouds, ...) with an automatically derived [Apache Arrow](https://arrow.apache.org/) schema, and a fluent `.Q` proxy for type-safe queries directly on model fields (`IMU.Q.acceleration.x.gt(9.8)`).
* **High-Performance I/O** — zero-copy Arrow transport, batched streaming, and optional gRPC compression, so datasets far larger than RAM can be pushed and pulled efficiently.
* **Deep Querying** — search across sequence/topic metadata *and* the physical content of sensor streams in a single request, with built-in temporal clustering and cross-sensor correlation.
* **ROS Bridge** — ingest ROS 1 `.bag` and ROS 2 `.mcap`/`.db3` files out of the box, with adapters for common message types and a clean extension path for custom/proprietary ones.
* **ML-Ready** — flatten nested sensor data into [`pandas`](https://pandas.pydata.org/) DataFrames and resample multi-rate sensors onto a uniform time grid for model training.
* **Secure by Default** — one-way/two-way TLS and API-key authentication are first-class options on every connection.

## Installation

Install the SDK via `pip`:

```bash
pip install mosaicolabs
```

*Requires Python 3.10 or higher.*

### Infrastructure Prerequisite

Before running any Mosaico service via the SDK, ensure your **Mosaico Infrastructure** is active and running. The easiest way is to follow the [installation guide](https://docs.mosaico.dev/daemon/install/).

```python
from mosaicolabs import MosaicoClient

# Connect to the Mosaico server
with MosaicoClient.connect(host="localhost", port=6726) as client:
    # Simple is-alive check
    print(client.version())
```

## Quick Start

### Reading Data

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect(host="localhost", port=6726) as client:
    # List available sequences
    sequences = client.list_sequences()
    print(f"Connected! Found sequences: {sequences}")

    # Get a handle for a specific sequence
    if sequences:
        handler = client.sequence_handler(sequences[0])
        print(f"- Topics: {handler.topics}")
        print(f"- Created: {handler.created_timestamp}")
        print(f"- Updated: {handler.updated_timestamps}")
```

### Ingesting Data

```python
from mosaicolabs import MosaicoClient, Message, IMU, Vector3d

with MosaicoClient.connect(host="localhost", port=6726) as client:
    # A `with` block ensures buffers are flushed and the sequence is
    # committed on exit, even if the code inside raises.
    with client.sequence_create(sequence_name="demo_run") as seq_writer:
        imu_writer = seq_writer.topic_create(
            topic_name="sensors/imu",
            ontology_type=IMU,
        )

        imu_writer.push(
            message=Message(
                timestamp_ns=1_700_000_000_000_000_000,
                data=IMU(
                    acceleration=Vector3d(x=0.1, y=0.0, z=9.81),
                    angular_velocity=Vector3d(x=0.0, y=0.0, z=0.0),
                ),
            )
        )
```

### Querying Data

```python
from mosaicolabs import MosaicoClient, QueryOntologyCatalog, IMU

with MosaicoClient.connect(host="localhost", port=6726) as client:
    # Find every sequence where the IMU registered a hard vertical impact
    qresponse = client.query(
        QueryOntologyCatalog().with_expression(IMU.Q.acceleration.z.gt(15.0))
    )

    if qresponse is not None:
        for item in qresponse:
            print(f"Sequence: {item.sequence.name}")
            print(f"Topics: {[topic.name for topic in item.topics]}")
```

### ROS Data Injector

Inject ROS bags (MCAP or legacy) directly into the platform:

```bash
mosaicolabs.ros_injector --file path/to/your/data.mcap --sequence my_test_run
```

### Interactive Examples

We provide pre-built examples to help you explore the SDK capabilities. You can run them using the `mosaicolabs.examples` command:

```bash
# List all available examples and help
mosaicolabs.examples --help

# Run a specific example
mosaicolabs.examples ros_injection
```

**Available Examples:**

  * `ros_injection`: Demonstrates downloading a sample dataset and ingesting it. **Run this first** — the other examples query the data it ingests.
  * `reconstruct_rosbags`: Reconstructs the ingested rosbag.
  * `data_inspection`: Shows how to list sequences and inspect topic metadata.
  * `query_catalogs`: Advanced querying based on ontology tags and sensor values.
  * `mujoco_vis`: Advanced querying and result visualization.

## Documentation

| Topic | Description |
| :--- | :--- |
| [Client](https://docs.mosaico.dev/python-sdk/SDK/client/) | Connecting to the platform: TLS, API-key auth, compression. |
| [Ontology](https://docs.mosaico.dev/python-sdk/SDK/ontology/) | Typed data models, custom ontologies, and the `.Q` query proxy. |
| [Data Handling](https://docs.mosaico.dev/python-sdk/SDK/handling/writing/) | Writing and reading sequences and topics. |
| [Query](https://docs.mosaico.dev/python-sdk/SDK/query/) | The full query DSL, temporal windows, and query chaining. |
| [ROS Bridge](https://docs.mosaico.dev/python-sdk/SDK/bridges/ros/) | Ingesting ROS 1/ROS 2 bags and writing custom adapters. |

## Changelog

See [CHANGELOG.md](https://github.com/mosaico-labs/mosaico/blob/main/mosaico-sdk-py/CHANGELOG.md) for release notes.

## Contributing

We welcome contributions! Please refer to our [Development Guide](https://github.com/mosaico-labs/mosaico?tab=contributing-ov-file) for instructions on how to set up your environment using **Poetry**.

## License

This project is licensed under the Apache-2.0 license.
