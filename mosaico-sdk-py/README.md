<p align="center">
  <img src="https://raw.githubusercontent.com/mosaico-labs/mosaico/main/logo/mono_black.svg" width="300" alt="Mosaico Logo">
</p>

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/mosaico-labs/mosaico)](https://github.com/mosaico-labs/mosaico/releases)

# Mosaico SDK

The **Mosaico SDK** is the primary interface for interacting with the **Mosaico Data Platform**, a high-performance system designed for the ingestion, storage, and retrieval of multi-modal sensor data (Robotics, IoT, and Computer Vision).

It provides a seamless bridge between raw data formats (like ROS bags) and the Mosaico high-performance storage layer, leveraging technologies like **PyArrow**, **Pandas**, and **Pydantic**.

For full documentation, see the [Mosaico SDK Documentation](https://docs.mosaico.dev/latest/SDK/).

## Key Features

  * **High-Performance Ingestion**: Native support for ROS 1 and ROS 2 data via `rosbags`.
  * **Unified Client**: A simple, context-managed `MosaicoClient` for data retrieval and catalog querying.
  * **Multi-Modal Support**: Optimized for IMU, Camera, LiDAR, and custom telemetry data.
  * **Built-in CLI Tools**: Integrated utilities for data injection and interactive examples.
  * **Rich Visualization**: Beautiful terminal output powered by `rich`.


## Installation

Install the SDK via `pip`:

```bash
pip install mosaicolabs
```

*Note: Requires Python 3.10 or higher.*


## CLI Utilities

The SDK comes with built-in command-line tools to speed up your workflow.

### Infrastructure Prerequisite

Before running any mosaico service via SDK, ensure your **Mosaico Infrastructure** is active and running. 
The easiest way to start is using the provided **Quick Start environment**.
Please, refer to the **[daemon Setup](https://docs.mosaico.dev/latest/daemon/install/)** for setting up the environment.

```python
from mosaicolabs import MosaicoClient

# Connect to the Mosaico server
with MosaicoClient.connect(host="localhost", port=6726) as client:
    # Simple is-alive check
    print(client.version())
```


### Interactive Examples

We provide pre-built examples to help you explore the SDK capabilities. You can run them using the `mosaicolabs.examples` command:

```bash
# List all available examples and help
mosaicolabs.examples --help

# Run a specific example
mosaicolabs.examples data_inspection
```

**Available Examples:**

  * `ros_injection`: Demonstrates downloading a sample dataset and ingesting it.
  * `data_inspection`: Shows how to list sequences and inspect topic metadata.
  * `query_catalogs`: Advanced querying based on ontology tags and sensor values.

### ROS Data Injector

Inject ROS bags (MCAP or legacy) directly into the platform:

```bash
mosaicolabs.ros_injector --file path/to/your/data.mcap --sequence my_test_run
```

## Quick Start (Python)

```python
from mosaicolabs import MosaicoClient

# Connect to the Mosaico server
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

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. See the [LICENSE](https://github.com/mosaico-labs/mosaico?tab=AGPL-3.0-1-ov-file#readme) file for more details.


## Contributing

We welcome contributions! Please refer to our [Development Guide](https://github.com/mosaico-labs/mosaico?tab=contributing-ov-file) for instructions on how to set up your environment using **Poetry**.
