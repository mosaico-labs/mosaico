<p align="center">
  <img src="https://raw.githubusercontent.com/mosaico-labs/mosaico/main/logo/mono_black.svg" width="300" alt="Mosaico Logo">
</p>

<p align="center">
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" /></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10%2B-blue" /></a>
</p>

# Mosaico SDK

The **Mosaico SDK** is the primary interface for interacting with the **Mosaico Data Platform**, a high-performance system designed for the ingestion, storage, and retrieval of multi-modal sensor data (Robotics, IoT, and Computer Vision).

For full documentation, see the [Mosaico SDK Documentation](https://docs.mosaico.dev/sdk/).

## Installation

Install the SDK via `pip`:

```bash
pip install mosaicolabs
```

### Infrastructure Prerequisite

Before running any mosaico service via SDK, ensure your **Mosaico Infrastructure** is active and running. 
The easiest way to follow the [documentation](https://docs.mosaico.dev/daemon/install/).

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
  * `mujoco_vis`: Advanced querying and result visualization.

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

This project is licensed under the Apache-2.0 licence.


## Contributing

We welcome contributions! Please refer to our [Development Guide](https://github.com/mosaico-labs/mosaico?tab=contributing-ov-file) for instructions on how to set up your environment using **Poetry**.
