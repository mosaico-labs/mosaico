

<div align="center">
<picture>
<source media="(prefers-color-scheme: light)" srcset="../logo/mono_black.svg">
<source media="(prefers-color-scheme: dark)" srcset="../logo/mono_white.svg">
<img alt="Mosaico logo" src="../logo/color_white.svg" height="100">
</picture>
</div>

> [!WARNING]
> **Mosaico is currently in an early development phase.**
>
> This software is not ready for production environments. Until the release of version **1.0**, the API, inner mechanisms and naming are subject to significant changes without notice.

# Mosaico SDK

The **Mosaico SDK** is the primary interface for interacting with the **Mosaico Data Platform**, a high-performance system designed for the ingestion, storage and retrieval of multi-modal sensor data (Robotics, IoT).

Unlike generic time-series databases, Mosaico understands the semantics of complex sensor data types—from LIDAR point clouds and high-res images to telemetry and transformations. This SDK provides Python-native bindings to:

  * **Define Data**: Use a strongly-typed **Ontology** (e.g., `Image`, `IMU`, `Pose`) that maps automatically to efficient PyArrow serialization.
  * **Ingest Streams**: Push time-synchronized data sequences to the platform with automatic batching and parallelism.
  * **Query & Retrieve**: Execute queries on metadata queries and time series and retrieve the sequences and topics that match the criteria.
  * **Bridge Ecosystems**: Native adapters for ingesting data directly from **ROS 1** (`.bag`) and **ROS 2** (`.mcap`, `.db3`) files.

## Installation

The SDK is currently available via source distribution. We use [Poetry](https://python-poetry.org/) for robust dependency management and packaging.

### Prerequisites

  * **Python:** Version **3.13** or newer is required.
  * **Git:** To clone the repository.
  * **Poetry:** For package management.

### Install Poetry

If you do not have Poetry installed, use the official installer:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Ensure the poetry binary is in your path by verifying the version:

```bash
poetry --version
# Expected: Poetry (version 1.8.0 or higher)
```

### Clone and Install SDK

Clone the repository and navigate to the SDK directory:

```bash
git clone https://github.com/mosaico-labs/mosaico.git
cd mosaico/mosaico-sdk-py
```

Install the dependencies. This will automatically create a virtual environment and install all required libraries (PyArrow, NumPy, ROSBags, etc.):

```bash
poetry install
```

### Activate Environment

You can spawn a shell within the configured virtual environment to work interactively:

```bash
eval $(poetry env activate)
```

Alternatively, you can run one-off commands without activating the shell:

```bash
poetry run python any_script.py
```



## Quick Start

This guide assumes familiarity with the terminology and core concepts of the Mosaico framework. If you are new to the platform, we strongly recommend reviewing the [**Core Concepts**](../CORE_CONCEPTS.md) section first, for a deep dive into the project's philosophy and how data is structured, stored, and managed.

###  ROS Injection
The SDK comes with a quick-start built-in CLI tool to inject ROS bags directly into the platform.

```bash
# Run the injector
mosaico.ros_injector ./my_bag_file.mcap \
    --name "Sequence_001" \
    --metadata '{"location": "lab", "operator": "alice"}'
```

The full list of options by running `mosaico.ros_injector -h`:

```bash
usage: mosaico.ros_injector [-h] --name NAME [--host HOST] [--port PORT] [--topics TOPICS [TOPICS ...]] [--metadata METADATA]
                                [--ros-distro {empty,latest,ros1_noetic,ros2_dashing,ros2_eloquent,ros2_foxy,ros2_galactic,ros2_humble,ros2_iron,ros2_jazzy,ros2_kilted}]
                                bag_path

Inject ROS Bag data into Mosaico.

positional arguments:
  bag_path              Path to .bag, .mcap or .db3 file

options:
  -h, --help            show the help message and exit
  --name, -n NAME       Target Sequence Name
  --host HOST           Mosaico Server Host
  --port PORT           Mosaico Server Port (Default: 6726)
  --topics TOPICS [TOPICS ...]
                        Specific topics to filter (supports glob patterns like /cam/*)
  --metadata METADATA   JSON string or path to JSON file containing sequence metadata
  --ros-distro {empty,latest,ros1_noetic,ros2_dashing,ros2_eloquent,ros2_foxy,ros2_galactic,ros2_humble,ros2_iron,ros2_jazzy,ros2_kilted}
                        Target ROS Distribution for message parsing (e.g., ros2_humble). If not set, defaults to EMPTY.
```

#### Programmatic Usage (Python Script)

For advanced workflows, such as integrating with CI/CD pipelines or custom automation scripts, you can use the `RosbagInjector` class directly within Python code.
This approach offers full typed control over the configuration and avoids the overhead of spawning subprocesses for the CLI.

```python
from pathlib import Path
from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig, Stores

def run_injection():
    # Define the Injection Configuration
    # This data class acts as the single source of truth for the operation.
    config = ROSInjectionConfig(
        # Input Data
        file_path=Path("data/session_01.db3"),
        
        # Target Platform Metadata
        sequence_name="test_ros_sequence",
        metadata={
            "driver_version": "v2.1", 
            "weather": "sunny",
            "location": "test_track_A"
        },
        
        # Topic Filtering (supports glob patterns)
        # This will only upload topics starting with '/cam'
        topics=["/cam*"],
        
        # ROS Configuration
        # Specifying the distro ensures correct parsing of standard messages
        # (.db3 sqlite3 rosbags need the specification of distro)
        ros_distro=Stores.ROS2_HUMBLE,
        
        # Custom Message Registration
        # Register proprietary messages before loading to prevent errors
        custom_msgs=[
            (
                "my_custom_pkg",                 # ROS Package Name
                Path("./definitions/my_pkg/"),   # Path to directory containing .msg files
                Stores.ROS2_HUMBLE,              # Scope (valid for this distro)
            ) # registry will automatically infer type names as `my_custom_pkg/msg/{filename}`
        ],
        
        # Execution Settings
        log_level="WARNING",  # Reduce verbosity for automated scripts
    )

    # Instantiate the Controller
    injector = RosbagInjector(config)

    # Execute
    # The run method handles connection, loading, and uploading automatically.
    # It raises exceptions for fatal errors, allowing you to wrap it in try/except blocks.
    try:
        injector.run()
        print("Injection job completed successfully.")
    except Exception as e:
        print(f"Injection job failed: {e}")

# Use as script or call the injection function in your code
if __name__ == "__main__":
    run_injection()
```

For more details on the injection process, refer to the [ROS Bridge Documentation](./doc-md/ros_bridge.md).

### Data Retrieval and Usage

Retrieving data from Mosaico follows a two-step pattern: first, obtain a **Handler** to inspect the resource's metadata, and then initialize a **Streamer** to consume the actual data flow. The SDK provides two primary handlers:

1.  **`SequenceHandler`**: Represents a complete recording session. It provides access to global metadata, the list of contained topics, and storage statistics. It serves as a factory for the `SequenceDataStreamer`, which delivers a unified, time-synchronized stream of **all** topics in the sequence.
2.  **`TopicHandler`**: Represents a specific data channel within a sequence (e.g., just the IMU or Left Camera). It provides topic-specific metadata and creates a `TopicDataStreamer` for efficient, targeted data access.

#### Sequence Stream (Unified Replay)

The `SequenceHandler` is ideal for sensor fusion tasks or full system replay. When you request a streamer from it, the SDK automatically performs a client-side **k-way merge sort**. This ensures that messages from all sensors are yielded in strict chronological order, regardless of how they were stored.

To start reading, call the `.get_data_streamer()` method, which returns a `SequenceDataStreamer` iterator.

```python
from mosaicolabs import MosaicoClient

client = MosaicoClient.connect("localhost", 6726)

# 1. Get the Handler (Catalog Access)
# This fetches metadata without downloading the full dataset.
seq_handler = client.sequence_handler("my_recorded_sequence")

if seq_handler:
    print(f"Sequence '{seq_handler.name}' contains: {seq_handler.topics}")

    # 2. Get the Streamer (Data Access)
    # This opens the connection and initializes buffers for time-synchronization.
    # Data will be fetched in small batches as you iterate.
    streamer = seq_handler.get_data_streamer()

    # 3. Consume the stream
    # The iterator yields a tuple: (topic_name, message_object)
    for topic, message in streamer:
        print(f"[{message.timestamp}] {topic}: {type(message.data).__name__}")

client.close()
```

####  Topic Stream (Targeted Access)

If you only need data from a specific sensor, it is much more network-efficient to stream just that topic.

You can obtain a `TopicHandler` either directly from the client or hierarchically from a `SequenceHandler`. Calling `.get_data_streamer()` on it returns a `TopicDataStreamer`.

```python
from mosaicolabs import MosaicoClient

client = MosaicoClient.connect("localhost", 6726)

# Option A: Get directly from Client using full path
topic_handler = client.topic_handler(sequence_name="my_sequence", topic_name= "sensors/imu")

# Option B: Get from a Sequence Handler
# seq_handler = client.sequence_handler("my_sequence")
# topic_handler = seq_handler.get_topic_handler("sensors/imu")

if topic_handler:
    print(f"Streaming topic: {topic_handler.name}")
    
    # Get the Streamer
    # Opens a direct Flight stream for this single topic (no merging overhead)
    t_streamer = topic_handler.get_data_streamer()

    # Consume the stream
    # Yields the message object directly (no tuple)
    for message in t_streamer:
        imu_data = message.data
        print(f"Accel: {imu_data.acceleration.x}")

client.close()
```


> [!NOTE] 
> **Memory Efficiency**
>
> The data stream is **not** downloaded all at once, as this would drain the RAM for long sequences. Instead, the SDK implements a smart buffering strategy: data is retrieved in **batches of limited memory**. As you iterate through the stream, processed batches are discarded and substituted automatically with new batches fetched from the server. This ensures you can process sequences far larger than your available RAM without performance degradation.


#### *Look-Ahead (Peeking)*

Both `SequenceDataStreamer` and `TopicDataStreamer` expose a `next_timestamp()` method. This allows you to inspect the acquisition time of the *next* available message **without consuming it** (i.e., without removing it from the stream buffer).

This is particularly useful for synchronizing Mosaico data with external clocks or simulation steps.

```python
# Peek at the time of the next message
next_time = streamer.next_timestamp()

if next_time:
    print(f"Next event is at {next_time}")
    
    # The next loop iteration will still yield the message we just peeked at
    topic, msg = next(streamer) 
    assert msg.timestamp == next_time
```

####  Tabular Extraction (ML & Analytics)

For users working with Machine Learning, Scikit-Learn, or deep learning frameworks, Mosaico provides the [`DataFrameExtractor`](./doc-md/ml/dataframe_extractor.md). This utility transforms sparse sensor streams into flattened, tabular DataFrames.

```python
from mosaicolabs.ml import DataFrameExtractor

seq_handler = client.sequence_handler("my_sequence")
extractor = DataFrameExtractor(seq_handler)

# Get 5-second windowed DataFrames
for df in extractor.to_pandas_chunks(window_sec=5.0):
    # df is a pandas.DataFrame with flattened columns like '/front_imu.acceleration.x'
    model.predict(df.fillna(0))

```

For more details on ML-specific features, refer to the **[ML Module Documentation](./doc-md/ml/ml.md)**.

### Documentation

Detailed documentation for specific modules can be found here:

  * [**Data Models & Ontology**](./doc-md/ontology.md): Understanding `Serializable`, `Message`, and data ontology types.
  * [**Communication**](./doc-md/communication.md): Client architecture and connection pooling.
  * [**Handlers (Writers/Readers)**](./doc-md/handlers.md): How to stream data to/from the platform.
  * [**Querying Remote Data**](./doc-md/queries.md): How to retrieve specific data by querying (topic/sequence) metadata and ontology time-series content.
  * **[ML Module](./doc-md/ml/ml.md)**: Bridge between Mosaico and the modern Data Science and AI ecosystem.
  * [**ROS Bridge**](./doc-md/ros_bridge.md): Adapters and loaders for ROS 1/2.
