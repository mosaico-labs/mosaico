---
title: ROS Ingestion
description: Example how-to for ROS Ingestion
---

This tutorial demonstrates a complete Mosaico data ingestion using the [NVIDIA R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1). You will learn how to automate the transition from monolithic ROS bags (.mcap) to a structured, queryable archive.

By following this guide, you will:

* **Automate Asset Preparation**: Programmatically download and manage remote datasets.
* **Implement Adaptation Logic**: Translate raw ROS types into the strongly-typed Mosaico Ontology.
* **Execute Ingestion**: Use the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] to handle batching and network transmission.
* **Verify Integrity**: Programmatically inspect the server to ensure the data is cataloged.

!!! example "Experiment Yourself"
    This guide is **fully executable**.

    1. **[Start the Mosaico Infrastructure](../../daemon/install.md)**
    2. **Run the example**
    ```bash
    mosaicolabs.examples ros_injection
    ```

??? question "In Depth Explanation"
    * **[How-To: Customizing the Data Ontology](./ontology_customization.md)**
    * **[Documentation: The ROS Bridge](../bridges/ros.md)**
    * **[API Reference: The ROS Bridge](../API_reference/bridges/ros/ros.md)**

## Step 1: Custom Ontology Definition (`isaac.py`)

In Mosaico, data is strongly typed. When dealing with specialized hardware like the NVIDIA Isaac Nova encoders, with custom data models, not available in the SDK, we must define a model that the platform understands.

### The Data Model

The `EncoderTicks` class defines the physical storage format.

```python
import pyarrow as pa
from mosaicolabs import Serializable

class EncoderTicks(Serializable): # (1)!
    # --- Wire Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct([
        pa.field("left_ticks", pa.uint32(), nullable=False),
        pa.field("right_ticks", pa.uint32(), nullable=False),
        pa.field("encoder_timestamp", pa.uint64(), nullable=False),
    ])

    # --- Pydantic Fields ---
    left_ticks: int # (2)!
    right_ticks: int
    encoder_timestamp: int

```

1. Inheriting from [`Serializable`][mosaicolabs.models.Serializable] automatically registers your model in the Mosaico ecosystem, making it dispatchable to the data platform, and enables the `.Q` [query proxy](../query.md#the-q-proxy-mechanism).
2. The field names in the `pa.struct` **must match exactly** the names of the Python attributes.

??? question "In Depth Explanation"
    * **[How-To: Customizing the Data Ontology](./ontology_customization.md)**
    * **[Documentation: Data Models & Ontology](../ontology.md)**

## Step 2: Implementing the ROS Adapter (`isaac_adapters.py`)

The translation between the raw ROS dictionary and the Mosaico ontology data model is made via *adapters*. Being a custom message type, `"isaac_ros_nova_interfaces/msg/EncoderTicks"` does not have a default adapter. Therefore we must define one:
the [`ROSAdapterBase`][mosaicolabs.ros_bridge.adapter_base.ROSAdapterBase] class provides the necessary infrastructure for this. We just need to implement the `from_dict` method, which is responsible for converting the raw ROS message dictionary into our custom ontology model.

### The Adapter Implementation

```python
from mosaicolabs.ros_bridge import ROSMessage, ROSAdapterBase, register_default_adapter
from mosaicolabs.ros_bridge.adapters.helpers import _validate_msgdata
from .isaac import EncoderTicks

@register_default_adapter
class EncoderTicksAdapter(ROSAdapterBase[EncoderTicks]):
    ros_msgtype = ("isaac_ros_nova_interfaces/msg/EncoderTicks",) # (1)!
    __mosaico_ontology_type__ = EncoderTicks # (2)!
    _REQUIRED_KEYS = ("left_ticks", "right_ticks", "encoder_timestamp") # (3)!

    @classmethod
    def from_dict(cls, ros_data: dict) -> EncoderTicks: # (4)!
        """
        Convert a ROS message dictionary to an EncoderTicks object.
        """
        _validate_msgdata(cls, ros_data)
        return EncoderTicks(
            left_ticks=ros_data["left_ticks"],
            right_ticks=ros_data["right_ticks"],
            encoder_timestamp=ros_data["encoder_timestamp"],
        )

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message: # (5)!
        """
        Translates a ROS EncoderTicks message into a Mosaico Message container.
        """
        return super().translate(ros_msg, **kwargs)
```

1. A tuple of strings representing the ROS message types that this adapter can handle.
2. The Mosaico ontology type that this adapter can handle.
3. A tuple of strings representing the required keys in the ROS message. This is used by the `_validate_msgdata` method to check that the ROS message does contains the required fields.
4. This is the heart of the translator. It takes a Python dictionary and maps the keys to our `EncoderTicks` ontology model.
5. This method is called by the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] class for each message in the bag. It is responsible for converting the raw ROS message dictionary into the Mosaico message,
wrapping the custom ontology model.

**Key Operation**: The [`@register_default_adapter`][mosaicolabs.ros_bridge.register_default_adapter] decorator ensures the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] automatically knows how to handle this message type during the ingestion loop.

## Step 3: Orchestrating the Ingestion Loop

In a real-world scenario, you often need to ingest a batch of files. The `main.py` implementation uses a 3-phase loop to manage this workflow.

### Phase 1: Asset Preparation

Before we can ingest data, we need the raw file. This phase downloads a verified dataset from NVIDIA.

```python
for bag_path in BAG_FILES_PATH:
    bag_file_url = BASE_BAGFILE_URL + bag_path
    # This utility handles downloads with visual progress bars
    out_bag_file = download_asset(bag_file_url, ASSET_DIR)
```

### Phase 2: High-Performance Ingestion

Configure and run the injector. This layer handles connection pooling, asynchronous serialization, and batching.

```python
config = ROSInjectionConfig(
        host=MOSAICO_HOST,
        port=MOSAICO_PORT,
        file_path=out_bag_file,
        sequence_name=out_bag_file.stem,  # Sequence name derived from filename
        # Some example metadata for the sequence
        metadata={
            "source_url": BAGFILE_URL,
            "ingested_via": "mosaico_example_ros_injection",
            "download_time_utc": str(downloaded_time),
        },
        log_level="INFO",
    )

# Handles connection, loading, adaptation, and batching
injector = RosbagInjector(config)
injector.run()  # Starts the ingestion process
```

### Phase 3: Verification & Retrieval

Programmatically confirm the sequence is available on the server using a context manager.

```python
with MosaicoClient.connect(host=MOSAICO_HOST, port=MOSAICO_PORT) as client: # (1)!
    seq_list = client.list_sequences() # (2)!
    if config.sequence_name in seq_list:
        print(f"Success: '{config.sequence_name}' is now queryable.")
```

1. Establishes a secure connection to the platform.
2. Asks for the lists of sequences on the server

??? question "In Depth Explanation"
    * **[How-To: Data Discovery and Inspection](./data_inspection.md)**
    * **[Documentation: The Reading Workflow](../handling/reading.md)**
    * **[API Reference: Data Retrieval](../API_reference/handlers/reading.md)**

## The full example code
The full example code is available under `mosaico-sdk-py/src/examples/ros_injection/main.py`.

### What to Expect

* **CLI Progress**: You will see a detailed breakdown of each ROS topic being resolved, adapted, and uploaded in real-time.
* **Zero-Copy Performance**: Behind the scenes, the SDK uses Apache Arrow to move data without the CPU overhead of traditional serialization.
