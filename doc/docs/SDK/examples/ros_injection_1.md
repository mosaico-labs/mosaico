# ROS Injection Example

This example provides a detailed, step-by-step walkthrough of a complete Mosaico data pipeline, from raw ROS bag ingestion to custom ontology creation and verification. It demonstrates how to bridge the gap between **Robot Operating System (ROS)** data and the **Mosaico Data Platform**. By following this pipeline, you will learn how to:

1. **Define a Custom Ontology**: Create a strongly-typed data model that matches a specific hardware sensor.
2. **Implement a ROS Adapter**: Build a translator that converts raw ROS dictionaries into your custom Mosaico model.
3. **Automate Ingestion**: Use a high-performance injector to upload a complete recording (MCAP) to the server.
4. **Verify Results**: Programmatically inspect the ingested data to ensure structural integrity.

The injected sequence is the `r2b_whitetunnel_0` sequence from the [NVIDIA R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1).


## Running the example

From `mosaico-sdk-py` root directory:

```bash
cd examples
poetry run python -m ros_injection.main
```

You should see output similar to the following:

```bash
Downloading: https://api.ngc.nvidia.com/v2/resources/org/nvidia/team/isaac/r2bdataset2024/1/files?redirect=true&path=r2b_whitetunnel/r2b_whitetunnel_0.mcap
Fetching r2b_whitetunnel_0.mcap ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 527.0/527.0 MB XY.Z MB/s 0:00:00

╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ Phase 2: Starting ROS Ingestion                                                                                                         │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
[14:29:38] INFO     mosaicolabs: SDK Logging initialized at level: INFO                                                logging_config.py:99
           INFO     mosaicolabs.ros_bridge.injector: Connecting to Mosaico at 'localhost:6276'...                           injector.py:266
[14:29:40] INFO     mosaicolabs.ros_bridge.injector: Opening bag: '/tmp/mosaico_assets/r2b_whitetunnel_0.mcap'              injector.py:274
           INFO     mosaicolabs.ros_bridge.injector: Starting upload...                                                     injector.py:291
/back_stereo_camera/left/camera_info               ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         564/564         100.0%   •   0:00:00   •   0:00:24     
/back_stereo_camera/left/image_compressed          ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         525/525         100.0%   •   0:00:00   •   0:00:24     
/back_stereo_camera/right/camera_info              ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         564/564         100.0%   •   0:00:00   •   0:00:24     
/back_stereo_camera/right/image_compressed         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         490/490         100.0%   •   0:00:00   •   0:00:24     
/chassis/battery_state                             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         27/27           100.0%   •   0:00:00   •   0:00:19     
/chassis/imu                                       ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         1080/1080       100.0%   •   0:00:00   •   0:00:24     
/chassis/odom                                      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         1080/1080       100.0%   •   0:00:00   •   0:00:24     
/chassis/ticks                                     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         1081/1081       100.0%   •   0:00:00   •   0:00:24     
/front_stereo_camera/left/camera_info              ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         562/562         100.0%   •   0:00:00   •   0:00:24     
/front_stereo_camera/left/image_compressed         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         521/521         100.0%   •   0:00:00   •   0:00:24     
/front_stereo_camera/right/camera_info             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         562/562         100.0%   •   0:00:00   •   0:00:24     
/front_stereo_camera/right/image_compressed        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         515/515         100.0%   •   0:00:00   •   0:00:24     
/front_stereo_imu/imu                              ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         1761/1761       100.0%   •   0:00:00   •   0:00:24     
/left_stereo_camera/left/camera_info               ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         527/527         100.0%   •   0:00:00   •   0:00:24     
/left_stereo_camera/left/image_compressed          ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         513/513         100.0%   •   0:00:00   •   0:00:24     
/left_stereo_camera/right/camera_info              ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         527/527         100.0%   •   0:00:00   •   0:00:24     
/left_stereo_camera/right/image_compressed         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         498/498         100.0%   •   0:00:00   •   0:00:24     
/right_stereo_camera/left/camera_info              ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         488/488         100.0%   •   0:00:00   •   0:00:24     
/right_stereo_camera/left/image_compressed         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         488/488         100.0%   •   0:00:00   •   0:00:24     
/right_stereo_camera/right/camera_info             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         488/488         100.0%   •   0:00:00   •   0:00:24     
/right_stereo_camera/right/image_compressed        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         488/488         100.0%   •   0:00:00   •   0:00:24     
/tf                                                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         4282/4282       100.0%   •   0:00:00   •   0:00:24     
/tf_static                                         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         1/1             100.0%   •   0:00:00   •   0:00:00     
Total Upload                                       ━━━━━━━━━━━━━━━━━━━━━━━━━━━━         17632/17632     100.0%   •   0:00:00   •   0:00:24     

...Other logging messages
```

## Step-by-Step Guide

### Step 1: Custom Ontology Definition (`isaac.py`)

In Mosaico, data is not just "blobs"; it is strongly typed. When dealing with specialized hardware like the NVIDIA Isaac Nova encoders, with custom data models, not available in the SDK, we must define a model that the platform understands.

#### The Data Model

The `EncoderTicks` class defines the physical storage format.

```python
import pyarrow as pa
from mosaicolabs import HeaderMixin, Serializable

class EncoderTicks(Serializable, HeaderMixin):
    # --- Wire Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct([
        pa.field("left_ticks", pa.uint32(), nullable=False),
        pa.field("right_ticks", pa.uint32(), nullable=False),
        pa.field("encoder_timestamp", pa.uint64(), nullable=False),
    ])

    # --- Pydantic Fields ---
    left_ticks: int
    right_ticks: int
    encoder_timestamp: int

```

**What is happening here?**

* **[`Serializable`](../ontology.md)**: Inheriting from this class automatically registers your model in the Mosaico ecosystem, making it dispatchable to the data platform, and enables the `.Q` query proxy.
* **[`HeaderMixin`](../ontology.md)**: This "injects" a standard `header` (including a timestamp and frame ID) into your model, ensuring it remains compatible with time-series analysis.
* **Schema Alignment**: The field names in the `pa.struct` (binary format) **must match exactly** the names of the Python attributes.

### Step 2: Implementing the ROS Adapter (`isaac_adapters.py`)

A ROS Bag contains raw data dictionaries. To ingest them, we need an "Adapter" that acts as a semantic translator between the ROS world and the Mosaico Ontology.
The [`ROSAdapterBase`][mosaicolabs.ros_bridge.ROSAdapterBase] class provides the necessary infrastructure for this. We just need to implement the `from_dict` method, which is responsible for converting the raw ROS message dictionary into our custom ontology model.
The adaptation between the ROS message and the Mosaico message is done via the `translate` method, which is inherited from the `ROSAdapterBase` class, which is called by the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] class for each message in the bag.

#### The Adapter Implementation

```python
from mosaicolabs.ros_bridge import ROSMessage, ROSAdapterBase, register_adapter
from mosaicolabs.ros_bridge.adapters.helpers import _make_header, _validate_msgdata
from .isaac import EncoderTicks

@register_adapter
class EncoderTicksAdapter(ROSAdapterBase[EncoderTicks]):
    ros_msgtype = ("isaac_ros_nova_interfaces/msg/EncoderTicks",)
    __mosaico_ontology_type__ = EncoderTicks
    _REQUIRED_KEYS = ("left_ticks", "right_ticks", "encoder_timestamp")

    @classmethod
    def from_dict(cls, ros_data: dict) -> EncoderTicks:
        """
        Convert a ROS message dictionary to an EncoderTicks object.
        """
        _validate_msgdata(cls, ros_data)
        return EncoderTicks(
            header=_make_header(ros_data.get("header")),
            left_ticks=ros_data["left_ticks"],
            right_ticks=ros_data["right_ticks"],
            encoder_timestamp=ros_data["encoder_timestamp"],
        )

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS EncoderTicks message into a Mosaico Message container.
        """
        return super().translate(ros_msg, **kwargs)

```

**Key Operations:**

* **`@register_adapter`**: This decorator tells the Mosaico ROS Bridge: "Whenever you see a message of type `isaac_ros_nova_interfaces/msg/EncoderTicks`, use this class to translate it".
* **`_validate_msgdata`**: This ensures the raw ROS message actually contains the fields we expect (`left_ticks`, etc.) before we try to process it.
* **`from_dict`**: This is the heart of the translator. It takes a Python dictionary and maps the keys to our `EncoderTicks` ontology model.
* **`translate`**: This method is called by the `RosbagInjector` class for each message in the bag. It is responsible for converting the raw ROS message dictionary into the Mosaico message,
wrapping thecustom ontology model.


### Step 3: The Execution Pipeline (`ros_injection.py`)

The main script orchestrates the entire process in three distinct phases.

#### Phase 1: Asset Preparation

Before we can ingest data, we need the raw file. This phase downloads a verified dataset from NVIDIA.

```python
# --- PHASE 1: Asset Preparation ---
try:
    out_bag_file = download_asset(BAGFILE_URL, ASSET_DIR)
except Exception as e:
    log.error(f"Failed to prepare asset: {e}")
    sys.exit(1)
```

#### Phase 2: High-Performance Injection

This is where the ROS Bridge takes over. It opens the bag, applies our custom `EncoderTicksAdapter`, plus the adapters already available in the SDK, and streams the data to the server.

```python
# Configure the ROS injection. This uses the 'Adaptation' philosophy to translate
# ROS types into the Mosaico Ontology.
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

The **[`ROSInjectionConfig`][mosaicolabs.ros_bridge.ROSInjectionConfig]** defines where the server is and what metadata to attach to the sequence.
The **[`injector.run()`][mosaicolabs.ros_bridge.RosbagInjector.run]** method handles the heavy lifting—file loading, message adaptation, and network batching—automatically.
Inside its logics, the **[`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector]** uses the SDK features to:

* **Connect** to the Mosaico Server using the provided host and port.
* **Create a new sequence** in the server via the `MosaicoClient.sequence_create()` API.
* **Adapt** the messages to the Mosaico Ontology using the provided adapters.
* **Spawn Topic writers** via the `SequenceWriter.topic_create()` API to write the adapted messages to the server in batches, via the `TopicWriter.push()` method.

#### Phase 3: Verification & Retrieval

Once the upload is finished, we connect to the Mosaico Server to retrieve the data from the sequence just created.

```python
with MosaicoClient.connect(host=MOSAICO_HOST, port=MOSAICO_PORT) as client:
    # Ask for a SequenceHandler for the sequence we just created. 
    # The sequence is identified by its name, which is the stem of the bagfile.
    shandler = client.sequence_handler(out_bag_file.stem)

    # Print some information about the sequence    
    print(f"Sequence Name: {shandler.name}")
    print(f"Topics Found: {len(shandler.topics)}")
    # ...

```

**Operations:**

* **`MosaicoClient.connect()`**: Establishes a secure connection to the platform.
* **`MosaicoClient.sequence_handler()`**: Retrieves a specialized object used to manage and query the specific recording we just uploaded.

---

## Conclusion

By combining custom **Ontology Models** with specialized **ROS Adapters**, the Mosaico SDK allows you to transform messy, unorganized robotics data into a clean, searchable, and strongly-typed catalog.