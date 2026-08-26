---
title: ROS
description: ROS-Mosaico Bridge
---

The **ROS Bridge** module serves as the bidirectional gateway between ROS (Robot Operating System) data and the Mosaico Data Platform: **ingesting** ROS bag files into Mosaico sequences (via [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector]), and **extracting** Mosaico sequences back out as ROS bag files (via [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor]). Its primary function is to solve the interoperability challenges associated with ROS bag files—specifically format fragmentation (ROS 1 `.bag` vs. ROS 2 `.mcap`/`.db3`) and the lack of strict schema enforcement in custom message definitions.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the ROS Ingestion employs the mosaico [Writing Workflow](../handling/writing.md), which is allowed only if the key has the `write` permission.


The core philosophy of the module is **"Adaptation, Not Just Parsing."** Rather than simply extracting raw dictionaries from ROS messages, the bridge actively translates them into the standardized **Mosaico Ontology**. For example, a [`geometry_msgs/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html) is validated, normalized, and instantiated as a strongly-typed [`mosaicolabs.models.data.Pose`][mosaicolabs.models.data.Pose] object before ingestion.

!!! example "Try-It Out"
    You can experiment yourself the ROS Bridge ingestion via the **[ROS Ingestion](https://docs.mosaico.dev/examples/ros_ingestion) Example**.

## Architecture

The module is composed of five collaborating components that handle both directions of the pipeline — ROS bag → Mosaico (ingestion) and Mosaico → ROS bag (extraction) — from raw file/server access down to the shared adaptation layer.

### The Loaders (`ROSLoader` & `MosaicoLoader`)

Each direction has its own loader:

* **[`ROSLoader`][mosaicolabs.bridges.ros.loader.ROSLoader]** (ingestion) acts as the abstraction layer over the physical bag files. It utilizes the [`rosbags`](https://pypi.org/project/rosbags/) library to provide a unified interface for reading both ROS 1 and ROS 2 formats (`.bag`, `.db3`, `.mcap`).
* **[`MosaicoLoader`][mosaicolabs.bridges.ros.loader.MosaicoLoader]** (extraction) is the mirror image: it streams messages back out of a Mosaico sequence via [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer], resolving each topic's adapter and original ROS message type instead of parsing one from a bag file.

Both loaders share the same topic classification logic (accepted / filtered / adapter-unresolved, plus source-specific rejection reasons), which is what powers `topics`, `rejected_topics`, and `resolve_adapter()` identically on either side.

* **Responsibilities:** raw deserialization (or, for `MosaicoLoader`, remote streaming) and topic filtering (supporting glob patterns like `/cam/*`).
* **Error Handling:** rejected topics are reported with a specific reason (filtered, no adapter, not in typestore, malformed metadata) rather than aborting the whole run; malformed *messages* on an otherwise-accepted topic are skipped and counted rather than raised.

!!! note "Typestore setup is the caller's responsibility"
    Neither loader consults the [`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry] itself — both take an already-built [`Typestore`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#typestores) as a constructor argument. Resolving `ros_distro`/`custom_msgs` into that `Typestore` is done by [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector] and [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] before they construct a loader — see [The Type Registry](#the-type-registry-rostyperegistry) below.

### The Orchestrator (`RosbagInjector`)

The **[`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector]** is the central command center of the ROS Bridge module. It is designed to be the primary entry point for developers who want to embed high-performance ROS ingestion directly into their Python applications or automation scripts.

The ingestor orchestrates the interaction between the **[`ROSLoader`][mosaicolabs.bridges.ros.loader.ROSLoader]** (file access), the **[`ROSBridge`][mosaicolabs.bridges.ros.ROSBridge]** (data adaptation), and the **[`MosaicoClient`][mosaicolabs.comm.MosaicoClient]** (network transmission). It handles the complex lifecycle of a data upload—including connection management, batching, and transaction safety—while providing real-time feedback through a visual CLI interface.

#### Core Workflow Execution: `run()`

Constructing `RosbagInjector(config)` resolves `ros_distro`/`custom_msgs` into a `Typestore` via the [Type Registry](#the-type-registry-rostyperegistry) up front. `run()` itself then drives a multi-phase pipeline:

1. **Handshake**: Establishes a connection to the Mosaico server and opens the bag file via `ROSLoader`.
2. **Sequence Creation**: Requests the server to initialize a new data sequence based on the provided name and metadata.
3. **Adaptive Streaming**: Iterates through the ROS bag records. For each message, it identifies the correct adapter, translates the ROS dictionary into a Mosaico object, and pushes it into an optimized write buffer.
4. **Transaction Finalization**: Once the bag is exhausted, it flushes all remaining buffers and signals the server to commit the sequence.

#### Configuring the Ingestion

The behavior of the ingestor is entirely driven by the **[`ROSInjectionConfig`][mosaicolabs.bridges.ros.ROSInjectionConfig]**. This configuration object ensures that the ingestion logic is decoupled from the user interface, allowing for consistent behavior whether triggered via the CLI or a complex script.

#### Per-Topic Metadata (`topic_metadata`)

Besides the sequence-level `metadata` dict, `topic_metadata: Optional[Dict[str, dict]]` lets you attach metadata to individual topics by exact topic name (the same exact-name convention as `adapter_overrides` and `topics_on_error`, rather than the glob patterns used by `topics`). Entries for topics excluded by `topics` filtering are simply unused. See [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields) below for how it merges with metadata the bridge computes automatically.

```python
config = ROSInjectionConfig(
    ...,
    topic_metadata={
        "/imu": {"unit": "rad/s"},
    },
)
```

It can also be loaded from a JSON file — handy for the CLI's `--topic-metadata` flag, or to keep large mappings out of your script:

```json title="topic_metadata.json"
{
  "/imu": {"unit": "rad/s"},
  "/estimation/pose": {"algorithm": "ekf_v2", "offline": true}
}
```

```python
import json

config = ROSInjectionConfig(
    ...,
    topic_metadata=json.loads(Path("topic_metadata.json").read_text()),
)
```

#### Updating an Existing Sequence (`update_if_exists`)

By default, ingesting into a `sequence_name` that already exists on the server raises an error. Setting **`update_if_exists=True`** switches this to an append/merge: the ingestor appends this bag's topics to the existing sequence instead of creating a new one. This covers two distinct real-world scenarios with the same mechanism:

* **Multi-part recordings**: a single logical recording split across several bag files, all of which should land in one sequence.
* **Reprocessing / augmentation**: a derived bag (e.g. offline estimation results computed from an already-ingested recording, with timestamps aligned to the original) whose topics should be merged into the sequence that was already ingested from the original recording.

Since sequence metadata can't be changed after creation, the per-topic `source_file` metadata (see [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields) below) is what keeps this traceable: inspecting a topic's own metadata tells you which bag file introduced it, even after several `update_if_exists=True` runs against the same sequence.

```python
config = ROSInjectionConfig(
    file_path=Path("estimation_results.mcap"),
    sequence_name="on_track_experiment",  # already ingested from the original recording
    metadata={},  # ignored: the sequence already exists, its metadata is immutable
    update_if_exists=True,
    topic_metadata={
        "/estimation/pose": {"algorithm": "ekf_v2", "offline": True},
    },
)
```

If `sequence_name` doesn't exist yet, `update_if_exists=True` simply creates it, same as leaving it at the default `False`.

!!! warning "Resuming after a crash is not idempotent"
    If the process crashes mid-bag and you re-run the same command with `update_if_exists=True`, the injector has no memory of which topics it already fully ingested before the crash — that bookkeeping lives only in an in-memory cache scoped to the crashed process's session. Expect `topic_create` to be called again for those topics on resume, which the server is expected to reject as duplicates. There is currently no built-in dedup against the sequence's already-existing topics before re-creating them, so a genuinely safe resume isn't supported yet — plan for re-ingesting into a fresh sequence name if a run fails partway through, rather than relying on `update_if_exists` to pick up where it left off.

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs import SessionLevelErrorPolicy, TopicLevelErrorPolicy
from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig
from rosbags.typesys import Stores

def run_injection():
    # Define the Injection Configuration
    # This data class acts as the single source for the operation.
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
        
        # Adapter Overrides
        # Use specific adapters for designated topics instead of the default.
        # In this case, instead to use PointCloudAdapter for depth camera,
        # MyCustomRGBDAdapter will be used for the specified topic.
        adapter_overrides={
            "/camera/depth/points": MyCustomRGBDAdapter,
        },

        # Per-Topic Metadata (exact topic name -> dict; the reserved "_ros_" key,
        # containing schema info and source_file, always overrides this on conflict)
        topic_metadata={
            "/cam/front": {"lens": "wide-angle", "calibrated": True},
        },

        # Update instead of Create
        # If "test_ros_sequence" already exists (e.g. a previous bag of a multi-part
        # recording, or a sequence to merge reprocessed results into), append to it
        # instead of raising an error.
        update_if_exists=False,

        # Execution Settings
        log_level="WARNING",  # Reduce verbosity for automated scripts

        # Session Level Error Handling
        on_error=SessionLevelErrorPolicy.Report, # Report the error and terminate the session

        # Topic Level Error Handling
        topics_on_error=TopicLevelErrorPolicy.Raise # Re-raise any exception
    )

    # Instantiate the Controller
    ingestor = RosbagInjector(config)

    # Execute
    # The run method handles connection, loading, and uploading automatically.
    # It raises exceptions for fatal errors, allowing you to wrap it in try/except blocks.
    try:
        ingestor.run()
        print("Injection job completed successfully.")
    except Exception as e:
        print(f"Injection job failed: {e}")

# Use as script or call the injection function in your code
if __name__ == "__main__":
    run_injection()
```

### The Adaptation Layer (`ROSBridge` & Adapters)

This layer represents the default semantic core of the module, translating raw ROS data into the Mosaico Ontology.

* **[`ROSAdapterBase`][mosaicolabs.bridges.ros.adapter_base.ROSAdapterBase]:** An abstract base class that establishes the **default** contracts for converting specific ROS message types into their corresponding Mosaico Ontology types.
* **Concrete Adapters:** The library provides built-in implementations for common standards, such as [`IMUAdapter`][mosaicolabs.bridges.ros.adapters.sensor_msgs.IMUAdapter] (mapping `sensor_msgs/Imu` to [`IMU`][mosaicolabs.models.sensors.IMU]) and [`ImageAdapter`][mosaicolabs.bridges.ros.adapters.sensor_msgs.ImageAdapter] (mapping `sensor_msgs/Image` to [`Image`][mosaicolabs.models.sensors.Image]). These adapters include advanced logic for recursive unwrapping, automatically extracting data from complex nested wrappers like [`PoseWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseWithCovarianceStamped.html). Developers can also implement custom adapters to handle non-standard or proprietary types.
* **[`ROSBridge`][mosaicolabs.bridges.ros.ROSBridge]:** A central registry and dispatch mechanism that maps ROS message type strings (e.g., [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html)) to their corresponding default adapter classes, ensuring the correct translation logic is applied for each message.

#### Extending the Bridge (Custom Adapters)

Users can extend the bridge to support new ROS message types by implementing a custom adapter and registering it.

1.  **Inherit from `ROSAdapterBase`**: Define the input ROS type string and the target Mosaico Ontology type.
2.  **Implement `from_dict`**: Define the logic to convert the [`ROSMessage.data`][mosaicolabs.bridges.ros.ROSMessage] dictionary into an instance of the target ontology object.
3.  **Register**: Decorate the class with [`@register_default_adapter`][mosaicolabs.bridges.ros.register_default_adapter].

```python
from mosaicolabs import Message
from mosaicolabs.bridges.ros import ROSAdapterBase, register_default_adapter, ROSMessage
from my_ontology import MyCustomData # Assuming this class exists

@register_default_adapter
class MyCustomAdapter(ROSAdapterBase[MyCustomData]):
    ros_msgtype = "my_pkg/msg/MyCustomType"
    __mosaico_ontology_type__ = MyCustomData

    @classmethod
    def from_dict(cls, ros_data: dict, **kwargs) -> MyCustomData:
        # Transformation logic here
        return MyCustomData(...)
```

#### Extending the Bridge (Unmodeled Adapters)

Not every ROS message type needs a hand-written adapter before it can be ingested. When the bridge encounters a topic whose message type has no registered adapter — a proprietary or custom `.msg`/`.idl` definition, for instance — it doesn't reject the topic. Instead, it synthesizes an **[`UnmodeledAdapter`][mosaicolabs.bridges.ros.adapters.unmodeled.UnmodeledAdapter]** for it at runtime, transparently and with no user intervention required, capable of translating that type in both directions: ROS bag to Mosaico, and back again from Mosaico to ROS.

This is possible because ROS bag files carry the schema of every message type alongside the raw data. The bridge converts that schema into an equivalent PyArrow schema and wraps it into a dynamically-generated [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology class via [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class] — the same mechanism the Mosaico ontology system uses for any schema that isn't backed by a hand-authored Python class.

!!! important "Const/enum data is not part of the message payload"
    ROS message constants (`UPPER_CASE` fields, e.g. `uint8 STATUS_FIX=0`) are not ingested as part of each message's data. Since they're fixed per message *type* rather than per message *instance*, they are extracted once and stored as **topic metadata** (alongside the original ROS message type and definition) instead of being duplicated into every ingested row.

See [Advanced: Ingesting Unmodeled Ontologies](../ontology.md#advanced-ingesting-unmodeled-ontologies) for the full mechanics of the `Unmodeled` ontology type — including how schema-variant fingerprinting keeps multiple versions of the same tag apart, and, most importantly, how to **query** this data on the server without ever needing to resolve a Python class for it.

#### Metadata: Reserved Keys & Custom Fields

!!! info "Internal behavior"
    This section documents implementation detail useful for interpreting or querying ingested metadata — not something you need to configure to use the bridge.

Sequence and topic metadata are populated from a mix of auto-computed and user-supplied sources. At the topic level, exactly **one** key is reserved because the bridge writes it itself: `_ros_`, encapsulated by [`RosSchemaMetadata`][mosaicolabs.bridges.ros.RosSchemaMetadata] so that the literal string `"_ros_"` exists in a single place in the codebase rather than being duplicated across adapters, loaders, and the injector. Everything the bridge computes automatically for a topic lives inside this one namespace:

* Written by every adapter's `schema_metadata()` (e.g. [`ROSAdapterBase.schema_metadata`][mosaicolabs.bridges.ros.adapter_base.ROSAdapterBase.schema_metadata]): the original ROS `msgtype`, the raw `msgdef`, and any `enums` extracted from `UPPER_CASE` message constants (the mechanism referenced in [Const/enum data is not part of the message payload](#extending-the-bridge-unmodeled-adapters) above).
* `source_file`: the name of the bag file (`file_path.name`) that first created that topic. Written once per topic, at topic-creation time, regardless of `update_if_exists`.

`_ros_` is fully reserved: `topic_metadata` ([Per-Topic Metadata](#per-topic-metadata-topic_metadata) above) is merged in *first*, then the bridge-computed `_ros_` block is applied on top — so the bridge always wins that key regardless of what `topic_metadata` sets for it. Every other key is fully user-owned; `topic_metadata` can freely use anything except `_ros_`.

* **`metadata`** *(sequence-level)*: only applied at sequence-creation time — the Mosaico server does not support mutating a sequence's metadata after ingestion, so it's simply ignored when `update_if_exists=True` targets an already-existing sequence.
* **`topic_metadata`**: merged underneath the auto-computed `_ros_` block (see above).

!!! note "CLI-only sequence metadata"
    When using the `mosaicolabs.ros_injector` CLI, an additional `rosbag_injection` key (the bag's filename) is automatically merged into `metadata` for traceability. This only happens in the CLI entry point — not when constructing `ROSInjectionConfig` directly in Python.

!!! tip "Querying these keys"
    `_ros_` and its nested fields (e.g. `_ros_.msgtype`, `_ros_.enums.<NAME>`) are ordinary metadata as far as the query engine is concerned — they're queryable through [`QuerySequence`][mosaicolabs.query.builders.QuerySequence] and [`QueryTopic`][mosaicolabs.query.builders.QueryTopic] `with_user_metadata()` exactly like any other metadata field, including [glob patterns for nested keys](../query.md#using-glob-pattern-for-metadata-keys) (e.g. `QueryTopic().with_user_metadata("_ros_.msgtype", eq="sensor_msgs/msg/Imu")`). See the [Query Workflow](../query.md) guide for the full API.

#### Override Adapters

Unlike the Custom Adapters above, which register a *new* mapping for a ROS type that has no default adapter, Override Adapters replace the *default* adapter for one specific topic only, leaving every other topic of that same ROS type on the standard path. This section explains how to implement and register them.

##### Overriding and Extending Adapters
While the ROS Bridge provides a robust set of default adapters for standard message types, real-world robotics often involve proprietary message definitions or non-standard uses of common types.
Through the **`adapter_overrides`** parameter in the `ROSInjectionConfig`, you can explicitly map a specific topic to a chosen adapter. This is particularly useful for types like [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html), where, for example, different LiDAR vendors may encode data in unique ways that require specialized parsing logic.

!!! important "Override adapter usage"
    Use adapter overrides for versatile message types like [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html), where different sensors (LiDAR, Radar, etc.) share the same ROS type but require unique parsing logic. Overrides should be defined and used when a given ROS message type has its own **default adapter** registered in the `ROSBridge` registry, but such an adapter cannot satisfy topic-specific requirements. If your message type is used consistently across all topics, simply use the [`@register_default_adapter`][mosaicolabs.bridges.ros.register_default_adapter] decorator to establish a global fallback.

##### Available Adapters override 

Built-in adapters are provided for the most common sensor types and are ready to use out of the box:

| Sensor type   | Adapter class         |
|---------------|-----------------------|
| LiDAR         | [`LidarAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.LidarAdapter]        |
| Radar         | [`RadarAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.RadarAdapter]        |
| RGBD Camera   | [`RGBDCameraAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.RGBDCameraAdapter]   |
| ToF Camera    | [`ToFCameraAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.ToFCameraAdapter]    |
| Stereo Camera | [`StereoCameraAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.StereoCameraAdapter] |

All of them extend [`PointCloudAdapterBase`][mosaicolabs.bridges.ros.adapters.sensor_msgs.PointCloudAdapterBase], which exposes the following interface:

- **`decode`**: deserializes the binary buffer of a `PointCloud2` message into named field arrays.
- **`_build`** *(abstract)*: constructs and returns an instance of the target ontology object from the decoded fields. Must be overridden in every concrete subclass.
- **`from_dict`**: validates that all required fields of the ontology are present before delegating to `_build`. A field is considered required when its [`MosaicoField`][mosaicolabs.models.core.MosaicoField] declaration has no explicit default (i.e. `default=...`) or it is declared **no Optional**.

##### Implementing a Custom PointCloud2 Adapter Override
To create a custom `PointCloud2` adapter, inherit from [`PointCloudAdapterBase`][mosaicolabs.bridges.ros.adapters.sensor_msgs.PointCloudAdapterBase].
You only need to define:

- `_build`: the mapping logic from decoded field arrays to your ontology instance.
- `_REQUIRED_FIELDS`: the list of fields that must be present in the decoded payload.
- `__mosaico_ontology_type__`: the target ontology class.

All core business logic is encapsulated inside `PointCloudAdapterBase`.

The following example shows a custom LiDAR adapter whose encoding differs from the generic [`LidarAdapter`][mosaicolabs.bridges.ros.adapters.override_msgs.LidarAdapter] already provided by Mosaico. 
Note that the adapter is **not** registered as default, since `sensor_msgs/msg/PointCloud2` already has one.

```python
from typing import Any, Optional, Type
from mosaicolabs import Message
from mosaicolabs.bridges.ros import ROSMessage
from mosaicolabs.bridges.ros.adapters import PointCloudAdapterBase
from my_ontology import MyLidar # Your target Ontology class

class MyLidarAdapter(PointCloudAdapterBase[MyLidar]):
    # Define the target Mosaico Ontology class
    __mosaico_ontology_type__: Type[MyLidar] = MyLidar

    _REQUIRED_FIELDS = [
        name for name, field in MyLidar.model_fields.items() 
        if field.is_required()
    ]

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> MyLidar:
        return MyLidar(...)

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Optional: Override the high-level translation if you need to
        manipulate the ROSMessage envelope before processing.
        """
        # Optionally add pre/post processing logic around the base translation.
        return super().translate(ros_msg, **kwargs)


    @classmethod
    def from_dict(cls, ros_data: dict) -> MyLidar:
        """
        The primary transformation logic.
        Converts the deserialized ROS dictionary into a Mosaico object.
        """
        # Core transformation logic: map raw ROS fields to your ontology type.
        return super().from_dict(ros_data)


    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Optional: Extract specific metadata from the ROS message
        to be stored in the Mosaico schema registry.
        """
        return None
```

!!! tip "Optional overrides"
    Only `_build` is mandatory. Override `translate`, `from_dict`, or `schema_metadata` only when the default behaviour of the base class does not meet your needs.

##### Registering the Override
Once implemented, the adapter is registered against a specific topic via `adapter_overrides` in [ROSInjectionConfig][mosaicolabs.bridges.ros.ROSInjectionConfig]:

```python
from .my_adapter import MyLidarAdapter

...

config = ROSInjectionConfig(
    file_path=Path("sensor_data.mcap"),
    sequence_name="custom_lidar_run",
    # Explicitly tell the bridge to use your custom adapter for this topic
    adapter_overrides={
        "/lidar/front/pointcloud": MyLidarAdapter,
    }
)

...

ingestor = RosbagInjector(config)
ingestor.run()
```

With this configuration, all the [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html) message received on `/lidar/front/pointcloud`, will be processed exclusively by `MyLidarAdapter`. All other topics continue to use the standard resolution logic.

By using this pattern, you can maintain a clean separation between your raw ROS data and your high-level Mosaico data models, ensuring that even the most "exotic" sensor data is correctly ingested and indexed.

##### Implementing a Custom Adapter Override
To create a custom adapter that overrides a existing ROS message, you must inherit from `ROSAdapterBase` and define the transformation logic.
Follow the steps described [here](#extending-the-bridge-custom-adapters) with the only caveat not to register the adapter with `@register_default_adapter` but instead [register the override](#registering-the-override)

#### CLI Usage

The module includes a command-line interface for quick ingestion tasks. The full list of options can be retrieved by running `mosaicolabs.ros_injector -h`

```bash
# Basic Usage
mosaicolabs.ros_injector ./data.mcap --name "Test_Run_01"

# Advanced Usage: Filtering topics and adding metadata
mosaicolabs.ros_injector ./data.db3 \
  --name "Test_Run_01" \
  --topics /camera/front/* /gps/fix \
  --metadata ./metadata.json \
  --ros-distro ros2_humble

# Advanced Usage: Per-topic metadata and appending to an existing sequence
# (e.g. a second bag of a multi-part recording, or reprocessed results
# to merge into an already-ingested sequence)
mosaicolabs.ros_injector ./estimation_results.mcap \
  --name "Test_Run_01" \
  --topic-metadata '{"/estimation/pose": {"algorithm": "ekf_v2"}}' \
  --update-if-exists
```

### The Extractor (`ROSSequenceExtractor`)

The **[`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor]** runs the ingestion pipeline in reverse: it reads a Mosaico sequence back out and writes it as a ROS 1 (`.bag`) or ROS 2 (`.mcap`/`.db3`) bag file, using the same [`ROSAdapterBase.to_ros()`][mosaicolabs.bridges.ros.adapter_base.ROSAdapterBase.to_ros] adapters (and the [`_ros_` metadata][mosaicolabs.bridges.ros.RosSchemaMetadata] recorded at ingestion time, e.g. to recover the original ROS message type) that made the ingestion adaptation possible in the first place.

#### Core Workflow Execution: `run()`

1. **Prepare Output Path**: resolves `rosbag_path / sequence_name` and enforces the `overwrite` policy — raises `FileExistsError` if the path exists and `overwrite=False`, otherwise deletes it first.
2. **Handshake**: connects to the Mosaico server and opens a [`MosaicoLoader`][mosaicolabs.bridges.ros.loader.MosaicoLoader] for the requested sequence (optionally filtered by `topics` and a `start_timestamp_ns`/`end_timestamp_ns` window).
3. **Adaptive Streaming**: for each `(topic, message)` pair, resolves the topic's adapter and original ROS message type, converts the Mosaico message back to a native ROS message via `to_ros()`, and writes it to the bag. Topics with no resolvable adapter, or whose `to_ros()` call fails, are skipped (logged as a warning) rather than aborting the whole extraction.

#### Configuring the Extraction

The behavior of the extractor is entirely driven by **[`ROSExtractorConfig`][mosaicolabs.bridges.ros.ROSExtractorConfig]** — the mirror image of `ROSInjectionConfig` for the reverse direction. Its main knobs:

* **`topics`**: the same glob-based include/exclude filtering as `ROSInjectionConfig.topics` (see [Configuring the Ingestion](#configuring-the-ingestion) above) — applied here against the sequence's topics instead of a bag's.
* **`ros_distro`** / **`storage_plugin`**: select the target ROS distribution and, for ROS 2, the storage backend (`StoragePlugin.MCAP` or `StoragePlugin.SQLITE3`) for the *output* bag. Together with `ros_distro`, this also determines the output bag format: `Stores.ROS1_NOETIC` writes a ROS 1 `.bag`, anything else writes a ROS 2 bag via the selected `storage_plugin`.
* **`start_timestamp_ns`** / **`end_timestamp_ns`**: an optional time window to extract. Out-of-range bounds are *clipped* to the sequence's own bounds (with a warning) rather than raising.
* **`overwrite`**: if the resolved output path (`rosbag_path / sequence_name`) already exists, `overwrite=False` (default) raises `FileExistsError`; `overwrite=True` deletes and recreates it.
* **`custom_msgs`**: register custom `.msg` definitions before extraction — needed when encoding an ontology type back to a ROS message whose `msgdef` isn't recoverable from the topic's own metadata (e.g. the sequence wasn't ingested from a ROS bag in the first place). See [The Type Registry](#the-type-registry-rostyperegistry) below for the full explanation.

#### Dry Run (`dry_run`)

Setting **`dry_run=True`** (or `--dry-run` on the CLI) resolves the sequence's topics and prints a report — per topic, the resolved adapter and target ROS message type (or the rejection reason), plus message counts — **without** opening a bag writer or touching the output path. This is deliberately checked before `_prepare_output_path()` runs, since that step can delete an existing output directory under `overwrite=True`; the dry run reports what *would* happen to that path (created, deleted+recreated, or a `FileExistsError`) without doing it.

```python
config = ROSExtractorConfig(
    rosbag_path=Path("./exports"),
    sequence_name="on_track_experiment",
    dry_run=True,
)
ROSSequenceExtractor(config).run()  # prints a report; nothing is written or deleted
```

```bash
mosaicolabs.ros_sequence_extractor on_track_experiment --rosbag_path ./exports --dry-run
```

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs.bridges.ros import ROSSequenceExtractor, ROSExtractorConfig
from rosbags.typesys import Stores

config = ROSExtractorConfig(
    rosbag_path=Path("./exports"),
    sequence_name="on_track_experiment",

    # Topic Filtering (supports glob patterns, same semantics as ROSInjectionConfig.topics)
    topics=["/cam*", "!/cam/debug*"],

    # Target ROS distribution/format for the output bag
    ros_distro=Stores.ROS2_HUMBLE,

    # Optional time-window clipping (nanoseconds); out-of-range bounds are clipped
    # to the sequence's own bounds rather than raising.
    start_timestamp_ns=None,
    end_timestamp_ns=None,

    overwrite=True,
)

extractor = ROSSequenceExtractor(config)
extractor.run()
```

#### CLI Usage

The full list of options can be retrieved by running `mosaicolabs.ros_sequence_extractor -h`.

```bash
# Basic Usage
mosaicolabs.ros_sequence_extractor on_track_experiment --rosbag_path ./exports

# Advanced Usage: Filtering topics and targeting a specific ROS distro/format
mosaicolabs.ros_sequence_extractor on_track_experiment \
  --rosbag_path ./exports \
  --topics /cam/* !/cam/debug* \
  --ros_distro ROS2_HUMBLE \
  --storage_plugin MCAP \
  --overwrite
```

### The Type Registry (`ROSTypeRegistry`)

The **[`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry]** manages ROS `.msg` schemas that aren't otherwise available from the data itself. It's consulted by **both** [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector] and [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] (via each one's `custom_msgs` config field) when building the `Typestore` they hand to their respective loader — neither `ROSLoader` nor `MosaicoLoader` talks to the registry directly (see [The Loaders](#the-loaders-rosloader-mosaicoloader) above).

* **Version Isolation (Stores)**: ROS messages often vary across distributions (e.g., a "Header" in ROS 1 Noetic is structurally different from ROS 2 Humble). The registry uses a "Profile" system to store these version-specific definitions separately, preventing cross-distribution conflicts.
* **Global vs. Scoped Definitions**: within one registry *instance*, you can register definitions **Globally** (available regardless of the distribution requested) or **Scoped** to a specific one.

You'll need `custom_msgs` in two distinct situations, one per direction:

* **Ingestion**: a bag whose messages don't carry their own schema (this is common for ROS 2 `.db3` bags, and can also happen with proprietary types the standard `rosbags` typestores don't know) can't be deserialized at all without that `.msg` definition being registered first — `ROSLoader` has no schema to fall back on.
* **Extraction**: encoding an ontology value back into a native ROS message (`to_ros()`) needs the target `msgtype` to be present in the typestore. `MosaicoLoader` tries to auto-register it from the topic's own `_ros_.msgdef` (recorded automatically at ingestion time — see [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields)), but that fallback only works if the sequence *was* ingested from a ROS bag in the first place. If the sequence's data came from somewhere else (e.g. written directly via the SDK, with no `_ros_` metadata at all) and its ontology type happens to be one that's `@register_default_adapter`-adapted to/from ROS, extraction has no `msgdef` to fall back on — you must register the `.msg` schema yourself via `custom_msgs` so the typestore has it.

!!! info "Instance-scoped, not global"
    Unlike some registry patterns, `ROSTypeRegistry` is a plain instantiable class — there is no shared global state. `RosbagInjector`/`ROSSequenceExtractor` each construct their own private instance by default, so one run's custom types can never leak into another run's typestore just because they happened to execute in the same process. This is what `custom_msgs` registers into. To deliberately share a set of definitions across many runs (see below), construct one `ROSTypeRegistry()` yourself and pass that same instance via each config's `registry` field.

| Method | Scope | Description |
| --- | --- | --- |
| **`register(...)`** | Single Message | Registers a single custom type on this instance. The source can be a path to a `.msg` file or a raw string containing the definition. |
| **`register_directory(...)`** | Batch Package | Scans a directory for all `.msg` files and registers them under a specific package name (e.g., `my_pkg/msg/Sensor`). |
| **`get_types(...)`** | Internal | Implements a "Cascade" logic: merges Global definitions with distribution-specific overrides for a loader. |
| **`reset()`** | Utility | Clears all definitions on this instance. Primarily used for unit testing to ensure isolation. |

#### Centralized Registration Example

For large projects with hundreds of proprietary types, centralize the registration calls in a single setup function (e.g., `setup_registry.py`) that builds and returns one shared registry, then pass that same instance to every `ROSInjectionConfig`/`ROSExtractorConfig` that should see it:

```python
# setup_registry.py
from pathlib import Path
from mosaicolabs.bridges.ros import ROSTypeRegistry
from rosbags.typesys import Stores

def build_project_registry() -> ROSTypeRegistry:
    registry = ROSTypeRegistry()

    # 1. Register a proprietary message valid for all ROS versions
    registry.register(
        msg_type="common_msgs/msg/SystemHeartbeat",
        source=Path("./definitions/Heartbeat.msg")
    )

    # 2. Batch register an entire package for ROS 2 Humble
    registry.register_directory(
        package_name="robot_v3_msgs",
        dir_path=Path("./definitions/robot_v3/msgs"),
        store=Stores.ROS2_HUMBLE
    )

    return registry
```

```python
# main_injection.py
from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig
from rosbags.typesys import Stores
from pathlib import Path

shared_registry = build_project_registry()

config = ROSInjectionConfig(
    file_path=Path("mission_data.mcap"),
    sequence_name="mission_01",
    metadata={"operator": "Alice"},
    ros_distro=Stores.ROS2_HUMBLE,
    # No need to list the individual (package, path, store) tuples again here —
    # `registry` already has everything `build_project_registry()` registered.
    registry=shared_registry,
)

ingestor = RosbagInjector(config)
ingestor.run()
```

The same `shared_registry` instance can be passed to a `ROSExtractorConfig` too, letting ingestion and extraction reuse the exact same set of custom definitions without re-registering them.


### Testing & Validation

The ROS Bag Injection module has been validated against a variety of standard datasets to ensure compatibility with different ROS distributions, message serialization formats (CDR/ROS 1), and bag container formats (`.bag`, `.mcap`, `.db3`). For evaluating Mosaico capabilities, we recommend the **[NVIDIA NGC Catalog - R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1)**, which has been verified to be fully compatible with the injection pipeline.

Note: the benchmarks below cover **ingestion** (`RosbagInjector`) and **extraction** (`ROSSequenceExtractor`) performances.

#### NVIDIA R2B Dataset 2024 Performances

Benchmarks below were captured on **macOS 26.2**, **Apple M2 Pro (10 cores, 16GB RAM)**. Injection time includes local MCAP/DB3 deserialization via [`ROSLoader`][mosaicolabs.bridges.ros.loader.ROSLoader], semantic translation through the [`ROSBridge`][mosaicolabs.bridges.ros.ROSBridge], and transmission to the Mosaico server. Compression factor depends on the data itself: scalar telemetry compresses well (~70%), while pre-compressed video feeds show minimal gains (~1%) since the data is already dense.

| Sequence Name | Compression Factor | Injection Time | Hardware Architecture | Notes |
| --- | --- | --- | --- | --- |
| **`r2b_galileo2`** | ~70% | ~40 sec | Apple M2 Pro (16GB) | High compression achieved for telemetry data. |
| **`r2b_galileo`** | ~1% | ~30 sec | Apple M2 Pro (16GB) | Low compression due to pre-compressed source images. |
| **`r2b_robotarm`** | ~66% | ~50 sec | Apple M2 Pro (16GB) | High efficiency for high-frequency state updates. |
| **`r2b_whitetunnel`** | ~1% | ~30 sec | Apple M2 Pro (16GB) | Low compression; contains topics with no available adapter. |

Extraction time includes data streaming from mosaico, semantic translation through the [`ROSBridge`][mosaicolabs.bridges.ros.ROSBridge], and serialization into the rosbag.

| Sequence Name |  Extraction Time | Hardware Architecture |
| --- | --- | --- |
| **`r2b_galileo2`** | ~12 sec | Apple M2 Pro (16GB) |
| **`r2b_galileo`** |  ~2 sec | Apple M2 Pro (16GB) |
| **`r2b_robotarm`** | ~18 sec | Apple M2 Pro (16GB) |
| **`r2b_whitetunnel`** | ~2 sec | Apple M2 Pro (16GB) |

#### Known Issues & Limitations

While the underlying `rosbags` library supports the majority of standard ROS 2 bag files, specific datasets with non-standard serialization alignment or proprietary encodings may encounter compatibility issues.

!!! warning "NVIDIA Isaac ROS Benchmark Dataset (2023)"
    The **[R2B Dataset 2023](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2023)** (unlike the 2024 release above) fails to deserialize: the [`AnyReader.deserialize`](https://ternaris.gitlab.io/rosbags/api/rosbags.highlevel.html#rosbags.highlevel.AnyReader.deserialize) method of the [`rosbags`](https://ternaris.gitlab.io/rosbags/index.html) library raises an assertion error (`assert pos + 4 + 3 >= len(rawdata)` in `rosbags.serde.cdr`), indicating a mismatch between the expected data length and the raw payload size. This originates in the upstream parser's handling of this dataset's serialization alignment; exclude it or transcode it with standard ROS 2 tools before ingestion.


## Supported Message Types
  
  ***ROS-Specific Data Models***
  
  In addition to mapping standard ROS messages to the core Mosaico ontology, the `ros-bridge` module implements two specialized data models. These are defined specifically for this module to handle ROS-native concepts that are not yet part of the official Mosaico standard:
  
  * **`FrameTransform`**: Designed to handle coordinate frame transformations (modeled after [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html)). It encapsulates a list of [`Transform`][mosaicolabs.models.data.geometry.Transform] objects to manage spatial relationships.
  * **`BatteryState`**: Modeled after [`sensor_msgs/msg/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)), this class captures comprehensive power supply metrics. It includes core data (voltage, current, capacity, percentage) and detailed metadata such as power supply health, technology status, and individual cell readings.
  * **`PointCloud2`**: Modeled after [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html),
    this class captures raw point cloud data including field layout, endianness, and binary payload.
    It includes the companion `PointField` model to describe each data channel (e.g., `x`, `y`, `z`, `intensity`).
  
  > **Note:** Although these are provisional additions, both `FrameTransform`, `BatteryState`, and `PointCloud2` inherit from [`Serializable`][mosaicolabs.models.core.Serializable]. This ensures they remain fully compatible with Mosaico’s existing serialization infrastructure.

### Supported Message Types Table

  | ROS Message Type | Mosaico Ontology Type | Adapter |
  | :--- | :--- | :--- |
  | [`geometry_msgs/msg/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html), [`PoseStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseStamped.html)... | [`Pose`][mosaicolabs.models.data.geometry.Pose] | `PoseAdapter` |
  | [`geometry_msgs/msg/Twist`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Twist.html), [`TwistStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TwistStamped.html)... | [`Velocity`][mosaicolabs.models.data.kinematics.Velocity] | `TwistAdapter` |
  | [`geometry_msgs/msg/Accel`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Accel.html), [`AccelStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/AccelStamped.html)... | [`Acceleration`][mosaicolabs.models.data.kinematics.Acceleration] | `AccelAdapter` |
  | [`geometry_msgs/msg/Vector3`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3.html), [`Vector3Stamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3Stamped.html) | [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d] | `Vector3Adapter` |
  | [`geometry_msgs/msg/Point`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Point.html), [`PointStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PointStamped.html) | [`Point3d`][mosaicolabs.models.data.geometry.Point3d] | `PointAdapter` |
  | [`geometry_msgs/msg/Quaternion`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Quaternion.html), [`QuaternionStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/QuaternionStamped.html) | [`Quaternion`][mosaicolabs.models.data.geometry.Quaternion] | `QuaternionAdapter` |
  | [`geometry_msgs/msg/Transform`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Transform.html), [`TransformStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TransformStamped.html) | [`Transform`][mosaicolabs.models.data.geometry.Transform] | `TransformAdapter` |
  | [`geometry_msgs/msg/Wrench`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Wrench.html), [`WrenchStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/WrenchStamped.html) | [`ForceTorque`][mosaicolabs.models.data.dynamics.ForceTorque] | `WrenchAdapter` |
  | [`geometry_msgs/msg/Polygon`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Polygon.html), [`PolygonStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PolygonStamped.html) | [`Polygon`][mosaicolabs.models.data.geometry.Polygon] | `PolygonAdapter` |
  | [`geometry_msgs/msg/Inertia`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Inertia.html), [`InertiaStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/InertiaStamped.html) | [`Inertia`][mosaicolabs.models.data.dynamics.Inertia] | `InertiaAdapter` |
  | [`nav_msgs/msg/Odometry`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Odometry.html) | [`MotionState`][mosaicolabs.models.data.kinematics.MotionState] | `OdometryAdapter` |
  | [`nav_msgs/msg/OccupancyGrid`](https://docs.ros2.org/foxy/api/nav_msgs/msg/OccupancyGrid.html) | [`OccupancyGrid`][mosaicolabs.models.futures.OccupancyGrid] (ROS-specific)| `OccupancyGridAdapter` |
  | [`nav_msgs/msg/GridCells`](https://docs.ros2.org/foxy/api/nav_msgs/msg/GridCells.html) | [`GridCells`][mosaicolabs.models.futures.GridCells] (ROS-specific)| `GridCellsAdapter` |
  | [`nav_msgs/msg/MapMetaData`](https://docs.ros2.org/foxy/api/nav_msgs/msg/MapMetaData.html) | [`MapMetadata`][mosaicolabs.models.futures.MapMetadata] (ROS-specific)| `MapMetadataAdapter` |
  | [`nav_msgs/msg/Path`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Path.html) | [`Path`][mosaicolabs.models.data.geometry.RobotPath] (ROS-specific)| `PathAdapter` |
  | [`nmea_msgs/msg/Sentence`](https://docs.ros2.org/foxy/api/nmea_msgs/msg/Sentence.html) | [`NMEASentence`][mosaicolabs.models.sensors.NMEASentence] | `NMEASentenceAdapter` |
  | [`sensor_msgs/msg/Image`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Image.html), [`CompressedImage`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CompressedImage.html) | [`Image`][mosaicolabs.models.sensors.Image], [`CompressedImage`][mosaicolabs.models.sensors.CompressedImage] | `ImageAdapter`, `CompressedImageAdapter` |
  | [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html) | [`IMU`][mosaicolabs.models.sensors.IMU] | `IMUAdapter` |
  | [`sensor_msgs/msg/MagneticField`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/MagneticField.html) | [`Magnetometer`][mosaicolabs.models.sensors.Magnetometer] | `MagneticFieldAdapter` |
  | [`sensor_msgs/msg/Joy`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Joy.html) | [`Joy`][mosaicolabs.models.sensors.Joy] | `JoyAdapter` |
  | [`sensor_msgs/msg/NavSatFix`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatFix.html) | [`GPS`][mosaicolabs.models.sensors.GPS], [`GPSStatus`][mosaicolabs.models.sensors.GPSStatus] | `GPSAdapter`, `NavSatStatusAdapter` |
  | [`sensor_msgs/msg/CameraInfo`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CameraInfo.html) | [`CameraInfo`][mosaicolabs.models.sensors.CameraInfo] | `CameraInfoAdapter` |
  | [`sensor_msgs/msg/RegionOfInterest`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/RegionOfInterest.html) | [`ROI`][mosaicolabs.models.data.ROI] | `ROIAdapter` |
  | [`sensor_msgs/msg/JointState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/JointState.html) | [`RobotJoint`][mosaicolabs.models.sensors.RobotJoint] | `RobotJointAdapter` |
  | [`sensor_msgs/msg/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html) | [`BatteryState`][mosaicolabs.bridges.ros.data_ontology.BatteryState] (ROS-specific)| `BatteryStateAdapter` |
  | [`sensor_msgs/msg/Temperature`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Temperature.html) | [`Temperature`][mosaicolabs.models.sensors.Temperature] | `TemperatureAdapter` |
  | [`sensor_msgs/msg/FluidPressure`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/FluidPressure.html) | [`FluidPressure`][mosaicolabs.models.sensors.Pressure] | `PressureAdapter` |
  | [`sensor_msgs/msg/LaserScan`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/LaserScan.html) | [`LaserScan`][mosaicolabs.models.futures.LaserScan] (ROS-specific)| `LaserScanAdapter` |
  | [`sensor_msgs/msg/MultiEchoLaserScan`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/MultiEchoLaserScan.html) | [`MultiEchoLaserScan`][mosaicolabs.models.futures.MultiEchoLaserScan] (ROS-specific)| `MultiEchoLaserScanAdapter` |
  | [`std_msgs/msg/String`](https://docs.ros2.org/foxy/api/std_msgs/msg/String.html)| [`String`][mosaicolabs.models.data.String]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Int8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int8.html) | [`Integer8(16,32,64)`][mosaicolabs.models.data.Integer8]| `_GenericStdAdapter` |
  | [`std_msgs/msg/UInt8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt8.html) | [`Unsigned8(16,32,64)`][mosaicolabs.models.data.Unsigned8]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Float32(64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Float32.html) | [`Floating32(64)`][mosaicolabs.models.data.Floating32]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Bool`](https://docs.ros2.org/foxy/api/std_msgs/msg/Bool.html) | [`Boolean`][mosaicolabs.models.data.Boolean]| `_GenericStdAdapter` |
  | [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html) | [`FrameTransform`][mosaicolabs.bridges.ros.data_ontology.FrameTransform] (ROS-specific)| `FrameTransformAdapter` |
  | [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html) | [`PointCloud2`][mosaicolabs.bridges.ros.data_ontology.PointCloud2] (ROS-specific)| `PointCloudAdapter` |
