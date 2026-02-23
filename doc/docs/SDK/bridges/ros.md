---
title: ROS
description: ROS-Mosaico Bridge
---


The **ROS Bridge** module serves as the ingestion gateway for ROS (Robot Operating System) data into the Mosaico Data Platform. Its primary function is to solve the interoperability challenges associated with ROS bag files—specifically format fragmentation (ROS 1 `.bag` vs. ROS 2 `.mcap`/`.db3`) and the lack of strict schema enforcement in custom message definitions.

The core philosophy of the module is **"Adaptation, Not Just Parsing."** Rather than simply extracting raw dictionaries from ROS messages, the bridge actively translates them into the standardized **Mosaico Ontology**. For example, a [`geometry_msgs/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html) is validated, normalized, and instantiated as a strongly-typed [`mosaicolabs.models.data.Pose`][mosaicolabs.models.data.Pose] object before ingestion.

## Architecture

The module is composed of four distinct layers that handle the pipeline from raw file access to server transmission.

### The Loader Layer (`ROSLoader`)

The [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader] acts as the abstraction layer over the physical bag files. It utilizes the [`rosbags`](https://pypi.org/project/rosbags/) library to provide a unified interface for reading both ROS 1 and ROS 2 formats (`.bag`, `.db3`, `.mcap`).

  * **Responsibilities:** File I/O, raw deserialization, and topic filtering (supporting glob patterns like `/cam/*`).
  * **Error Handling:** It implements configurable policies (`IGNORE`, `LOG_WARN`, `RAISE`) to handle corrupted messages or deserialization failures without crashing the entire pipeline.

### The Adaptation Layer (`ROSBridge` & Adapters)

This layer represents the semantic core of the module, translating raw ROS data into the Mosaico Ontology.

* **[`ROSAdapterBase`][mosaicolabs.ros_bridge.adapter_base.ROSAdapterBase]:** An abstract base class that establishes the contract for converting specific ROS message types into their corresponding Mosaico Ontology types.
* **Concrete Adapters:** The library provides built-in implementations for common standards, such as [`IMUAdapter`][mosaicolabs.ros_bridge.adapters.sensor_msgs.IMUAdapter] (mapping `sensor_msgs/Imu` to [`IMU`][mosaicolabs.models.sensors.IMU]) and [`ImageAdapter`][mosaicolabs.ros_bridge.adapters.sensor_msgs.ImageAdapter] (mapping `sensor_msgs/Image` to [`Image`][mosaicolabs.models.sensors.Image]). These adapters include advanced logic for recursive unwrapping, automatically extracting data from complex nested wrappers like [`PoseWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseWithCovarianceStamped.html). Developers can also implement custom adapters to handle non-standard or proprietary types.
* **[`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]:** A central registry and dispatch mechanism that maps ROS message type strings (e.g., [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html)) to their corresponding adapter classes, ensuring the correct translation logic is applied for each message.

#### Extending the Bridge (Custom Adapters)

Users can extend the bridge to support new ROS message types by implementing a custom adapter and registering it.

1.  **Inherit from `ROSAdapterBase`**: Define the input ROS type string and the target Mosaico Ontology type.
2.  **Implement `from_dict`**: Define the logic to convert the [`ROSMessage.data`][mosaicolabs.ros_bridge.ROSMessage] dictionary into an intance of the target ontology object.
3.  **Register**: Decorate the class with [`@register_adapter`][mosaicolabs.ros_bridge.register_adapter].

```python
from mosaicolabs.ros_bridge import ROSAdapterBase, register_adapter, ROSMessage
from mosaicolabs.models import Message
from my_ontology import MyCustomData # Assuming this class exists

@register_adapter
class MyCustomAdapter(ROSAdapterBase[MyCustomData]):
    ros_msgtype = "my_pkg/msg/MyCustomType"
    __mosaico_ontology_type__ = MyCustomData

    @classmethod
    def from_dict(cls, ros_data: dict) -> MyCustomData:
        # Transformation logic here
        return MyCustomData(...)
```

### The Orchestrator (`RosbagInjector`)

The **[`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector]** is the central command center of the ROS Bridge module. It is designed to be the primary entry point for developers who want to embed high-performance ROS ingestion directly into their Python applications or automation scripts.

The injector acts as a "glue" layer, orchestrating the interaction between the **[`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader]** (file access), the **[`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]** (data adaptation), and the **[`MosaicoClient`][mosaicolabs.comm.MosaicoClient]** (network transmission). It handles the complex lifecycle of a data upload—including connection management, batching, and transaction safety—while providing real-time feedback through a visual CLI interface.

#### Core Workflow Execution: `run()`

The `run()` method is the heart of the injector. When called, it initiates a multi-phase pipeline:

1. **Handshake & Registry**: Establishes a connection to the Mosaico server and registers any provided custom `.msg` definitions into the global [`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry].
2. **Sequence Creation**: Requests the server to initialize a new data sequence based on the provided name and metadata.
3. **Adaptive Streaming**: Iterates through the ROS bag records. For each message, it identifies the correct adapter, translates the ROS dictionary into a Mosaico object, and pushes it into an optimized asynchronous write buffer.
4. **Transaction Finalization**: Once the bag is exhausted, it flushes all remaining buffers and signals the server to commit the sequence.


#### The Blueprint: `ROSInjectionConfig`

The behavior of the injector is entirely driven by the **[`ROSInjectionConfig`][mosaicolabs.ros_bridge.ROSInjectionConfig]**. This configuration object ensures that the ingestion logic is decoupled from the user interface, allowing for consistent behavior whether triggered via the CLI or a complex script.

| Attribute | Type | Description |
| --- | --- | --- |
| **`file_path`** | `Path` | The location of the source ROS bag (`.mcap`, `.db3`, or `.bag`). |
| **`sequence_name`** | `str` | The unique identifier for the sequence on the server. |
| **`metadata`** | `dict` | Searchable tags and context (e.g., `{"weather": "rainy"}`) attached to the sequence. |
| **`ros_distro`** | [`Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores) | **Crucial for `.db3` bags:** Specifies the ROS distribution (e.g., `ROS2_HUMBLE`) to ensure standard messages are parsed with the correct schema version. |
| **`topics`** | `List[str]` | A filter list supporting glob patterns (e.g., `["/camera/*"]`). If omitted, all supported topics are ingested. |
| **`custom_msgs`** | `List` | A list of tuples `(package, path, store)` used to dynamically register proprietary message definitions at runtime. |
| **`on_error`** | [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy] | **Safety Switch:** Determines if a failed upload should `Delete` the partial sequence or `Report` the error and keep the data. |
| **`log_level`** | `str` | Controls terminal verbosity, ranging from `DEBUG` to `ERROR`. |

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig, Stores

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

#### CLI Usage

The module includes a command-line interface for quick ingestion tasks. The full list of options can be retrieved by running `mosaico.ros_injector -h`

```bash
# Basic Usage
poetry run mosaico.ros_injector ./data.mcap --name "Test_Run_01"

# Advanced Usage: Filtering topics and adding metadata
poetry run mosaico.ros_injector ./data.db3 \
  --name "Test_Run_01" \
  --topics /camera/front/* /gps/fix \
  --metadata ./metadata.json \
  --ros-distro ros2_humble
```

### The Type Registry (`ROSTypeRegistry`)

The **[`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry]** is a context-aware singleton designed to manage the schemas required to decode ROS data. ROS message definitions are frequently external to the data files themselves—this is especially true for ROS 2 `.db3` (SQLite) formats and proprietary datasets containing custom sensors. Without these definitions, the bridge cannot deserialize the raw binary "blobs" into readable dictionaries.

* **Schema Resolution**: It allows the [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader] to resolve custom `.msg` definitions on-the-fly during bag playback.
* **Version Isolation (Stores)**: ROS messages often vary across distributions (e.g., a "Header" in ROS 1 Noetic is structurally different from ROS 2 Humble). The registry uses a "Profile" system to store these version-specific definitions separately, preventing cross-distribution conflicts.
* **Global vs. Scoped Definitions**: You can register definitions **Globally** (available to all loaders) or **Scoped** to a specific distribution.

#### Pre-loading Definitions

While you can pass custom messages via [`ROSInjectionConfig`][mosaicolabs.ros_bridge.ROSInjectionConfig], it can become cumbersome for large-scale projects with hundreds of proprietary types. The recommended approach is to pre-load the registry at the start of your application. This makes the definitions available to all subsequent loaders automatically.

| Method | Scope | Description |
| --- | --- | --- |
| **`register(...)`** | Single Message | Registers a single custom type. The source can be a path to a `.msg` file or a raw string containing the definition. |
| **`register_directory(...)`** | Batch Package | Scans a directory for all `.msg` files and registers them under a specific package name (e.g., `my_pkg/msg/Sensor`). |
| **`get_types(...)`** | Internal | Implements a "Cascade" logic: merges Global definitions with distribution-specific overrides for a loader. |
| **`reset()`** | Utility | Clears all stored definitions. Primarily used for unit testing to ensure process isolation. |

#### Centralized Registration Example

A clean way to manage large projects is to centralize your message registration in a single setup function (e.g., `setup_registry.py`):

```python
from pathlib import Path
from mosaicolabs.ros_bridge import ROSTypeRegistry, Stores

def initialize_project_schemas():
    # 1. Register a proprietary message valid for all ROS versions
    ROSTypeRegistry.register(
        msg_type="common_msgs/msg/SystemHeartbeat",
        source=Path("./definitions/Heartbeat.msg")
    )

    # 2. Batch register an entire package for ROS 2 Humble
    ROSTypeRegistry.register_directory(
        package_name="robot_v3_msgs",
        dir_path=Path("./definitions/robot_v3/msgs"),
        store=Stores.ROS2_HUMBLE
    )

```

Once registered, the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] (and the underlying [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader]) automatically detects and uses these definitions. There is no longer the need to pass the `custom_msgs` list in the [`ROSInjectionConfig`][mosaicolabs.ros_bridge.ROSInjectionConfig].

```python
# main_injection.py
import setup_registry  # Runs the registration logic above
from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig, Stores
from pathlib import Path

# Initialize registry
setup_registry.initialize_project_schemas()

# Configure injection WITHOUT listing custom messages again
config = ROSInjectionConfig(
    file_path=Path("mission_data.mcap"),
    sequence_name="mission_01",
    metadata={"operator": "Alice"},
    ros_distro=Stores.ROS2_HUMBLE,  # Loader will pull the Humble-specific types we registered
    # custom_msgs=[]  <-- No longer needed!
)

injector = RosbagInjector(config)
injector.run()
```


### Testing & Validation

The ROS Bag Injection module has been validated against a variety of standard datasets to ensure compatibility with different ROS distributions, message serialization formats (CDR/ROS 1), and bag container formats (`.bag`, `.mcap`, `.db3`).

#### Recommended Dataset for Verification

For evaluating Mosaico capabilities, we recommend the **[NVIDIA NGC Catalog - R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1)**. This dataset has been verified to be fully compatible with the injection pipeline.

The following table details the injection performance for the **NVIDIA R2B Dataset 2024**. These benchmarks were captured on a system running **macOS 26.2** with an **Apple M2 Pro (10 cores, 16GB RAM)**.

#### NVIDIA R2B Dataset 2024 Injection Performance

| Sequence Name | Compression Factor | Injection Time | Hardware Architecture | Notes |
| --- | --- | --- | --- | --- |
| **`r2b_galileo2`** | ~70% | ~40 sec | Apple M2 Pro (16GB) | High compression achieved for telemetry data. |
| **`r2b_galileo`** | ~1% | ~30 sec | Apple M2 Pro (16GB) | Low compression due to pre-compressed source images. |
| **`r2b_robotarm`** | ~66% | ~50 sec | Apple M2 Pro (16GB) | High efficiency for high-frequency state updates. |
| **`r2b_whitetunnel`** | ~1% | ~30 sec | Apple M2 Pro (16GB) | Low compression; contains topics with no available adapter. |

#### Understanding Performance Factors

* **Compression Factors**: Sequences like `r2b_galileo2` achieve high ratios (~70%) because Mosaico optimizes the underlying columnar storage for scalar telemetry. Conversely, sequences with pre-compressed video feeds show minimal gains (~1%) because the data is already in a dense format.
* **Injection Time**: This metric includes the overhead of local MCAP/DB3 deserialization via [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader], semantic translation through the [`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge], and the asynchronous transmission to the Mosaico server.
* **Hardware Impact**: On the **Apple M2 Pro**, the [`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector] utilizes multi-threading for the **Adaptation Layer**, allowing serialization tasks to run in parallel while the main thread manages the Flight stream.

#### Known Issues & Limitations

While the underlying `rosbags` library supports the majority of standard ROS 2 bag files, specific datasets with non-standard serialization alignment or proprietary encodings may encounter compatibility issues.

**NVIDIA Isaac ROS Benchmark Dataset (2023)**

  * **Source:** [NVIDIA NGC Catalog - R2B Dataset 2023](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2023)
  * **Issue:** Deserialization failure during ingestion.
  * **Technical Details:** The ingestion process fails within the [`AnyReader.deserialize`](https://ternaris.gitlab.io/rosbags/api/rosbags.highlevel.html#rosbags.highlevel.AnyReader.deserialize) method of the [`rosbags`](https://ternaris.gitlab.io/rosbags/index.html) library. The internal CDR deserializer triggers an assertion error indicating a mismatch in the expected data length vs. the raw payload size.
  * **Error Signature:**
    ```python
    # In rosbags.serde.cdr:
    assert pos + 4 + 3 >= len(rawdata)
    ```
  * **Recommendation:** This issue originates in the upstream parser handling of this specific dataset's serialization alignment. It is currently recommended to exclude this dataset or transcode it using standard ROS 2 tools before ingestion.


## Supported Message Types

***ROS-Specific Data Models***

In addition to mapping standard ROS messages to the core Mosaico ontology, the `ros-bridge` module implements two specialized data models. These are defined specifically for this module to handle ROS-native concepts that are not yet part of the official Mosaico standard:

* **`FrameTransform`**: Designed to handle coordinate frame transformations (modeled after [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html)). It encapsulates a list of [`Transform`][mosaicolabs.models.data.geometry.Transform] objects to manage spatial relationships.
* **`BatteryState`**: Modeled after [`sensor_msgs/msg/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)), this class captures comprehensive power supply metrics. It includes core data (voltage, current, capacity, percentage) and detailed metadata such as power supply health, technology status, and individual cell readings.

> **Note:** Although these are provisional additions, both `FrameTransform` and `BatteryState` inherit from [`Serializable`][mosaicolabs.models.Serializable] and [`HeaderMixin`][mosaicolabs.models.HeaderMixin]. This ensures they remain fully compatible with Mosaico’s existing serialization and header management infrastructure.

### Supported Message Types Table

| ROS Message Type | Mosaico Ontology Type | Adapter |
| :--- | :--- | :--- |
| [`geometry_msgs/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html), [`PoseStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseStamped.html)... | [`Pose`][mosaicolabs.models.data.geometry.Pose] | `PoseAdapter` |
| [`geometry_msgs/Twist`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Twist.html), [`TwistStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TwistStamped.html)... | [`Velocity`][mosaicolabs.models.data.kinematics.Velocity] | `TwistAdapter` |
| [`geometry_msgs/Accel`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Accel.html), [`AccelStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/AccelStamped.html)... | [`Acceleration`][mosaicolabs.models.data.kinematics.Acceleration] | `AccelAdapter` |
| [`geometry_msgs/Vector3`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3.html), [`Vector3Stamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3Stamped.html) | [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d] | `Vector3Adapter` |
| [`geometry_msgs/Point`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Point.html), [`PointStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PointStamped.html) | [`Point3d`][mosaicolabs.models.data.geometry.Point3d] | `PointAdapter` |
| [`geometry_msgs/Quaternion`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Quaternion.html), [`QuaternionStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/QuaternionStamped.html) | [`Quaternion`][mosaicolabs.models.data.geometry.Quaternion] | `QuaternionAdapter` |
| [`geometry_msgs/Transform`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Transform.html), [`TransformStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TransformStamped.html) | [`Transform`][mosaicolabs.models.data.geometry.Transform] | `TransformAdapter` |
| [`geometry_msgs/Wrench`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Wrench.html), [`WrenchStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/WrenchStamped.html) | [`ForceTorque`][mosaicolabs.models.data.dynamics.ForceTorque] | `WrenchAdapter` |
| [`nav_msgs/Odometry`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Odometry.html) | [`MotionState`][mosaicolabs.models.data.kinematics.MotionState] | `OdometryAdapter` |
| [`nmea_msgs/Sentence`](https://docs.ros2.org/foxy/api/nmea_msgs/msg/Sentence.html) | [`NMEASentence`][mosaicolabs.models.sensors.NMEASentence] | `NMEASentenceAdapter` |
| [`sensor_msgs/Image`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Image.html), [`CompressedImage`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CompressedImage.html) | [`Image`][mosaicolabs.models.sensors.Image], [`CompressedImage`][mosaicolabs.models.sensors.CompressedImage] | `ImageAdapter`, `CompressedImageAdapter` |
| [`sensor_msgs/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html) | [`IMU`][mosaicolabs.models.sensors.IMU] | `IMUAdapter` |
| [`sensor_msgs/NavSatFix`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatFix.html) | [`GPS`][mosaicolabs.models.sensors.GPS], [`GPSStatus`][mosaicolabs.models.sensors.GPSStatus] | `GPSAdapter`, `NavSatStatusAdapter` |
| [`sensor_msgs/CameraInfo`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CameraInfo.html) | [`CameraInfo`][mosaicolabs.models.sensors.CameraInfo] | `CameraInfoAdapter` |
| [`sensor_msgs/RegionOfInterest`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/RegionOfInterest.html) | [`ROI`][mosaicolabs.models.data.ROI] | `ROIAdapter` |
| [`sensor_msgs/JointState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/JointState.html) | [`RobotJoint`][mosaicolabs.models.sensors.RobotJoint] | `RobotJointAdapter` |
| [`sensor_msgs/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html) | [`BatteryState`][mosaicolabs.ros_bridge.data_ontology.BatteryState] (ROS-specific)| `BatteryStateAdapter` |
| [`std_msgs/msg/String`](https://docs.ros2.org/foxy/api/std_msgs/msg/String.html)| [`String`][mosaicolabs.models.data.String]| `_GenericStdAdapter` |
| [`std_msgs/msg/Int8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int8.html) | [`Integer8(16,32,64)`][mosaicolabs.models.data.Integer8]| `_GenericStdAdapter` |
| [`std_msgs/msg/UInt8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt8.html) | [`Unsigned8(16,32,64)`][mosaicolabs.models.data.Unsigned8]| `_GenericStdAdapter` |
| [`std_msgs/msg/Float32(64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Float32.html) | [`Floating32(64)`][mosaicolabs.models.data.Floating32]| `_GenericStdAdapter` |
| [`std_msgs/msg/Bool`](https://docs.ros2.org/foxy/api/std_msgs/msg/Bool.html) | [`Boolean`][mosaicolabs.models.data.Boolean]| `_GenericStdAdapter` |
| [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html) | [`FrameTransform`][mosaicolabs.ros_bridge.data_ontology.FrameTransform] (ROS-specific)| `FrameTransformAdapter` |
