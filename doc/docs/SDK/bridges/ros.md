---
title: ROS
description: ROS-Mosaico Bridge
---

The **ROS Bridge** module serves as the ingestion gateway for ROS (Robot Operating System) data into the Mosaico Data Platform. Its primary function is to solve the interoperability challenges associated with ROS bag files—specifically format fragmentation (ROS 1 `.bag` vs. ROS 2 `.mcap`/`.db3`) and the lack of strict schema enforcement in custom message definitions.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the ROS Ingestion employs the mosaico [Writing Workflow](../handling/writing.md), which is allowed only if the key has at least [`APIKeyPermissionEnum.Write`][mosaicolabs.enum.APIKeyPermissionEnum.Write] permission.


The core philosophy of the module is **"Adaptation, Not Just Parsing."** Rather than simply extracting raw dictionaries from ROS messages, the bridge actively translates them into the standardized **Mosaico Ontology**. For example, a [`geometry_msgs/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html) is validated, normalized, and instantiated as a strongly-typed [`mosaicolabs.models.data.Pose`][mosaicolabs.models.data.Pose] object before ingestion.

!!! example "Try-It Out"
    You can experiment yourself the ROS Bridge ingestion via the **[ROS Ingestion](../examples/ros_injection.md) Example**.

## Architecture

The module is composed of four distinct layers that handle the pipeline from raw file access to server transmission.

### The Loader (`ROSLoader`)

The [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader] acts as the abstraction layer over the physical bag files. It utilizes the [`rosbags`](https://pypi.org/project/rosbags/) library to provide a unified interface for reading both ROS 1 and ROS 2 formats (`.bag`, `.db3`, `.mcap`).

  * **Responsibilities:** File I/O, raw deserialization, and topic filtering (supporting glob patterns like `/cam/*`).
  * **Error Handling:** It implements configurable policies (`IGNORE`, `LOG_WARN`, `RAISE`) to handle corrupted messages or deserialization failures without crashing the entire pipeline.

### The Orchestrator (`RosbagInjector`)

The **[`RosbagInjector`][mosaicolabs.ros_bridge.RosbagInjector]** is the central command center of the ROS Bridge module. It is designed to be the primary entry point for developers who want to embed high-performance ROS ingestion directly into their Python applications or automation scripts.

The ingestor orchestrates the interaction between the **[`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader]** (file access), the **[`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]** (data adaptation), and the **[`MosaicoClient`][mosaicolabs.comm.MosaicoClient]** (network transmission). It handles the complex lifecycle of a data upload—including connection management, batching, and transaction safety—while providing real-time feedback through a visual CLI interface.

#### Core Workflow Execution: `run()`

The `run()` method is the heart of the ingestor. When called, it initiates a multi-phase pipeline:

1. **Handshake & Registry**: Establishes a connection to the Mosaico server and registers any provided custom `.msg` definitions into the global [`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry].
2. **Sequence Creation**: Requests the server to initialize a new data sequence based on the provided name and metadata.
3. **Adaptive Streaming**: Iterates through the ROS bag records. For each message, it identifies the correct adapter, translates the ROS dictionary into a Mosaico object, and pushes it into an optimized write buffer.
4. **Transaction Finalization**: Once the bag is exhausted, it flushes all remaining buffers and signals the server to commit the sequence.

#### Configuring the Ingestion

The behavior of the ingestor is entirely driven by the **[`ROSInjectionConfig`][mosaicolabs.ros_bridge.ROSInjectionConfig]**. This configuration object ensures that the ingestion logic is decoupled from the user interface, allowing for consistent behavior whether triggered via the CLI or a complex script.

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs import SessionLevelErrorPolicy, TopicLevelErrorPolicy
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
        
        # Adapter Overrides
        # Use specific adapters for designated topics instead of the default.
        # In this case, instead to use PointCloudAdapter for depth camera,
        # MyCustomRGBDAdapter will be used for the specified topic.
        adapter_override={
            "/camera/depth/points": MyCustomRGBDAdapter,
        },

        # Execution Settings
        log_level="WARNING",  # Reduce verbosity for automated scripts

        # Session Level Error Handling
        on_error=SessionLevelErrorPolicy.Report # Report the error and terminate the session

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

* **[`ROSAdapterBase`][mosaicolabs.ros_bridge.adapter_base.ROSAdapterBase]:** An abstract base class that establishes the **default** contracts for converting specific ROS message types into their corresponding Mosaico Ontology types.
* **Concrete Adapters:** The library provides built-in implementations for common standards, such as [`IMUAdapter`][mosaicolabs.ros_bridge.adapters.sensor_msgs.IMUAdapter] (mapping `sensor_msgs/Imu` to [`IMU`][mosaicolabs.models.sensors.IMU]) and [`ImageAdapter`][mosaicolabs.ros_bridge.adapters.sensor_msgs.ImageAdapter] (mapping `sensor_msgs/Image` to [`Image`][mosaicolabs.models.sensors.Image]). These adapters include advanced logic for recursive unwrapping, automatically extracting data from complex nested wrappers like [`PoseWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseWithCovarianceStamped.html). Developers can also implement custom adapters to handle non-standard or proprietary types.
* **[`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]:** A central registry and dispatch mechanism that maps ROS message type strings (e.g., [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html)) to their corresponding default adapter classes, ensuring the correct translation logic is applied for each message.

#### Extending the Bridge (Custom Adapters)

Users can extend the bridge to support new ROS message types by implementing a custom adapter and registering it.

1.  **Inherit from `ROSAdapterBase`**: Define the input ROS type string and the target Mosaico Ontology type.
2.  **Implement `from_dict`**: Define the logic to convert the [`ROSMessage.data`][mosaicolabs.ros_bridge.ROSMessage] dictionary into an intance of the target ontology object.
3.  **Register**: Decorate the class with [`@register_default_adapter`][mosaicolabs.ros_bridge.register_default_adapter].

```python
from mosaicolabs.ros_bridge import ROSAdapterBase, register_default_adapter, ROSMessage
from mosaicolabs.models import Message
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

#### Override Adapters
This section explains how to extend the bridge's capabilities by implementing and registering Override Adapters.

##### Overriding and Extending Adapters
While the ROS Bridge provides a robust set of default adapters for standard message types, real-world robotics often involve proprietary message definitions or non-standard uses of common types.
Through the **`adapter_override`** parameter in the `ROSInjectionConfig`, you can explicitly map a specific topic to a chosen adapter. This is particularly useful for types like [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html), where, for example, different LiDAR vendors may encode data in unique ways that require specialized parsing logic.

!!! important "Override adapter usage"
    Use adapter overrides for versatile message types like [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html), where different sensors (LiDAR, Radar, etc.) share the same ROS type but require unique parsing logic. Overrides should be defined and used when a given ROS message type has its own **default adapter** registered in the `ROSBridge` registry, but such an adapter cannot satisfy topic-specific requirements. If your message type is used consistently across all topics, simply use the [`@register_default_adapter`][mosaicolabs.ros_bridge.register_default_adapter] decorator to establish a global fallback.

##### Implementing a Custom Adapter Override
To create a custom adapter, you must inherit from `ROSAdapterBase` and define the transformation logic.
Here is a structural example of a custom LiDAR adapter; note that the adapter is not registered as **default**, since the message type `sensor_msgs/msg/PointCloud2` already has it:

```python
from typing import Any, Optional, Type, Tuple
from mosaicolabs.ros_bridge import ROSAdapterBase, ROSMessage
from mosaicolabs.models import Message
from my_ontology import MyLidar # Your target Ontology class

class MyCustomLidarAdapter(ROSAdapterBase[MyLidar]):
    # Define which ROS type this adapter handles
    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/PointCloud2"

    # Define the target Mosaico Ontology class
    __mosaico_ontology_type__: Type[MyLidar] = MyLidar

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
        return MyLidar(
            # ... map ros_data fields to MyLidar fields
        )


    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Optional: Extract specific metadata from the ROS message
        to be stored in the Mosaico schema registry.
        """
        return None
```

##### Key Methods
**`from_dict(ros_data)`**: This is the most important method. It receives the ROS message as a Python dictionary and must return an instance of your target Mosaico Ontology class.

**`translate(ros_msg)`**: This is the entry point called by the ROSBridge. It just calls `from_dict`, but you can override it if you need access to the message metadata during conversion.

**`schema_metadata(ros_data)`**: Use this to extract metadata information about the sensor configuration that is useful for the platform to know.

##### Registering the Override
Once implemented, the adapter is registered against a specific topic via `adapter_override` in [ROSInjectionConfig][mosaicolabs.ros_bridge.ROSInjectionConfig]:

```python
from .my_adapter import MyCustomLidarAdapter

...

config = ROSInjectionConfig(
    file_path=Path("sensor_data.mcap"),
    sequence_name="custom_lidar_run",
    # Explicitly tell the bridge to use your custom adapter for this topic
    adapter_override={
        "/lidar/front/pointcloud": MyCustomLidarAdapter,
    }
)

...

ingestor = RosbagInjector(config)
ingestor.run()
```

With this configuration, all the [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html) message received on `/lidar/front/pointcloud`, will be processed exclusively by `MyCustomLidarAdapter`. All other topics continue to use the standard resolution logic.

By using this pattern, you can maintain a clean separation between your raw ROS data and your high-level Mosaico data models, ensuring that even the most "exotic" sensor data is correctly ingested and indexed.

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

ingestor = RosbagInjector(config)
ingestor.run()
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
* **Injection Time**: This metric includes the overhead of local MCAP/DB3 deserialization via [`ROSLoader`][mosaicolabs.ros_bridge.loader.ROSLoader], semantic translation through the [`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge], and the transmission to the Mosaico server.

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
  * **`PointCloud2`**: Modeled after [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html),
    this class captures raw point cloud data including field layout, endianness, and binary payload.
    It includes the companion `PointField` model to describe each data channel (e.g., `x`, `y`, `z`, `intensity`).
  
  > **Note:** Although these are provisional additions, both `FrameTransform`, `BatteryState`, and `PointCloud2` inherit from [`Serializable`][mosaicolabs.models.Serializable]. This ensures they remain fully compatible with Mosaico’s existing serialization infrastructure.

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
  | [`nav_msgs/msg/Odometry`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Odometry.html) | [`MotionState`][mosaicolabs.models.data.kinematics.MotionState] | `OdometryAdapter` |
  | [`nmea_msgs/msg/Sentence`](https://docs.ros2.org/foxy/api/nmea_msgs/msg/Sentence.html) | [`NMEASentence`][mosaicolabs.models.sensors.NMEASentence] | `NMEASentenceAdapter` |
  | [`sensor_msgs/msg/Image`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Image.html), [`CompressedImage`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CompressedImage.html) | [`Image`][mosaicolabs.models.sensors.Image], [`CompressedImage`][mosaicolabs.models.sensors.CompressedImage] | `ImageAdapter`, `CompressedImageAdapter` |
  | [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html) | [`IMU`][mosaicolabs.models.sensors.IMU] | `IMUAdapter` |
  | [`sensor_msgs/msg/NavSatFix`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatFix.html) | [`GPS`][mosaicolabs.models.sensors.GPS], [`GPSStatus`][mosaicolabs.models.sensors.GPSStatus] | `GPSAdapter`, `NavSatStatusAdapter` |
  | [`sensor_msgs/msg/CameraInfo`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CameraInfo.html) | [`CameraInfo`][mosaicolabs.models.sensors.CameraInfo] | `CameraInfoAdapter` |
  | [`sensor_msgs/msg/RegionOfInterest`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/RegionOfInterest.html) | [`ROI`][mosaicolabs.models.data.ROI] | `ROIAdapter` |
  | [`sensor_msgs/msg/JointState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/JointState.html) | [`RobotJoint`][mosaicolabs.models.sensors.RobotJoint] | `RobotJointAdapter` |
  | [`sensor_msgs/msg/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html) | [`BatteryState`][mosaicolabs.ros_bridge.data_ontology.BatteryState] (ROS-specific)| `BatteryStateAdapter` |
  | [`std_msgs/msg/String`](https://docs.ros2.org/foxy/api/std_msgs/msg/String.html)| [`String`][mosaicolabs.models.data.String]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Int8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int8.html) | [`Integer8(16,32,64)`][mosaicolabs.models.data.Integer8]| `_GenericStdAdapter` |
  | [`std_msgs/msg/UInt8(16,32,64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt8.html) | [`Unsigned8(16,32,64)`][mosaicolabs.models.data.Unsigned8]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Float32(64)`](https://docs.ros2.org/foxy/api/std_msgs/msg/Float32.html) | [`Floating32(64)`][mosaicolabs.models.data.Floating32]| `_GenericStdAdapter` |
  | [`std_msgs/msg/Bool`](https://docs.ros2.org/foxy/api/std_msgs/msg/Bool.html) | [`Boolean`][mosaicolabs.models.data.Boolean]| `_GenericStdAdapter` |
  | [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html) | [`FrameTransform`][mosaicolabs.ros_bridge.data_ontology.FrameTransform] (ROS-specific)| `FrameTransformAdapter` |
  | [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html) | [`PointCloud2`][mosaicolabs.ros_bridge.data_ontology.PointCloud2] (ROS-specific)| `PointCloudAdapter` |
