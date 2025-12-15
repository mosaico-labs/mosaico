<div align="right">
<picture>
<source media="(prefers-color-scheme: light)" srcset="../../logo/mono_black.svg">
<source media="(prefers-color-scheme: dark)" srcset="../../logo/mono_white.svg">
<img alt="Mosaico logo" src="../../logo/color_white.svg" height="30">
</picture>
</div>

# ROS Bridge Module Documentation

## Overview & Philosophy

The **ROS Bridge** module serves as the ingestion gateway for ROS (Robot Operating System) data into the Mosaico Data Platform. Its primary function is to solve the interoperability challenges associated with ROS bag files—specifically format fragmentation (ROS 1 `.bag` vs. ROS 2 `.mcap`/`.db3`) and the lack of strict schema enforcement in custom message definitions.

The core philosophy of the module is **"Adaptation, Not Just Parsing."** Rather than simply extracting raw dictionaries from ROS messages, the bridge actively translates them into the standardized **Mosaico Ontology**. For example, a `geometry_msgs/Pose` is validated, normalized, and instantiated as a strongly-typed `mosaicolabs.models.data.Pose` object before ingestion.

## Architecture

The module is composed of four distinct layers that handle the pipeline from raw file access to server transmission.

### The Loader Layer (`ROSLoader`)

The `ROSLoader` acts as the abstraction layer over the physical bag files. It utilizes the [`rosbags`](https://pypi.org/project/rosbags/) library to provide a unified interface for reading both ROS 1 and ROS 2 formats (`.bag`, `.db3`, `.mcap`).

  * **Responsibilities:** File I/O, raw deserialization, and topic filtering (supporting glob patterns like `/cam/*`).
  * **Error Handling:** It implements configurable policies (`IGNORE`, `LOG_WARN`, `RAISE`) to handle corrupted messages or deserialization failures without crashing the entire pipeline.

### The Adaptation Layer (`ROSBridge` & Adapters)

This layer represents the semantic core of the module, translating raw ROS data into the Mosaico Ontology.

* **`ROSAdapterBase`:** An abstract base class that establishes the contract for converting specific ROS message types into their corresponding Mosaico Ontology types.
* **Concrete Adapters:** The library provides built-in implementations for common standards, such as `IMUAdapter` (mapping `sensor_msgs/Imu` to `IMU`) and `ImageAdapter` (mapping `sensor_msgs/Image` to `Image`). These adapters include advanced logic for recursive unwrapping, automatically extracting data from complex nested wrappers like `PoseWithCovarianceStamped`. Developers can also implement custom adapters to handle non-standard or proprietary types.
* **`ROSBridge`:** A central registry and dispatch mechanism that maps ROS message type strings (e.g., `sensor_msgs/msg/Imu`) to their corresponding adapter classes, ensuring the correct translation logic is applied for each message.

#### Extending the Bridge (Custom Adapters)

Users can extend the bridge to support new ROS message types by implementing a custom adapter and registering it.

1.  **Inherit from `ROSAdapterBase`**: Define the input ROS type string and the target Mosaico Ontology type.
2.  **Implement `translate`**: Define the logic to convert the `ROSMessage` dictionary into a `Message` object containing the ontology data.
3.  **Register**: Decorate the class with `@register_adapter`.

```python
from mosaicolabs.ros_bridge import ROSAdapterBase, register_adapter, ROSMessage
from mosaicolabs.models import Message
from my_ontology import MyCustomData # Assuming this class exists

@register_adapter
class MyCustomAdapter(ROSAdapterBase[MyCustomData]):
    ros_msgtype = "my_pkg/msg/MyCustomType"
    __mosaico_ontology_type__ = MyCustomData

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs) -> Message:
        # Transformation logic here
        return Message(
            timestamp_ns=ros_msg.timestamp,
            data=MyCustomData(...)
        )
```

### The Orchestrator (`RosbagInjector`)

The `RosbagInjector` is the primary entry point for embedding ROS ingestion into Python applications and coordinates the entire workflow. It connects the `ROSLoader` to the `MosaicoClient`, managing the flow of data, batching, error reporting, and progress visualization via a CLI interface.

```python
class RosbagInjector:
    def __init__(self, config: ROSInjectionConfig)
    def run(self)
```

  * **`run()`**: Executes the injection pipeline: connects to the server, opens the bag, iterates through messages, adapts them, and transmits them to the Mosaico platform.

#### `ROSInjectionConfig`

A data class defining the configuration for the injection process.

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `file_path` | `Path` | Path to the input bag file (`.mcap`, `.db3`, `.bag`). |
| `sequence_name` | `str` | The name of the target sequence to create on the server. |
| `metadata` | `dict` | User-defined metadata to attach to the sequence (e.g., `{"driver": "TestDriver"}`). |
| `ros_distro` | `Stores` | (Optional) The target ROS distribution (e.g., `Stores.ROS2_HUMBLE`). Defaults to `EMPTY`. |
| `topics` | `List[str]` | (Optional) A list of topics to filter. Supports glob patterns (e.g., `["/cam/*"]`). |
| `custom_msgs` | `List` | A list of tuples `(package, path, store)` to register custom `.msg` definitions. |
| `on_error` | `OnErrorPolicy` | Behavior when sequence writing fails (`Delete` sequence or `Report` error). |

#### CLI Usage

The module includes a command-line interface for quick ingestion tasks.

```bash
# Basic Usage
mosaico.ros_injector ./data.mcap --name "Test_Run_01"

# Advanced Usage: Filtering topics and adding metadata
mosaico.ros_injector ./data.db3 \
  --name "Test_Run_01" \
  --topics /camera/front/* /gps/fix \
  --metadata ./metadata.json \
  --ros-distro ros2_humble
```

The full list of options can be retrieved by running `mosaico.ros_injector -h`:

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

For code-first workflows, such as integrating with CI/CD or custom automation scripts, it is possible to use the `RosbagInjector` class directly within Python code.

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

### The Type Registry (`ROSTypeRegistry`)

ROS message definitions are not always self-contained within bag files (particularly in the ROS 2 `.db3` format), and datasets often contain proprietary data types that are not part of the standard message libraries. These definitions rely on external schema files (`.msg`). The `ROSTypeRegistry` acts as a context-aware singleton to manage these dependencies.

* **Custom Messages:** It facilitates the registration of local `.msg` files, allowing the loader to parse proprietary or non-standard message types effectively.
* **Versioning:** It implements a profile system (Stores) to resolve version conflicts, managing cases where message definitions differ between distributions (e.g., `ROS1_NOETIC` vs. `ROS2_HUMBLE`).

For complex projects with many custom message definitions, passing them repeatedly via the `custom_msgs` argument of the `ROSInjectionConfig` class can become unwieldy. Instead, you can use the **`ROSTypeRegistry`** to pre-load definitions at the start of your application.

  * **`register_directory(package_name, dir_path, store=None)`**: Batch registers all `.msg` files found in a directory under a specific package name. The registry will automatically infer type names as `{package_name}/msg/{filename}`
  * **`register(msg_type, source, store=None)`**: Registers a single message type definition.

#### Centralized Custom Message Registration

For example, create a setup script (e.g., `setup_registry.py`) that runs before your injection logic.

```python
from pathlib import Path
from mosaicolabs.ros_bridge import ROSTypeRegistry, Stores

def register_project_messages():
    """
    Centralized registration of all custom ROS message definitions used in the project.
    """
    
    # Register a Single Message Globally (Applies to all ROS versions)
    # Useful for simple, stable message definitions.
    ROSTypeRegistry.register(
        msg_type="my_robot_msgs/msg/BatteryStatus",
        source=Path("./definitions/common/BatteryStatus.msg")
    )

    # Register for a Specific Distro (e.g., ROS 2 Humble)
    # Essential if the definition changed between versions or relies on specific headers.
    ROSTypeRegistry.register(
        msg_type="my_robot_msgs/msg/NavigationState",
        source=Path("./definitions/humble/NavigationState.msg"),
        store=Stores.ROS2_HUMBLE
    )

    # Batch Register an Entire Directory
    # Automatically infers type names: "package_name/msg/{filename}"
    # Example: ./definitions/lidar/Scan.msg -> "velodyne_msgs/msg/Scan"
    ROSTypeRegistry.register_directory(
        package_name="velodyne_msgs",
        dir_path=Path("./definitions/lidar"),
        store=Stores.ROS2_HUMBLE
    )

    print("Custom message definitions registered successfully.")

```

Once registered, the `RosbagInjector` (and the underlying `ROSLoader`) automatically detects and uses these definitions. There is no longer the need to pass the `custom_msgs` list in the `ROSInjectionConfig`.

```python
# main_injection.py
import setup_registry  # Runs the registration logic above
from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig, Stores
from pathlib import Path

# Initialize registry
setup_registry.register_project_messages()

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

The following table summarizes the injection performance for key sequences within this dataset:

| Sequence Name | Compression Factor | Injection Time | Notes |
| :--- | :--- | :--- | :--- |
| **`r2b_galileo2`** | ~70% | ~40 sec | High compression achieved. |
| **`r2b_galileo`** | ~1% | ~20 sec | **Low Compression:** Source contains pre-compressed images. <br> **Skipped Topics:** `/chassis/ticks` (No adapter available). |
| **`r2b_robotarm`** | ~66% | ~50 sec | High compression achieved. |
| **`r2b_whitetunnel`** | ~1% | ~20 sec | **Low Compression:** Source contains pre-compressed images. <br> **Skipped Topics:** `/chassis/ticks` (No adapter available). |

#### Known Issues & Limitations

While the underlying `rosbags` library supports the majority of standard ROS 2 bag files, specific datasets with non-standard serialization alignment or proprietary encodings may encounter compatibility issues.

**NVIDIA Isaac ROS Benchmark Dataset (2023)**

  * **Source:** [NVIDIA NGC Catalog - R2B Dataset 2023](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2023)
  * **Issue:** Deserialization failure during ingestion.
  * **Technical Details:** The ingestion process fails within the `AnyReader.deserialize` method of the `rosbags` library. The internal CDR deserializer triggers an assertion error indicating a mismatch in the expected data length vs. the raw payload size.
  * **Error Signature:**
    ```python
    # In rosbags.serde.cdr:
    assert pos + 4 + 3 >= len(rawdata)
    ```
  * **Recommendation:** This issue originates in the upstream `rosbags` parser handling of this specific dataset's serialization alignment. It is currently recommended to exclude this dataset or transcode it using standard ROS 2 tools before ingestion.


## Supported Message Types

***ROS-Specific Data Models***

In addition to mapping standard ROS messages to the core Mosaico ontology, the `ros-bridge` module implements two specialized data models. These are defined specifically for this module to handle ROS-native concepts that are not yet part of the official Mosaico standard:

* **`FrameTransform`**: Designed to handle coordinate frame transformations (modeled after `tf2_msgs/msg/TFMessage`). It encapsulates a list of `Transform` objects to manage spatial relationships.
* **`BatteryState`**: Modeled after `sensor_msgs/msg/BatteryState`, this class captures comprehensive power supply metrics. It includes core data (voltage, current, capacity, percentage) and detailed metadata such as power supply health, technology status, and individual cell readings.

> **Note:** Although these are provisional additions, both `FrameTransform` and `BatteryState` inherit from `Serializable` and `HeaderMixin`. This ensures they remain fully compatible with Mosaico’s existing serialization and header management infrastructure.

### Supported Message Types Table

| ROS Message Type | Mosaico Ontology Type | Adapter |
| :--- | :--- | :--- |
| `geometry_msgs/Pose`, `PoseStamped`... | `Pose` | `PoseAdapter` |
| `geometry_msgs/Twist`, `TwistStamped`... | `Velocity` | `TwistAdapter` |
| `geometry_msgs/Accel`, `AccelStamped`... | `Acceleration` | `AccelAdapter` |
| `geometry_msgs/Vector3`, `Vector3Stamped` | `Vector3d` | `Vector3Adapter` |
| `geometry_msgs/Point`, `PointStamped` | `Point3d` | `PointAdapter` |
| `geometry_msgs/Quaternion`, `QuaternionStamped` | `Quaternion` | `QuaternionAdapter` |
| `geometry_msgs/Transform`, `TransformStamped` | `Transform` | `TransformAdapter` |
| `geometry_msgs/Wrench`, `WrenchStamped` | `Velocity` | `WrenchAdapter` |
| `nav_msgs/Odometry` | `MotionState` | `OdometryAdapter` |
| `nmea_msgs/Sentence` | `NMEASentence` | `NMEASentenceAdapter` |
| `sensor_msgs/Image`, `CompressedImage` | `Image`, `CompressedImage` | `ImageAdapter`, `CompressedImageAdapter` |
| `sensor_msgs/Imu` | `IMU` | `IMUAdapter` |
| `sensor_msgs/NavSatFix` | `GPS`, `GPSStatus` | `GPSAdapter`, `NavSatStatusAdapter` |
| `sensor_msgs/CameraInfo` | `CameraInfo` | `CameraInfoAdapter` |
| `sensor_msgs/RegionOfInterest` | `ROI` | `ROIAdapter` |
| `sensor_msgs/JointState` | `RobotJoint` | `RobotJointAdapter` |
| `sensor_msgs/BatteryState` | `BatteryState` (ROS-specific)| `BatteryStateAdapter` |
| `std_msgs/msg/String`| `String`| `_GenericStdAdapter` |
| `std_msgs/msg/Int8(16,32,64)` | `Integer8(16,32,64)`| `_GenericStdAdapter` |
| `std_msgs/msg/UInt8(16,32,64)` | `Unsigned8(16,32,64)`| `_GenericStdAdapter` |
| `std_msgs/msg/Float32(64)` | `Floating32(64)`| `_GenericStdAdapter` |
| `std_msgs/msg/Bool` | `Boolean`| `_GenericStdAdapter` |
| `tf2_msgs/msg/TFMessage` | `FrameTransform` (ROS-specific)| `FrameTransformAdapter` |
