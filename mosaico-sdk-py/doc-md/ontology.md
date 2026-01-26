<div align="right">
<picture>
<source media="(prefers-color-scheme: light)" srcset="../../logo/mono_black.svg">
<source media="(prefers-color-scheme: dark)" srcset="../../logo/mono_white.svg">
<img alt="Mosaico logo" src="../../logo/color_white.svg" height="30">
</picture>
</div>

# Data Models & Ontology

This page details the Mosaico Data Ontology, the structural backbone of the SDK. It explains the architecture used to define data types and provides a detailed reference for the available Sensor and Data modules, including field definitions.

> [!NOTE]
> **Ontology Scope & Roadmap**
>
> The current version of the library implements a foundational subset of data types tailored primarily for **Robotics** and **Autonomous Systems** (e.g., IMU, GNSS, Cameras, Rigid Body Transforms).
>
> Mosaico is actively evolving. New sensor types (e.g., Lidar, Radar, Sonar) and domain-specific data structures are implemented frequently with each release. If a specific data type you require is missing, please refer to the [Contributing](../../CONTRIBUTING.md) page or [consider defining your own custom types](#customizing-the-data-ontology).

## Architecture & Base Classes

The Mosaico SDK employs a **Registry/Factory** pattern combined with **Mixins** to define data structures. This architecture ensures that every data object is:
1.  **Validatable:** Using Pydantic for runtime type checking.
2.  **Serializable:** Automatically mapping to efficient PyArrow schemas for high-throughput transport.
3.  **Queryable:** Providing a fluent API for filtering data (e.g., `IMU.Q.acceleration.x > 0`).

### `Serializable`

The `Serializable` class is the **common base factory of the Mosaico Data Ontology**. It acts as the abstract base class for all data payloads that can be transmitted to the platform.

It is designed to solve three major problems in a strongly-typed data system:

1.  **Registry Management:** How do we know which Python class corresponds to the string `"imu"` coming from a server response?
2.  **Factory Pattern:** How do we instantiate the correct class dynamically without a giant `if/else` block?
3.  **Schema Enforcement:** How do we ensure every data type has a valid PyArrow schema for efficient binary transport?

Unlike standard inheritance, `Serializable` uses the `__init_subclass__` hook. This method is automatically called whenever a developer defines a new subclass.

```python
class MyCustomSensor(Serializable):  # <--- __init_subclass__ triggers here
    ...
```
When this happens, `Serializable` performs the following steps automatically:

1.  **Validates Schema:** Checks if the subclass defined a `__msco_pyarrow_struct__`. If missing, it raises an error at definition time (import time), preventing runtime failures later.
2.  **Generates Tag:** If the class doesn't define `__ontology_tag__`, it auto-generates one from the class name (e.g., `MyCustomSensor` -> `"my_custom_sensor"`).
3.  **Registers Class:** It adds the new class to the global `_SENSOR_REGISTRY`.
4.  **Injects Query Proxy:** It dynamically adds a `.Q` attribute to the class, enabling the fluent query syntax (e.g., `MyCustomSensor.Q.voltage > 12.0`).

#### Class Reference

**Attributes:**

  * `__serialization_format__` (*ClassVar[SerializationFormat]*):
    A hint to the writer system on how to batch data.
      * `Default`: Optimizes for record count (e.g., flush every 1000 items). Used for lightweight telemetry.
      * `Image`: Optimizes for byte size (e.g., flush every 4MB). Used for heavy blobs like images or lidar.
  * `__ontology_tag__` (*ClassVar[str]*):
    The unique string identifier used in the platform catalog. If not set manually, it is auto-generated (CamelCase to snake\_case).
  * `__msco_pyarrow_struct__` (*ClassVar[pa.StructType]*):
    **Required.** The PyArrow definition of the data layout. This defines how the object is serialized to Parquet/Arrow format on the wire.

**Methods:**

  * **`create(tag: str, **kwargs) -> Serializable`**
    The universal factory method.

      * **Args:** `tag` is the string identifier (e.g., `"imu"`). `kwargs` are the data fields.
      * **Returns:** An instance of the correct subclass (e.g., `IMU(...)`).
      * **Use Case:** Used by the `ROSAdapter` or `TopicDataStreamer` to instantiate objects when reading data streams where the type is only known at runtime.

  * **`list_registered() -> List[str]`**
    Returns all available ontology tags currently loaded in the runtime.

  * **`get_class_type(tag: str) -> Type[Serializable]`**
    Resolves a string tag to the actual Python class object.

  * **`ontology_tag()`**
    Returns the tag for the current instance or class. Safer than accessing `__ontology_tag__` directly as it ensures initialization succeeded.

### Data Augmentation: Timestamps & Uncertainty
A key architectural feature of the Mosaico Data Ontology is the universal augmentation of data types via **Mixins**. Almost every class in the ontology—from high-level sensors like `IMU` down to elementary data primitives like `Vector3d` or `Float32`—inherits from two Mixin classes, which inject standard fields into data models via composition, ensuring consistency across different sensor types:

**`HeaderMixin`**: Injects a standard ROS-style `Header` field. Ideally used for all timestamped data.

* **`header`** (`Header`): The standard metadata header.
    * **`seq`** (`uint32`): Sequence ID. Legacy field, often unused in modern systems.
    * **`stamp`** (`Time`): Time of data acquisition.
        * **`sec`** (`int64`): Seconds component.
        * **`nanosec`** (`uint32`): Nanoseconds component [0, 1e9).
    * **`frame_id`** (`string`): Coordinate frame ID (e.g., 'map', 'camera_link').

**`CovarianceMixin`**: Injects uncertainty fields for sensor fusion applications.

* **`covariance`** (`list[float64]`): The covariance matrix (flattened) of the data.
* **`covariance_type`** (`int16`): Enum integer representing the covariance parameterization.

This design enables a powerful dual-usage pattern: **Standalone Messages** vs. **Embedded Fields**.

#### Standalone Usage (First-Class Messages)

Because even simple types like `Vector3d` are augmented with headers, you can treat them as independent, timestamped messages. This is useful for transmitting processed signals, debug values, or simple sensor readings without needing a complex container.

```python
# Sending a raw 3D vector as a timestamped message with uncertainty
accel_msg = Vector3d(
    x=0.0, 
    y=0.0, 
    z=9.81,
    header=Header(stamp=Time.now(), frame_id="base_link"),
    covariance=[0.01, 0, 0, 0, 0.01, 0, 0, 0, 0.01]  # 3x3 Diagonal matrix
)

# This is a valid payload for a TopicWriter
writer.push(Message(timestamp_ns=ts, data=accel_msg))

# Sending an error message string as a timestamped message
error_msg = String(
    data="Waypoint-miss in navigation detected!"
    header=Header(stamp=Time.now(), frame_id="base_link"),
)

# This is another valid payload for a TopicWriter
writer.push(Message(timestamp_ns=ts, data=error_msg))
```

#### 2\. Embedded Usage (Specific Attributes)

When these types are used as fields inside a larger structure (e.g., `IMU`), the mixins allow you to attach specific metadata to *parts* of the message.

For example, an `IMU` message has a global timestamp (in `IMU.header`), but its `acceleration` field (a `Vector3d`) can carry its own specific **covariance** matrix, distinct from the `angular_velocity` covariance. In this context, the inner `Vector3d.header` is typically left as `None`, avoiding redundancy.

```python
# Embedding Vector3d inside an IMU message
imu_msg = IMU(
    # Global header for the whole sensor reading
    header=Header(stamp=Time.now(), frame_id="imu_link"),
    
    # Acceleration field: No header (inherits global time), but HAS specific covariance
    acceleration=Vector3d(
        x=0.5, y=-0.2, z=9.8,
        covariance=[0.1, 0, 0, 0, 0.1, 0, 0, 0, 0.1]
    ),
    
    # Angular Velocity field: HAS different covariance
    angular_velocity=Vector3d(
        x=0.01, y=0.0, z=-0.01,
        covariance=[0.05, 0, 0, 0, 0.05, 0, 0, 0, 0.05]
    )
)
```

### `Message`

The `Message` class is the **universal transport envelope** for all data within the Mosaico platform. It acts as a wrapper that combines specific sensor data (the payload) with middleware-level metadata (the context).

While logically a `Message` contains a `data` object (e.g., an `IMU` or `Image`), physically on the wire (PyArrow/Parquet), the fields are **flattened**.

  * **Logical:** `Message(timestamp_ns=123, data=IMU(acceleration=Vector3d(x=1.0,...)))`
  * **Physical:** `Struct(timestamp_ns=123, acceleration, ...)`

This flattening is handled automatically by the `encode()` and `get_schema()` methods. This ensures zero-overhead access to nested data during queries while maintaining a clean object-oriented API in Python.

#### Fields

  * **`timestamp_ns`** (`int64`):
    The middleware processing timestamp in nanoseconds (Unix epoch). This usually represents when the data was *recorded* or *received* by the middleware during data recording onboard the robot, **distinct from the sensor's internal acquisition time** (which would be found inside `data.header.stamp`).
  * **`data`** (`Serializable`):
    The polymorphic payload. This can be any instance of a class registered in the Mosaico Ontology (e.g., `IMU`, `Image`, `GPS`).
  * **`message_header`** (`Header`, optional):
    An optional secondary header for middleware-specific metadata, distinct from the sensor's own header.

#### Methods (***Internal library usage***)

  * **`create(cls, tag: str, **kwargs) -> Message`**
    Factory method to instantiate a Message from a flat dictionary of arguments (typical when reading from Parquet/Arrow).

      * **Logic:** It inspects the registered class for the given `tag`, separates the arguments meant for the envelope (`timestamp_ns`) from those meant for the payload, and constructs the full object tree.
      * **Args:**
          * `tag`: The string identifier of the payload type (e.g., `"imu"`).
          * `**kwargs`: A merged dictionary containing all fields.

  * **`from_dataframe_row(row: pd.Series, topic_name: str) -> Optional[Message]`** (***Static Method***)
    Reconstructs a full `Message` object from a flattened DataFrame row generated by the [`DataFrameExtractor`](./ml/dataframe_extractor.md).
    * **Logic**: It automatically detects the naming convention, identifies the correct ontology tag from the column headers, and handles the conversion of `NaN` values back to `None`. It extracts both the middleware `timestamp_ns` and the internal `data` payload.
    * **Args**: 
        * `row`: A single row (Series) from a Mosaico DataFrame.
        * `topic_name`: The canonical name of the topic to extract.

  * **`encode() -> Dict[str, Any]`**
    Serializes the object tree into a flat dictionary suitable for PyArrow. It merges the envelope fields and the payload fields into a single level.

  * **`get_schema(cls, data_cls: Type[Serializable]) -> pa.Schema`**
    Generates the physical PyArrow schema for a specific message type. It merges the static `Message` schema with the dynamic `data_cls` schema.
      * **Validation:** It performs a collision check to ensure the payload class doesn't define fields that conflict with the envelope (e.g., a sensor cannot have a field named `timestamp_ns`).


#### Public API

  * **`get_data(target_type: Type[T]) -> T`**
    A type-safe accessor for the payload. It runtime-checks that the `data` attribute matches the expected `target_type` and returns it with proper type hinting for IDE autocompletion.

    ```python
    # Example
    msg = sequence_reader.next()
    imu = msg.get_data(IMU) # Returns IMU object or raises TypeError
    print(imu.acceleration.x)
    ```

  * **`ontology_type() -> Type[Serializable]`**
    Retrieves the Python class type of the ontology object stored in the data field. This accesses the `__class_type__` of the underlying data, allowing for dynamic type inspection of the message payload.

    ```python
    # Example
    msg_type = msg.ontology_type()
    # Returns the class, e.g., <class 'IMU'>
    ```

  * **`ontology_tag() -> str`**
    Returns the unique ontology tag name associated with the object in the data field. This provides a string-based identifier for the data type, useful for logging or routing logic without importing the specific class.

    ```python
    # Example
    tag = msg.ontology_tag()
    print(tag) # Output example: "imu"
    ```


## Data Ontology

These are the reusable mathematical and geometric building blocks used to construct the higher-level Sensor classes.

### Base Types (`base_types.py`)
Wrappers for Python primitives that allow them to be sent as timestamped `Messages`. All wrappers contain a single field:

**Fields**
  * **`data`**: The primitive value. Types include:
      * **Integers:** `Integer8`, `Integer16`, `Integer32`, `Integer64` (signed), and `Unsigned8/16/32/64`.
      * **Floats:** `Floating16`, `Floating32`, `Floating64`.
      * **Others:** `Boolean`, `String`, `LargeString` (for >2GB text).
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

### Geometry (`geometry.py`)
Defines spatial/geometric types.

#### Vectors & Points
Semantically represent vectors or points in space.

**Fields**
  * **`Vector2d`** / **`Point2d`**:
      * **`x`** (`float64`): X component.
      * **`y`** (`float64`): Y component.
      * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
      * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
      * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.
  * **`Vector3d`** / **`Point3d`**:
      * **`x`** (`float64`): X component.
      * **`y`** (`float64`): Y component.
      * **`z`** (`float64`): Z component.
      * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
      * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
      * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.
  * **`Vector4d`**:
      * **`x`**, **`y`**, **`z`**, **`w`** (`float64`): Components.
      * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
      * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
      * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

#### `Quaternion`
Semantically represents a Rotation.

**Fields**
  * **`x`**, **`y`**, **`z`**, **`w`** (`float64`): Quaternion components.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

#### `Transform`
Represents a spatial transformation between two coordinate frames.

**Fields**
  * **`translation`** (`Vector3d`): 3D translation vector.
  * **`rotation`** (`Quaternion`): Quaternion representing rotation.
  * **`target_frame_id`** (`string`): Target frame identifier.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

#### `Pose`
Represents an object's position and orientation in space.

**Fields**
  * **`position`** (`Point3d`): 3D position point.
  * **`orientation`** (`Quaternion`): Quaternion representing orientation.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

### Kinematics (`kinematics.py`)

#### `Velocity`
Represents 6-Degree-of-Freedom Velocity (Twist).

**Fields**
  * **`linear`** (`Vector3d`, ***Optional***): 3D linear velocity vector.
  * **`angular`** (`Vector3d`, ***Optional***): 3D angular velocity vector.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

> **Note**: The `linear` and `angular` fields are marked `Optional` to allow for partial definitions (e.g., specifying only linear velocity) without enforcing dummy values for the unused component and compromising the semantic meaning of the message. The fields are subject to strict model validation: user must provide either `linear`, `angular`, or both. Providing neither will result in a validation error, ensuring the semantic integrity of the type.

#### `Acceleration`
Represents 6-Degree-of-Freedom Acceleration.

**Fields**
  * **`linear`** (`Vector3d`, ***Optional***): 3D linear acceleration vector.
  * **`angular`** (`Vector3d`, ***Optional***): 3D angular acceleration vector.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

> **Note**: The `linear` and `angular` fields are marked `Optional` to allow for partial definitions (e.g., specifying only linear acceleration) without enforcing dummy values for the unused component and compromising the semantic meaning of the message. The fields are subject to strict model validation: user must provide either `linear`, `angular`, or both. Providing neither will result in a validation error, ensuring the semantic integrity of the type.

#### `MotionState`
A complete kinematic snapshot.

**Fields**
  * **`pose`** (`Pose`): 6D pose.
  * **`velocity`** (`Velocity`): 6D velocity.
  * **`target_frame_id`** (`string`): Target frame identifier.
  * **`acceleration`** (`Acceleration`, ***Optional***): 6D acceleration.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

### Dynamics (`dynamics.py`)

#### `ForceTorque`
Represents a "Wrench" applied at a specific point.

**Fields**

  * **`force`** (`Vector3d`): 3D linear force vector ($N$).
  * **`torque`** (`Vector3d`): 3D torque vector ($N \cdot m$).
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).
  * **`covariance`** (`list[float64]`, ***Optional***): Row-major representation of the covariance matrix.
  * **`covariance_type`** (`int16`, ***Optional***): Enum integer representing the covariance parameterization.

### ROI (`roi.py`)

#### `ROI`
A Region of Interest in an image.

**Fields**
  * **`offset`** (`Vector2d`): Top-Left corner ($x, y$) of the ROI.
  * **`height`** (`uint32`): Height in pixels.
  * **`width`** (`uint32`): Width in pixels.
  * **`do_rectify`** (`bool`, ***Optional***): True if the ROI applies to the rectified image. False if it applies to the raw image.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).


## Sensors Ontology

These classes represent physical hardware sensors or high-level perception outputs. They are the primary payloads wrapped inside a `Message`.

### Camera (`camera.py`)

The Camera module provides the following data classes.

#### `CameraInfo`
Meta-information for interpreting images from a calibrated camera. Mirrors standard robotics camera models.

**Fields:**
  * **`height`** (`uint32`): Height in pixels of the image with which the camera was calibrated.
  * **`width`** (`uint32`): Width in pixels of the image with which the camera was calibrated.
  * **`distortion_model`** (`string`): The distortion model used (e.g., 'plumb_bob', 'rational_polynomial').
  * **`distortion_parameters`** (`list[float64]`): The distortion coefficients ($k_1, k_2, t_1, t_2, k_3...$). Size depends on the model.
  * **`intrinsic_parameters`** (`list[float64]`, size 9): The 3x3 Intrinsic Matrix ($K$) flattened row-major. Projects 3D points in the camera coordinate frame to 2D pixel coordinates.
  * **`rectification_parameters`** (`list[float64]`, size 9): The 3x3 Rectification Matrix ($R$) flattened row-major. Used for stereo cameras to align the two image planes.
  * **`projection_parameters`** (`list[float64]`, size 12): The 3x4 Projection Matrix ($P$) flattened row-major. Projects 3D world points directly into the rectified image pixel coordinates.
  * **`binning`** ([`Vector2d](#vectors--points), ***Optional***): Hardware binning factor ($x, y$). If null, assumes (0, 0) (no binning).
  * **`roi`** (`ROI`, ***Optional***): Region of Interest. Used if the image is a sub-crop of the full resolution.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).


### Image (`image.py`)

#### `Image` (Raw)

Represents uncompressed pixel data with explicit memory layout control. It includes helper methods to convert to/from standard Python `PIL` images.

**Fields:**

* **`data`** (`binary`): The flattened image memory buffer.
* **`format`** (`string`): Container format (e.g., 'raw', 'png').
* **`width`** (`int32`): Image width in pixels.
* **`height`** (`int32`): Image height in pixels.
* **`stride`** (`int32`): Bytes per row. Essential for alignment.
* **`encoding`** (`string`): Pixel format (e.g., 'bgr8', 'mono16').
* **`is_bigendian`** (`bool`, ***Optional***): True if data is Big-Endian. Defaults to system endianness if null.
* **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

**Methods:**

* **`from_pillow(cls, pil_image: PIL.Image, ...) -> Image`**
Factory method that automatically handles data flattening, stride calculation, and type casting (e.g., converting a float32 Depth map to the correct byte representation). The function accepts the preferred serialization format; the allowed formats are `png` or `raw` (lossless representation). If None, `png` is selected.
* **`to_pillow() -> PIL.Image`**
Converts the raw binary data back into a standard Pillow Image object. Handles complex logic like reshuffling BGR to RGB, handling big-endian systems, and reshaping 1D buffers back to 2D arrays.
* **`from_linear_pixels(cls, data: List[int], stride: int, ...) -> Image`**
Low-level factory to create an Image instance directly from a raw byte list and dimensions. Implements the "Wide Grayscale" trick for saving complex types into standard containers. The function accepts the preferred serialization format; the allowed formats are `png` or `raw` (lossless representation). If None, `png` is selected.
* **`to_linear_pixels() -> List[int]`**
Returns the raw, flattened integer list of pixel data, decoding any transport container (like PNG) if necessary.

#### *The "Wide Grayscale" Concept*

The **Wide Grayscale** strategy is a technique used by the SDK to losslessly store image data (such as 32-bit floating-point depth maps or 16-bit raw sensor data) inside standard image containers like PNG. Instead of treating the data as "pixels" with semantic meaning (colors), the SDK treats the image memory as a **raw byte stream**.

1. **Reinterpretation:** The original buffer (e.g., a 100x100 image of `float32`) is viewed simply as a list of bytes. Since each `float32` is 4 bytes, a single row of 100 pixels becomes 400 bytes.
2. **Reshaping:** This byte stream is reshaped into a new 2D matrix where:
  * **Height** = Original Image Height.
  * **Width** = The full **Stride** of the original image (total bytes per row, including padding).
3. **Encoding:** This new "wide" matrix is saved as a standard **8-bit Grayscale (Mode 'L')** image.

To the PNG encoder, it looks like a very wide, boring grayscale image. To the SDK, it is a perfect, bit-exact copy of the original memory buffer, preserved in a compressed, standard format.

#### `CompressedImage`

Container for encoded binary blobs. Unlike the raw `Image` class, this class uses a simple stateless codec for standard formats (PNG, JPEG) but requires an [external session decoder](#statefuldecodingsession) for stateful video streams (H.264, HEVC).

**Fields:**

* **`data`** (`binary`): The serialized (compressed) image data as bytes.
* **`format`** (`string`): The compression format identifier (e.g., 'jpeg', 'png').
* **`header`** (`Header`, ***Optional***): Standard metadata header (timestamp, frame_id).

**Methods:**

* **`from_image(cls, image: PIL.Image, format: str = 'png', ...) -> CompressedImage`**
Factory method that compresses a Pillow image into a binary blob.
> [!NOTE]
> **Stateful vs Stateless**
>
> This method uses a default stateless codec. It is valid for standard formats like 'png' and 'jpeg'. For stateful formats (H.264, HEVC), you must currently use custom encoders, as the default implementation only supports single-image compression.

* **`to_image() -> Optional[PIL.Image]`**
Decompresses the internal binary data into a usable Pillow Image object.
> [!WARNING]
> **Limitation**: 
>
>This method works **only** for stateless formats (PNG, JPEG). If the image is a video frame (H.264, HEVC), this method will not work because it lacks the decoder context. Use `StatefulDecodingSession` for video streams.



#### `StatefulDecodingSession`

A helper class designed to manage the decoding lifecycle of video streams (H.264, HEVC). It prevents memory corruption by maintaining a separate `av.CodecContext` for each context stream (e.g. topic).

**Methods:**

* **`decode(img_data: bytes, format: ImageFormat, context: str) -> Optional[PIL.Image]`**
Decodes a video packet. The `context` argument (usually the topic name) ensures that packets from different streams do not mix their decoding state.


**Example: Decoding a Mixed Sequence**

This example shows how to handle both simple images (JPEG/PNG) and complex video streams (H.264) in the same loop using the new `StatefulDecodingSession`.

```python
# Initialize the session manager
decoder_session = StatefulDecodingSession()

# Iterate through your sequence
for topic_name, message in sequence_handler.get_data_streamer():

    # ...

    # Let's suppose we know it is a CompressedImage topic, with H.264 format
    img = message.get_data(CompressedImage)
    pil_image = decoder_session.decode(
        img_data=img.data,
        format=img.format,
        context=topic_name  # Unique ID for this stream
    )
        
    pil_image = message.to_image()

# Cleanup resources when done
decoder_session.close()
```

### IMU (`imu.py`)

#### `IMU`
Aggregates inertial measurements.

**Fields**
  * **`acceleration`** (`Vector3d`): Linear acceleration vector in $m/s^2$.
  * **`angular_velocity`** (`Vector3d`): Angular velocity vector in $rad/s$.
  * **`orientation`** (`Quaternion`, ***Optional***): Estimated orientation quaternion.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

### GPS / GNSS (`gps.py`)

#### `GPS`
A processed navigation solution.

**Fields**
  * **`position`** (`Point3d`): Lat/Lon/Alt (WGS 84).
  * **`velocity`** (`Vector3d`, ***Optional***): Velocity vector [North, East, Alt] in $m/s$.
  * **`status`** (`GPSStatus`, ***Optional***): Receiver status info.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

#### `GPSStatus`
Status of the GNSS receiver and satellite fix.

**Fields**
  * **`status`** (`int8`): Fix status.
  * **`service`** (`uint16`): Service used (GPS, GLONASS, etc).
  * **`satellites`** (`int8`, ***Optional***): Satellites visible/used.
  * **`hdop`** (`float64`, ***Optional***): Horizontal Dilution of Precision.
  * **`vdop`** (`float64`, ***Optional***): Vertical Dilution of Precision.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

#### `NMEASentence`
Raw ASCII strings output by GNSS receivers.

**Fields**
  * **`sentence`** (`string`): Raw ASCII sentence (e.g., `$GPGGA...`).
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

### Magnetometer (`magnetometer.py`)

#### `Magnetometer`
Magnetic field measurement data.

**Fields**
  * **`magnetic_field`** (`Vector3d`): Magnetic field vector [$m_x, m_y, m_z$] in microTesla ($\mu T$).
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).

### Robot (`robot.py`)

#### `RobotJoint`
Snapshot of robot joint states. Arrays must be index-aligned.

**Fields**
  * **`names`** (`list[string]`): Names of the different robot joints.
  * **`positions`** (`list[float64]`): Positions ($rad$ or $m$) of the different robot joints.
  * **`velocities`** (`list[float64]`): Velocities ($rad/s$ or $m/s$) of the different robot joints.
  * **`efforts`** (`list[float64]`): Efforts ($N$ or $N \cdot m$) applied to the different robot joints.
  * **`header`** (`Header`, ***Optional***): Standard metadata header (stamp, frame_id).


## Customizing the Data Ontology

While the Mosaico SDK provides a comprehensive set of standard robotics types (IMU, Camera, etc.), your application may require domain-specific data structures. The ontology is designed to be easily extensible, allowing you to define custom types that are essentially "first-class citizens" within the SDK.

To define a new data type that is fully compatible with the Mosaico platform (validatable, serializable, and queryable), follow these steps:

### 1\. Inheritance

Your class **must** inherit from `mosaicolabs.models.Serializable`. This base class handles auto-registration, factory creation, and query proxy generation.

  * **Timestamps & Headers:** If your data represents a time-stamped event or sensor reading, it is **highly recommended** to also inherit from `HeaderMixin` instead of defining your own timestamp fields. This injects the standard `header` field (stamp, frame_id, seq), ensuring your data aligns with the rest of the ecosystem (e.g., for time-synchronization).
  * **Uncertainty:** If your data includes measurement uncertainty, inherit from `CovarianceMixin` to standardize how covariance matrices are stored.

### 2\. Define the Schema (`__msco_pyarrow_struct__`)

You must define a class-level attribute named `__msco_pyarrow_struct__`. This is a `pyarrow.struct` that defines exactly how your data will be serialized to Parquet/Arrow format on the wire.

#### 2.1\. Override the Serialization Format (`__serialization_format__`)

Mosaico allows you to adapt the data serialization formats on the remote server to ensure efficient data compression. This is controlled by overriding the `__serialization_format__` class attribute in your custom ontology.

The available formats are defined in the `SerializationFormat` enum:

```python
class SerializationFormat(StrEnum):
    """
    Defines the structural format used when serializing ontology data
    for storage or transmission.
    """

    Default = "default"
    """
    Recommended for fixed-width tabular data format, like a standard DataFrame. 
    (e.g., sensors with a constant number of fields and fixed-size data)
    """

    Ragged = "ragged"
    """
    Recommended for data containing variable-length lists or sequences 
    (e.g., point clouds, lists of detections, or non-uniform arrays). 
    """

    Image = "image"
    """
    Represents raw or compressed image data, often requiring specialized 
    compression/decompression handling.
    """

```

**Note:** If the `__serialization_format__` variable is not explicitly set, the value defaults to `SerializationFormat.Default` (standard tabular data).

### 3\. Define Class Fields

Define the actual Python fields for your class using standard type hints (Pydantic style).

> [!NOTE]
> **Naming** 
>
> The names of your Python fields **must match exactly** the names defined in your PyArrow schema.


### Example: `EnvironmentSensor`
Here is a complete example of defining a custom sensor for environmental monitoring.

<div align="center">
<picture>
<img alt="Mosaico logo" src="./imgs/ontology_customization.png" height="400">
</picture>
</div>


```python

# file: custom_ontology.py

from typing import Optional
import pyarrow as pa
from mosaicolabs.models import Serializable, HeaderMixin

class EnvironmentSensor(Serializable, HeaderMixin):
    """
    Custom sensor reading for Temperature, Humidity, and Pressure.
    """

    # --- 1. Define the Wire Schema ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "temperature", 
                pa.float32(), 
                nullable=False, 
                metadata={"description": "Temperature in Celsius"}
            ),
            pa.field(
                "humidity", 
                pa.float32(), 
                nullable=True, 
                metadata={"description": "Relative humidity (0.0 to 1.0)"}
            ),
            pa.field(
                "pressure", 
                pa.float32(), 
                nullable=True, 
                metadata={"description": "Atmospheric pressure in Pascal"}
            ),
        ]
    )

    # --- 2. Define Python Fields (Must match schema names!) ---
    temperature: float
    humidity: Optional[float] = None
    pressure: Optional[float] = None


# --- Usage --- file: main.py

# !! Import your custom definitions: this forces Python executing the class definition 
# and registering the tag in the Serializable factory
from my_project.custom_ontology import EnvironmentSensor

from mosaicolabs.models import Message, Header, Time

# Instantiate your custom type
meas = EnvironmentSensor(
    header=Header(stamp=Time.now(), frame_id="lab_sensor_1"),
    temperature=23.5,
    humidity=0.45
)

# It is now ready to be pushed to a writer or used in a query

```