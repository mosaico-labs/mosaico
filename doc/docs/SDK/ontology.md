---
title: Data Models & Ontology
description: Strongly typed data structures.
sidebar:
    order: 3
---

The **Mosaico Data Ontology** is the semantic backbone of the SDK. 
It defines the structural "rules" that transform raw binary streams into meaningful physical data, such as GPS coordinates, 
inertial measurements, or camera frames.

By using a strongly-typed ontology, Mosaico ensures that your data remains consistent, validatable, 
and highly optimized for both high-throughput transport and complex queries.


## Core Philosophy

The ontology is designed to solve the "generic data" problem in robotics by ensuring every data object is:

1. **Validatable**: Uses Pydantic for strict runtime type checking of sensor fields.
2. **Serializable**: Automatically maps Python objects to efficient **PyArrow** schemas for high-speed binary transport.
3. **Queryable**: Injects a fluent API (`.Q`) into every class, allowing you to filter databases based on physical values (e.g., `IMU.Q.acceleration.x > 6.0`).
4. **Middleware-Agnostic**: Acts as an abstraction layer so that your analysis code doesn't care if the data originally came from ROS, a simulator, or a custom logger.

## Available Ontology Classes

The Mosaico SDK provides a comprehensive library of models that transform raw binary streams into validated, queryable Python objects. These are grouped by their physical and logical application below.

### Base Data Models
API Reference: [Base Data Types](./API_reference/models/data_types.md)

These models serve as timestamped, metadata-aware wrappers for standard primitives. They allow simple diagnostic or scalar values to be treated as first-class members of the platform.

| Module | Classes | Purpose |
| --- | --- | --- |
| **Primitives** | [`String`][mosaicolabs.models.data.base_types.String], [`LargeString`][mosaicolabs.models.data.base_types.LargeString] | UTF-8 text data for logs or status messages. |
| **Booleans** | [`Boolean`][mosaicolabs.models.data.base_types.Boolean] | Logic flags (True/False). |
| **Signed Integers** | [`Integer8`][mosaicolabs.models.data.base_types.Integer8], [`Integer16`][mosaicolabs.models.data.base_types.Integer16], [`Integer32`][mosaicolabs.models.data.base_types.Integer32], [`Integer64`][mosaicolabs.models.data.base_types.Integer64] | Signed whole numbers of varying bit-depth. |
| **Unsigned Integers** | [`Unsigned8`][mosaicolabs.models.data.base_types.Unsigned8], [`Unsigned16`][mosaicolabs.models.data.base_types.Unsigned16], [`Unsigned32`][mosaicolabs.models.data.base_types.Unsigned32], [`Unsigned64`][mosaicolabs.models.data.base_types.Unsigned64] | Non-negative integers for counters or IDs. |
| **Floating Point** | [`Floating16`][mosaicolabs.models.data.base_types.Floating16], [`Floating32`][mosaicolabs.models.data.base_types.Floating32], [`Floating64`][mosaicolabs.models.data.base_types.Floating64] | Real numbers for high-precision physical values. |

### Geometry & Kinematics Models
API Reference: [Geometry Models](./API_reference/models/geometry.md)

These structures define spatial relationships and the movement states of objects in 2D or 3D coordinate frames.

| Module | Classes | Purpose |
| --- | --- | --- |
| **Points & Vectors** | [`Vector2d/3d/4d`][mosaicolabs.models.data.geometry.Vector2d], [`Point2d/3d`][mosaicolabs.models.data.geometry.Point2d] | Fundamental spatial directions and locations. |
| **Rotations** | [`Quaternion`][mosaicolabs.models.data.geometry.Quaternion] | Compact, singularity-free 3D orientation (). |
| **Spatial State** | [`Pose`][mosaicolabs.models.data.geometry.Pose], [`Transform`][mosaicolabs.models.data.geometry.Transform] | Absolute positions or relative coordinate frame shifts. |
| **Motion** | [`Velocity`][mosaicolabs.models.data.kinematics.Velocity], [`Acceleration`][mosaicolabs.models.data.kinematics.Acceleration] | Linear and angular movement rates (Twists and Accels). |
| **Aggregated State** | [`MotionState`][mosaicolabs.models.data.kinematics.MotionState] | An atomic snapshot combining Pose, Velocity, and Acceleration. |

### Sensor Models
API Reference: [Sensor Models](./API_reference/models/sensors.md)

High-level models representing physical hardware devices and their processed outputs.

| Module | Classes | Purpose |
| --- | --- | --- |
| **Inertial** | [`IMU`][mosaicolabs.models.sensors.IMU] | 6-DOF inertial data: linear acceleration and angular velocity. |
| **Navigation** | [`GPS`][mosaicolabs.models.sensors.GPS], [`GPSStatus`][mosaicolabs.models.sensors.GPSStatus], [`NMEASentence`][mosaicolabs.models.sensors.NMEASentence] | Geodetic fixes (WGS 84), signal quality, and raw NMEA strings. |
| **Vision** | [`Image`][mosaicolabs.models.sensors.Image], [`CompressedImage`][mosaicolabs.models.sensors.CompressedImage], [`CameraInfo`][mosaicolabs.models.sensors.CameraInfo], [`ROI`][mosaicolabs.models.data.ROI] | Raw pixels, encoded streams (JPEG/H264), calibration, and regions of interest. |
| **Environment** | [`Temperature`][mosaicolabs.models.sensors.Temperature], [`Pressure`][mosaicolabs.models.sensors.Pressure], [`Range`][mosaicolabs.models.sensors.Range] | Thermal readings (K), pressure (Pa), and distance intervals (m). |
| **Dynamics** | [`ForceTorque`][mosaicolabs.models.data.dynamics.ForceTorque] | 3D force and torque vectors for load sensing. |
| **Magnetic** | [`Magnetometer`][mosaicolabs.models.sensors.Magnetometer] | Magnetic field vectors measured in microTesla (). |
| **Robotics** | [`RobotJoint`][mosaicolabs.models.sensors.RobotJoint] | States (position, velocity, effort) for index-aligned actuator arrays. |

## Architecture

The ontology architecture relies on three primary abstractions: the **Factory** (`Serializable`), the **Envelope** (`Message`) and the **Mixins**

### 1. `Serializable` (The Factory)
API Reference: [`mosaicolabs.models.Serializable`][mosaicolabs.models.Serializable]

Every data payload in Mosaico inherits from the `Serializable` class. It manages the global registry of data types and ensures that the system knows exactly how to convert a string tag like `"imu"` back into a Python class with a specific binary schema.
`Serializable` uses the `__init_subclass__` hook, which is automatically called whenever a developer defines a new subclass.

```python
class MyCustomSensor(Serializable):  # <--- __init_subclass__ triggers here
    ...
```
When this happens, `Serializable` performs the following steps automatically:

1.  **Validates Schema:** Checks if the subclass defined the PyArrow struct schema (`__msco_pyarrow_struct__`). 
If missing, it raises an error at definition time (import time), preventing runtime failures later.
2.  **Generates Tag:** If the class doesn't define `__ontology_tag__`, it auto-generates one from the class name (e.g., `MyCustomSensor` -> `"my_custom_sensor"`).
3.  **Registers Class:** It adds the new class to the global types registry.
4.  **Injects Query Proxy:** It dynamically adds a `.Q` attribute to the class, enabling the fluent query syntax (e.g., `MyCustomSensor.Q.voltage > 12.0`).


### 2. `Message` (The Envelope)
API Reference: [`mosaicolabs.models.Message`][mosaicolabs.models.Message]

The **`Message`** class is the universal transport envelope for all data within the Mosaico platform. 
It acts as a wrapper that combines specific sensor data (the payload) with middleware-level metadata.

```python
from mosaicolabs import Message, Time, Header, Temperature
# Use Case: Create a Temperature timestamped message with uncertainty
meas_time = Time.now()
temp_msg = Message(
    timestamp_ns=meas_time.to_nanoseconds(), # here the message timestamp is the same as the measurement, but it can be different
    data=Temperature.from_celsius(
        value=57,
        header=Header(stamp=meas_time, frame_id="comp_case"),
        variance=0.03
    )
)
```


While logically a `Message` contains a `data` object (e.g., an instance of an Ontology type), physically on the wire (PyArrow/Parquet), the fields are **flattened**.

  * **Logical:** `Message(timestamp_ns=123567890, data=IMU(acceleration=Vector3d(x=1.0,...)))`
  * **Physical:** `Struct(timestamp_ns=123567890, acceleration, ...)`

This flattening is handled automatically by the class internal methods.
This ensures zero-overhead access to nested data during queries while maintaining a clean object-oriented API in Python.


### 3. Mixins: Headers & Uncertainty

Mosaico uses **Mixins** to inject standard fields across different data types, ensuring a consistent interface.
Almost every class in the ontology, from high-level sensors down to elementary data primitives like `Vector3d` or `Float32`, 
inherits from two Mixin classes, which inject standard fields into data models via composition, ensuring consistency across different sensor types.
The integration of mixins into the Mosaico Data Ontology enables a flexible dual-usage pattern, **Standalone Messages** and **Embedded Fields**, 
which will be detailed later and allow base geometric types to serve as either independent data streams or granular components of complex sensor models.

#### `HeaderMixin`
API Reference: [`mosaicolabs.models.mixins.HeaderMixin`][mosaicolabs.models.mixins.HeaderMixin]

Injects a standard (Optional) `header` containing a sequence ID, a frame ID (e.g., `"base_link"`), and a high-precision acquisition timestamp (`stamp`).

```python
class MySensor(Serializable, HeaderMixin):
    # Injects a header with stamp, frame_id, and seq fields
    ...
```


#### `CovarianceMixin`
API Reference: [`mosaicolabs.models.mixins.CovarianceMixin`][mosaicolabs.models.mixins.CovarianceMixin]

Injects multidimensional uncertainty fields, typically used for flattened covariance matrices in sensor fusion applications.

```python
class MySensor(Serializable, CovarianceMixin):
    # Injects a covariance matrix with covariance and covariance_type fields
    ...
```


#### `VarianceMixin`
API Reference: [`mosaicolabs.models.mixins.VarianceMixin`][mosaicolabs.models.mixins.VarianceMixin]

Injects monodimensional uncertainty fields, useful for sensors with 1-dimensional uncertain data (like `Temperature` or `Pressure`).

```python
class MySensor(Serializable, VarianceMixin):
    # Injects a variance with variance and variance_type fields
    ...
```


#### Standalone Usage

Because elementary types (such as `Vector3d`, `String`, or `Float32`) inherit directly from these mixins, they are "first-class" members of the ontology. 
You can treat them as independent, timestamped messages without needing to wrap them in a more complex container.

This is ideal for pushing processed signals, debug values, or simple sensor readings that require their own metadata and uncertainty context.

```python
# Use Case: Sending a raw 3D vector as a timestamped message with uncertainty
accel_msg = Vector3d(
    x=0.0, 
    y=0.0, 
    z=9.81,
    header=Header(stamp=Time.now(), frame_id="base_link"),
    covariance=[0.01, 0, 0, 0, 0.01, 0, 0, 0, 0.01]  # 3x3 Diagonal matrix
)

# `acc_writer` is a TopicWriter associated to the new sequence that is being uploaded.
acc_writer.push(message=Message(timestamp_ns=ts, data=accel_msg)) # (1)!

# Use Case: Sending a timestamped diagnostic error
error_msg = String(
    data="Waypoint-miss in navigation detected!",
    header=Header(stamp=Time.now(), frame_id="base_link")
)

# `log_writer` is another TopicWriter associated to the new sequence that is being uploaded.
log_writer.push(message=Message(timestamp_ns=ts, data=error_msg))
```

1. The `push` command will be covered in the documentation of the [Writers](./handling/writing.md#the-data-engine-topicwriter)
    API Reference:
    * [`mosaicolabs.handlers.SequenceWriter`][mosaicolabs.handlers.SequenceWriter]
    * [`mosaicolabs.handlers.TopicWriter`][mosaicolabs.handlers.TopicWriter]

#### Embedded Usage

When these base types are used as internal fields within a larger structure (e.g., an `IMU` or `MotionState` model), the mixins allow you to attach metadata to specific *parts* of a message.

In this context, while the parent object (the `IMU`) carries a global timestamp, the individual fields (like `acceleration`) can carry their own specific **covariance** matrices.
To avoid data redundancy, the internal `header` of the embedded field is typically left as `None`, as it inherits the temporal context from the parent message.

```python
# Use Case: Embedding Vector3d inside a complex IMU message
imu_msg = IMU(
    # Parent Header: Defines the time and frame for the entire sensor packet
    header=Header(stamp=Time.now(), frame_id="imu_link"),
    
    # Embedded Field 1: Acceleration
    # Inherits global time, but specifies its own unique uncertainty
    acceleration=Vector3d(
        x=0.5, y=-0.2, z=9.8,
        covariance=[0.1, 0, 0, 0, 0.1, 0, 0, 0, 0.1] # Specific to acceleration
    ),
    
    # Embedded Field 2: Angular Velocity
    # Carries a distinct covariance matrix independent of the acceleration
    angular_velocity=Vector3d(
        x=0.01, y=0.0, z=-0.01,
        covariance=[0.05, 0, 0, 0, 0.05, 0, 0, 0, 0.05] # Specific to velocity
    )
)

# as above, `imu_writer` is another TopicWriter associated to the new sequence that is being uploaded.
imu_writer.push(imu_msg)

```


## Querying Data Ontology with the Query (`.Q`) Proxy

The Mosaico SDK allows you to perform deep discovery directly on the physical content of your sensor streams. Every class inheriting from [`Serializable`][mosaicolabs.models.Serializable], including standard sensors, geometric primitives, and custom user models, is automatically injected with a static **`.Q` proxy** attribute.

This proxy acts as a type-safe bridge between your Python data models and the platform's search engine, enabling you to construct complex filters using standard Python dot notation.

### How the Proxy Works

The `.Q` proxy recursively inspects the model’s schema to expose every queryable field path. It identifies the data type of each field and provides only the operators valid for that type (e.g., numeric comparisons for acceleration, substring matches for frame IDs).

* **Direct Field Access**: Filter based on primary values, such as `Temperature.Q.value.gt(25.0)`.
* **Nested Navigation**: Traverse complex, embedded structures. For example, in the [`GPS`][mosaicolabs.models.sensors.GPS] model, you can drill down into the status sub-field: `GPS.Q.status.satellites.geq(8)`.
* **Mixin Integration**: Fields inherited from mixins are automatically included in the proxy. This allows you to query standard metadata (from `HeaderMixin`) or uncertainty metrics (from `VarianceMixin` or `CovarianceMixin`) across any model.

### Queryability Examples

The following table illustrates how the proxy flattens complex hierarchies into queryable paths:

| Type Field Path | Proxy Field Path | Source Type | Queryable Type | Supported Operators |
| --- | --- | --- | --- | --- |
| `IMU.acceleration.x` | `IMU.Q.acceleration.x` | `float` | **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
| `GPS.status.hdop` | `GPS.Q.status.hdop` | `float` | **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
| `IMU.header.frame_id` | `IMU.Q.header.frame_id` | `str` | **String** | `.eq()`, `.neq()`, `.match()`, `.in_()` |
| `GPS.covariance_type` | `GPS.Q.covariance_type` | `int` | **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

### Practical Usage

To execute these filters, pass the expressions generated by the proxy to the [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder.

```python
from mosaicolabs import MosaicoClient, IMU, GPS, QueryOntologyCatalog

with MosaicoClient.connect("localhost", 6726) as client:
    # orchestrate a query filtering by physical thresholds AND metadata
    qresponse = client.query(
        QueryOntologyCatalog(include_timestamp_range=True) # Ask for the start/end timestamps of occurrences
        .with_expression(IMU.Q.acceleration.z.gt(15.0))
        .with_expression(GPS.Q.status.service.eq(2))
    )

    # The server returns a QueryResponse grouped by Sequence for structured data management
    if qresponse is not None:
        for item in qresponse:
            # 'item.sequence' contains the name for the matched sequence
            print(f"Sequence: {item.sequence.name}") 
            
            # 'item.topics' contains only the topics and time-segments 
            # that satisfied the QueryOntologyCatalog criteria
            for topic in item.topics:
                # Access high-precision timestamps for the data segments found
                start, end = topic.timestamp_range.start, topic.timestamp_range.end
                print(f"  Topic: {topic.name} | Match Window: {start} to {end}")
```

For a comprehensive list of all supported operators and advanced filtering strategies (such as query chaining), see the **[Full Query Documentation](./query.md)** and the Ontology types SDK Reference in the **API Reference**:

* [Base Data Models](./API_reference/models/data_types.md)
* [Sensors Models](./API_reference/models/sensors.md)
* [Geometry Models](./API_reference/models/geometry.md)
* [Platform Models](./API_reference/models/platform.md)

## Customizing the Ontology

The Mosaico SDK is built for extensibility, allowing you to define domain-specific data structures that can be registered to the platform and live alongside standard types.
Custom types are automatically validatable, serializable, and queryable once registered in the platform.

Follow these three steps to implement a compatible custom data type:

### 1. Inheritance and Mixins

Your custom class **must** inherit from `Serializable` to enable auto-registration, factory creation, and the queryability of the model. 
To align with the Mosaico ecosystem, use the following mixins:

* **`HeaderMixin`**: Required for timestamped data or sensor readings. It injects a standard `header` (stamp, frame_id, seq), ensuring your data remains compatible with time-synchronization and coordinate frame logic.
* **`CovarianceMixin`**: Used for data including measurement uncertainty, standardizing the storage of covariance matrices.


### 2. Define the Wire Schema (`__msco_pyarrow_struct__`)

You must define a class-level `__msco_pyarrow_struct__` using `pyarrow.struct`. This explicitly dictates how your Python object is serialized into high-performance Apache Arrow/Parquet buffers for network transmission and storage.

#### 2.1 Serialization Format Optimization
API Reference: [`mosaicolabs.enum.SerializationFormat`][mosaicolabs.enum.SerializationFormat]

You can optimize remote server performance by overriding the `__serialization_format__` attribute. This controls how the server compresses and organizes your data.


| Format | Identifier | Use Case Recommendation |
| --- | --- | --- |
| **Default** | `"default"` | **Standard Table**: Fixed-width data with a constant number of fields. |
| **Ragged** | `"ragged"` | **Variable Length**: Best for lists, sequences, or point clouds. |
| **Image** | `"image"` | **Blobs**: Raw or compressed images requiring specialized codec handling. |

If not explicitly set, the system defaults to `Default` format.

### 3. Define Class Fields

Define the Python attributes for your class using standard type hints. 
Note that the names of your Python class fields **must match exactly** the field names defined in your `__msco_pyarrow_struct__` schema.


### Customization Example: `EnvironmentSensor`

This example demonstrates a custom sensor for environmental monitoring that tracks temperature, humidity, and pressure.

```python
# file: custom_ontology.py

from typing import Optional
import pyarrow as pa
from mosaicolabs.models import Serializable, HeaderMixin

class EnvironmentSensor(Serializable, HeaderMixin):
    """
    Custom sensor reading for Temperature, Humidity, and Pressure.
    """

    # --- 1. Define the Wire Schema (PyArrow Layout) ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field("temperature", pa.float32(), nullable=False),
            pa.field("humidity", pa.float32(), nullable=True),
            pa.field("pressure", pa.float32(), nullable=True),
        ]
    )

    # --- 2. Define Python Fields (Must match schema exactly) ---
    temperature: float
    humidity: Optional[float] = None
    pressure: Optional[float] = None


# --- Usage Example ---
from mosaicolabs.models import Message, Header, Time

# Initialize with standard metadata
meas = EnvironmentSensor(
    header=Header(stamp=Time.now(), frame_id="lab_sensor_1"),
    temperature=23.5,
    humidity=0.45
)

# Ready for streaming or querying
# writer.push(Message(timestamp_ns=ts, data=meas))

```
<figure markdown="span">
  ![Ontology customization example](../assets/ontology_customization.png)
  <figcaption>Schema for defining a custom ontology model.</figcaption>
</figure>

