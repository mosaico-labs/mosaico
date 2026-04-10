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

#### Futures
API Reference: [Futures Models](./API_reference/models/futures.md)

Prospective high-level models representing emerging or not yet fully standardized sensor hardware. The `futures` module acts as a **transitional space** where experimental ontologies are introduced and iteratively refined before being promoted to the stable, production-ready ontology set.

The following sensors are supported as first-class data types in this module:

| Model | Description |
|---|---|
| **LiDAR** | 3D point cloud data from laser-based ranging sensors, supporting full spatial geometry and optional intensity/RGB fields. |
| **Radar** | Range, velocity, and azimuth measurements from radio-frequency sensors, including Doppler-based target detection. |
| **LaserScan** | 2D planar sweep data from single-line laser rangefinders, encoded as ordered range and intensity arrays. |
| **Multi-Echo LaserScan** | Extension of LaserScan supporting multiple return echoes per beam, enabling richer surface characterization. |
| **RGBD Camera** | Paired color and depth frames from structured-light or time-of-flight RGB-D sensors. |
| **Stereo Camera** | Synchronized left/right image pairs from stereo rigs, suitable for disparity estimation and 3D reconstruction. |
| **ToF Camera** | Per-pixel depth and amplitude data from time-of-flight imaging sensors. |

!!! warning "Experimental Ontologies"
    The `futures` module is a transitional area where ontologies under active experimentation are hosted before graduating to the stable ontology set. Field definitions, unit conventions, and structural relationships are **not yet considered final** and will be refined based on feedback from real-world integrations and adopters. Once an ontology reaches sufficient maturity and coverage, it will be promoted out of `futures` and into the core, production-ready modules.

## MosaicoType & MosaicoField
When defining ontologies in the Mosaico SDK, every class attribute carries two pieces of information:

* **Python type** used by Pydantic for validation and IDE support.
* **PyArrow type** used for efficient columnar serialization to Parquet/Arrow.

`MosaicoType` and `MosaicoField` let you express both in a single annotation, providing a clean **single-source-of-truth API** for ontology field declarations.

You annotate each attribute once, and the Arrow schema is derived automatically at class-definition time by introspecting Pydantic's `model_fields`: no separate schema declaration to maintain, no risk of the two representations drifting apart.
 
Because the whole mechanism is built on top of **Pydantic model fields** via `Annotated` metadata, extending or customising field behaviour is as simple as adding standard Pydantic `Field` kwargs: no subclassing, no metaclass magic, no separate schema registry to maintain.


### MosaicoType
API Reference: [`mosaicolabs.models.MosaicoType`][mosaicolabs.models.MosaicoType]

`MosaicoType` is a collection of `Annotated` type aliases. Each alias bundles the corresponding Python primitive type with its PyArrow counterpart as inline metadata, making the Arrow type immediately visible to the schema auto-builder without any additional configuration.

#### Scalar types

| Alias | Python type | Arrow type |
|---|---|---|
| `MosaicoType.uint8` | `int` | `pa.uint8()` |
| `MosaicoType.int8` | `int` | `pa.int8()` |
| `MosaicoType.uint16` | `int` | `pa.uint16()` |
| `MosaicoType.int16` | `int` | `pa.int16()` |
| `MosaicoType.uint32` | `int` | `pa.uint32()` |
| `MosaicoType.int32` | `int` | `pa.int32()` |
| `MosaicoType.uint64` | `int` | `pa.uint64()` |
| `MosaicoType.int64` | `int` | `pa.int64()` |
| `MosaicoType.float16` | `float` | `pa.float16()` |
| `MosaicoType.float32` | `float` | `pa.float32()` |
| `MosaicoType.float64` | `float` | `pa.float64()` |
| `MosaicoType.bool` | `bool` | `pa.bool_()` |
| `MosaicoType.string` | `str` | `pa.string()` |
| `MosaicoType.large_string` | `str` | `pa.large_string()` |
| `MosaicoType.binary` | `bytes` | `pa.binary()` |
| `MosaicoType.large_binary` | `bytes` | `pa.large_binary()` |

#### Explicit type definition
Using `MosaicoType` provides precise control over the underlying PyArrow schema:

```python
from mosaicolabs import MosaicoField, MosaicoType, Serializable

class MyOntology(Serializable):
    x: MosaicoType.float32
    y: Optional[MosaicoType.float32] = None

```

* `x`: defined directly, will be converted in a **not nullable** PyArrow field.
* `y`: wrapped in `Optional[...]`, will be converted in a **nullable** PyArrow field.

#### Using Fallback types
You can also use standard Python type hints.
`Mosaico` automatically maps these to specific PyArrow types.

```python
class MyOntology(Serializable):
    x: int
    y: Optional[float] = None

```

In this scenario, the types are resolved using the following **fallback mapping**:

| Python type | PyArrow equivalent |
| ---| --- |
| `int` | `pa.int64()` |
| `float` | `pa.float64()` |
| `str` | `pa.string()` |
| `bool` | `pa.bool_()` |
| `bytes` | `pa.bytes()` |

!!! note "Note"
    Just like with explicit types, using `Optional` with fallback types will correctly define the PyArrow field as **nullable**. If `Optional` is not used, the field is defined as **not nullable**.

#### List types

For list fields, `MosaicoType` exposes a `list_()` static method that wraps a scalar type, either a `MosaicoType` alias or a raw Python primitive, into the appropriate `pa.list_` Arrow type.

An optional `list_size` parameter produces a fixed-size list (`pa.list_(type, size)`), omitting it yields a variable-length list.

```python
from mosaicolabs import MosaicoField, MosaicoField, Serializable

class MyOntology(Serializable):
    # Variable-length list of float32
    scores: Optional[MosaicoType.list_(MosaicoType.float32)] = None
 
    # Fixed-size list of 3 float32 (e.g. an RGB vector)
    color: MosaicoType.list_(MosaicoType.float32, list_size=3)
 
    # Works with raw Python primitives too
    tags: MosaicoType.list_(str)

    # Works with other Pydantic models with pyarrow struct
    vec: MosaicoType.list_(Vector3d)

    # Fallback List
    vec2: List[Vector2d]
```
Using Python's built-in `list` (or `List` from `typing`) generates a **list of unfixed size** in the underlying Arrow schema.
This means:

- The list can hold **any number of elements** at runtime.
- No size constraint is enforced at the schema level.
- This is equivalent to calling `MosaicoType.list_(str)` with no `size` argument.


`MosaicoType.list_()` accepts an optional `size` parameter. When provided, it maps to an Arrow **fixed-size list** (`pa.list_(type, list_size=N)`), which enforces that every value in the column contains exactly `N` elements.

| | `list[str]` | `MosaicoType.list_(str)` | `MosaicoType.list_(str, 3)` |
|---|---|---|---|
| Arrow type | `pa.list_(pa.string())` | `pa.list_(pa.string())` | `pa.list_(pa.string(), 3)` |
| Size enforced | No | No | Yes, exactly 3 |
| Interoperable with Pydantic | Yes | Yes | Yes |
| Supports nested models | Yes | Yes | Yes |

Use `MosaicoType.list_()` with a `size` argument when:

- The list represents a **fixed-dimensional structure**, such as a vector, a coordinate
  tuple, or an RGB triplet.
- You want the Arrow schema to **statically encode the size**, enabling optimised
  columnar storage and stricter validation.
- You are working with **embedding vectors** or other ML features where dimensionality
  is always known and constant.

If you do **not** pass a `size` argument, `MosaicoType.list_(T)` and `list[T]` produce
an identical Arrow schema. The choice then becomes a matter of style or explicitness:
 
```python
tags: list[str]               # idiomatic Python - preferred for readability
tags: MosaicoType.list_(str)  # explicit Mosaico style - equivalent result
```

!!! note "Note"
    Explicit type definition and fallback types properties are hold in this case.

#### Custom Arrow types

For specialised Arrow types not covered by the built-in aliases, you can always use `MosaicoType.annotate()` method.
This utility allows you to embed a raw PyArrow type directly into your ontology while maintaining full compatibility with the Mosaico schema builder.

`MosaicoType.annotate()` is a helper designed to bridge standard Python types with specific PyArrow configurations
(like timestamp precision or timezones). It requires two arguments:

* **The Python Fallback Type**: Used for runtime validation and Python-side type hinting (e.g., int, str).
* **The PyArrow Type**: The specific pyarrow type object to be used in the schema.

```python
from mosaicolabs import Serializable, MosaicoField
class MyOntology(Serializable):
    ts: MosaicoType.annotate(int, pa.timestamp("us", tz="UTC")) = MosaicoField(
        description="UTC timestamp.")
```

!!! info "Migration Note"
    Using `MosaicoType.annotate(int, ...)` is functionally equivalent to the standard `Annotated[int, ...]`, but it provides a more explicit and optimized path for the Mosaico schema builder to resolve complex Arrow types.

### MosaicoField
API Reference: [`mosaicolabs.models.types.MosaicoField`][mosaicolabs.models.types.MosaicoField]

`MosaicoField` is a factory function that returns a standard Pydantic `Field` instance, adding Mosaico-specific semantics on top of the native `pydantic.Field`. Because the return type is a native Pydantic `Field`, every standard Pydantic feature like validators, aliases, `model_fields` introspection,  works out of the box.

#### Basic usage

```python
from mosaicolabs import MosaicoType, MosaicoField, Serializable

class MyPointOntology(Serializable):
    x:     MosaicoType.float32 = MosaicoField(description="X coordinate")
    y:     MosaicoType.float32 = MosaicoField(description="Y coordinate")
    label: Optional[MosaicoType.string] = MosaicoField(
        default=None, description="Point label")
    score: MosaicoType.float32 = MosaicoField(nullable=True)
```

With `MosaicoField` you can define the `default` value of your attribute, the `nullable` attribute of pyarrow field and also a `description`.
In particular you can omit `nullable` if your `default = None`, in this case `nullable` will be set to `True` automatically.

### Nullability and Parquet V2

The `nullable` flag in `MosaicoField` controls whether the Arrow schema emits the field
as nullable. The default is `False`, fields are non-nullable unless explicitly stated
otherwise.

The distinction matters most when a reusable struct ontology is embedded inside a parent
ontology as an optional field. Consider a `Quaternion`: its individual components
(`x`, `y`, `z`, `w`) are logically required — a quaternion with missing components is
meaningless and cannot be constructed.

However, all four leaf fields must be declared as nullable due to how ParquetV2 handles
`null` optional columns during data reading. Consider a parent class such as `IMU`,
where `orientation` is declared as `Optional[Quaternion]`. If that column is `null` in
the Parquet file but the inner fields are **not** nullable in the schema, ParquetV2
cannot represent the absent struct correctly and instead reconstructs it as a
zero-initialised instance:

```python
# wrong — should be None
orientation = Quaternion(x=0, y=0, z=0, w=0)
```

Declaring all leaf fields as nullable prevents this silent corruption: a fully-null
struct is preserved as `None` through the read/write round-trip, matching the original
intent of the `Optional` annotation.


```python
class Quaternion(Serializable):
    x: MosaicoType.float32 = MosaicoField(description="X component", nullable=True)
    y: MosaicoType.float32 = MosaicoField(description="Y component", nullable=True)
    z: MosaicoType.float32 = MosaicoField(description="Z component", nullable=True)
    w: MosaicoType.float32 = MosaicoField(description="W component", nullable=True)
```

A parent ontology may choose to include the quaternion as an optional field — for
example, a detection without orientation data is still valid. In that case the *struct*
itself must also be nullable in the Arrow schema:

```python
class DetectionOntology(Serializable):
    position: MosaicoType.float32 = MosaicoField(description="Position")
 
    # The quaternion struct as a whole is optional in this ontology,
    # but its internal fields remain required when it is present.
    orientation: Optional[QuaternionOntology] = MosaicoField(nullable=True, default=None)
```

## Architecture

The ontology architecture relies on three primary abstractions: the **Factory** (`Serializable`), the **Envelope** (`Message`) and the **Mixins**

### `Serializable` (The Factory)
API Reference: [`mosaicolabs.models.Serializable`][mosaicolabs.models.Serializable]

Every data payload in Mosaico inherits from the `Serializable` class. It manages the global registry of data types and ensures that the system knows exactly how to convert a string tag like `"imu"` back into a Python class with a specific binary schema.
`Serializable` uses the `__pydantic_init_subclass__` hook, which is automatically called whenever a developer defines a new subclass.

```python
class MyCustomSensor(Serializable):  # <--- __pydantic_init_subclass__ triggers here
    ...
```
When this happens, `Serializable` performs the following steps automatically:

1.  **Generate the schema:** Introspect `model_fields` to extract the PyArrow type embedded in each field's `Annotated` metadata via `MosaicoType` aliases or raw `Annotated[T, pa.SomeType()]` annotations and build the `__msco_pyarrow_struct__` automatically. 
2.  **Generates Tag:** If the class doesn't define `__ontology_tag__`, it auto-generates one from the class name (e.g., `MyCustomSensor` -> `"my_custom_sensor"`).
3.  **Registers Class:** It adds the new class to the global types registry.
4.  **Injects Query Proxy:** It dynamically adds a `.Q` attribute to the class, enabling the fluent query syntax (e.g., `MyCustomSensor.Q.voltage > 12.0`).

### `Message` (The Envelope)

API Reference: [`mosaicolabs.models.Message`][mosaicolabs.models.Message]

The **`Message`** class is the universal transport envelope for all data within the Mosaico platform. It acts as the "Source of Truth" for synchronization and spatial context, combining specific sensor data (the payload) with critical middleware-level metadata. By centralizing metadata at the envelope level, Mosaico ensures that every data point—regardless of its complexity—carries a consistent temporal and spatial identity.

```python
from mosaicolabs import Message, Time, Temperature

# Create a Temperature message with unified envelope metadata
meas_time = Time.now()

temp_msg = Message(
    timestamp_ns=meas_time.to_nanoseconds(),  # Primary synchronization clock
    frame_id="comp_case",                     # Spatial reference frame
    seq_id=101,                               # Optional sequence ID for ordering
    data=Temperature.from_celsius(
        value=57,
        variance=0.03
    )
)

```

While logically a `Message` contains a `data` object, the physical representation on the wire (PyArrow/Parquet) is **flattened**, 
ensuring zero-overhead access to nested data during queries while maintaining a clean, object-oriented API in Python.

* **Logical:** `Message(timestamp_ns=123, frame_id="map", data=IMU(acceleration=Vector3d(x=1.0,...)))`
* **Physical:** `Struct(timestamp_ns=123, frame_id="map", seq_id=null, acceleration, ...)`

The `Message` mechanism enables a flexible dual-usage pattern for every Mosaico ontology type, supporting both **Standalone Messages** and **Embedded Fields**.

#### Standalone Messages

Any `Serializable` type (from elementary types like `String` and `Float32` to complex sensors like `IMU`) can be used as a standalone message. When assigned to the `data` field of a `Message` envelope, the type represents an independent data stream with its own global timestamp and metadata, that can be pushed via a dedicated [`TopicWriter`][mosaicolabs.handlers.TopicWriter].

This is ideal for pushing processed signals, debug values, or simple sensor readings.

```python
# Sending a raw Vector3d as a timestamped standalone message with its own uncertainty
accel_msg = Message(
    timestamp_ns=ts,
    frame_id="base_link",
    data=Vector3d(
        x=0.0, 
        y=0.0, 
        z=9.81,
        covariance=[0.01, 0, 0, 0, 0.01, 0, 0, 0, 0.01]  # 3x3 Diagonal matrix
    )
)

accel_writer.push(message=accel_msg)

# Sending a raw String as a timestamped standalone message
log_msg = Message(
    timestamp_ns=ts,
    frame_id="base_link",
    data=String(data="Waypoint-miss in navigation detected!")
)

log_writer.push(message=log_msg)
```

#### Embedded Fields

`Serializable` types can also be embedded as internal fields within a larger structure. In this context, they behave as standard data types. While the parent `Message` provides the global temporal context, the embedded fields can carry their own granular attributes, such as unique uncertainty matrices.

```python
# Embedding Vector3d inside a complex IMU model
imu_payload = IMU(
    # Embedded Field 1: Acceleration with its own specific uncertainty
    # Here the Vector3d instance inherits the timestamp and frame_id
    # from the parent IMU Message.
    acceleration=Vector3d(
        x=0.5, y=-0.2, z=9.8,
        covariance=[0.1, 0, 0, 0, 0.1, 0, 0, 0, 0.1]
    ),
    # Embedded Field 2: Angular Velocity
    angular_velocity=Vector3d(x=0.0, y=0.0, z=0.0)
)

# Wrap the complex payload in the Message envelope
imu_writer.push(Message(timestamp_ns=ts, frame_id="imu_link", data=imu_payload))

```

### Mixins: Uncertainty & Robustness

Mosaico uses **Mixins** to inject standard uncertainty fields across different data types, ensuring a consistent interface for sensor fusion and error analysis. These fields are typically used to represent the precision of the sensor data.

#### `CovarianceMixin`

API Reference: [`mosaicolabs.models.mixins.CovarianceMixin`][mosaicolabs.models.mixins.CovarianceMixin]

Injects multidimensional uncertainty fields, typically used for flattened covariance matrices (e.g., 3x3 or 6x6) in sensor fusion applications.

```python
class MySensor(Serializable, CovarianceMixin):
    # Automatically receives covariance and covariance_type fields
    ...

```

#### `VarianceMixin`

API Reference: [`mosaicolabs.models.mixins.VarianceMixin`][mosaicolabs.models.mixins.VarianceMixin]

Injects monodimensional uncertainty fields, useful for sensors with 1-dimensional uncertain data like `Temperature` or `Pressure`.

```python
class MySensor(Serializable, VarianceMixin):
    # Automatically receives variance and variance_type fields
    ...

```

By leveraging these mixins, the platform can perform deep analysis on data quality—such as filtering for only "high-confidence" segments—without requiring unique logic for every sensor type.

### Extending with Mixins

One of the most powerful consequences of building on top of Pydantic model fields is how natural **mixin composition** becomes. Because every field, including its Arrow type metadata, lives in `model_fields`, you can split concerns into focused mixin classes and combine them freely without any additional registration or schema merging step.

```python
from mosaicolabs import BaseModel, MosaicoType, MosaicoField, Serializable


class GeometryMixin(BaseModel):
    x: MosaicoType.float32 = MosaicoField(description="X coordinate")
    y: MosaicoType.float32 = MosaicoField(description="Y coordinate")
    z: MosaicoType.float32 = MosaicoField(description="Z coordinate")


class ConfidenceMixin(BaseModel):
    confidence: MosaicoType.float32 = MosaicoField(description="Detection score [0, 1]")

class MetadataMixin(BaseModel):
    label:     Optional[MosaicoType.string] = MosaicoField(default=None, nullable=True)
    sensor_id: MosaicoType.string = MosaicoField(description="Source sensor identifier")
    ts: Annotated[int, pa.timestamp("us", tz="UTC")]

# Combine mixins, the Arrow schema aggregates all fields automatically
class DetectionOntology(Serializable, GeometryMixin, ConfidenceMixin, MetadataMixin):
    pass
```

When `DetectionOntology` is defined, `__pydantic_init_subclass__` calls `_build_ontology_struct`, which walks the full `model_fields` MRO chain, extracts the PyArrow metadata from each `Annotated` annotation, and produces a single consolidated `pa.struct`, no extra code required.

This makes ontology composition **additive by default**: add a mixin to inherit its fields, remove it to drop them. The schema stays consistent with zero boilerplate.


## Querying Data Ontology with the Query (`.Q`) Proxy

The Mosaico SDK allows you to perform deep discovery directly on the physical content of your sensor streams. Every class inheriting from [`Serializable`][mosaicolabs.models.Serializable], including standard sensors, geometric primitives, and custom user models, is automatically injected with a static **`.Q` proxy** attribute.

This proxy acts as a type-safe bridge between your Python data models and the platform's search engine, enabling you to construct complex filters using standard Python dot notation.

### How the Proxy Works

The `.Q` proxy recursively inspects the model’s schema to expose every queryable field path. It identifies the data type of each field and provides only the operators valid for that type (e.g., numeric comparisons for acceleration, substring matches for frame IDs).

* **Direct Field Access**: Filter based on primary values, such as `Temperature.Q.value.gt(25.0)`.
* **Nested Navigation**: Traverse complex, embedded structures. For example, in the [`GPS`][mosaicolabs.models.sensors.GPS] model, you can drill down into the status sub-field: `GPS.Q.status.satellites.geq(8)`.
* **Mixin Integration**: Fields inherited from mixins are automatically included in the proxy. This allows you to query uncertainty metrics (from `VarianceMixin` or `CovarianceMixin`) across any model.

### Queryability Examples

The following table illustrates how the proxy flattens complex hierarchies into queryable paths:

| Type Field Path | Proxy Field Path | Source Type | Queryable Type | Supported Operators |
| --- | --- | --- | --- | --- |
| `IMU.acceleration.x` | `IMU.Q.acceleration.x` | `float` | **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
| `GPS.status.hdop` | `GPS.Q.status.hdop` | `float` | **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
| `IMU.frame_id` | `IMU.Q.frame_id` | `str` | **String** | `.eq()`, `.neq()`, `.match()`, `.in_()` |
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

* **`CovarianceMixin`**: Used for data including measurement uncertainty, standardizing the storage of covariance matrices.

### 2. Define the Wire Schema

Annotate each field with a `MosaicoType` alias and wrap it with `MosaicoField`. Fields and schema are declared together in a single annotation — the `__msco_pyarrow_struct__` is derived automatically from `model_fields` at class-definition time, so there is no separate schema declaration to maintain.

#### 2.1 Serialization Format Optimization
API Reference: [`mosaicolabs.enum.SerializationFormat`][mosaicolabs.enum.SerializationFormat]

You can optimize remote server performance by overriding the `__serialization_format__` attribute. This controls how the server compresses and organizes your data.


| Format | Identifier | Use Case Recommendation |
| --- | --- | --- |
| **Default** | `"default"` | **Standard Table**: Fixed-width data with a constant number of fields. |
| **Ragged** | `"ragged"` | **Variable Length**: Best for lists, sequences, or point clouds. |
| **Image** | `"image"` | **Blobs**: Raw or compressed images requiring specialized codec handling. |

If not explicitly set, the system defaults to `Default` format.

### Customization Example: `EnvironmentSensor`

This example demonstrates a custom sensor for environmental monitoring that tracks temperature, humidity, and pressure.

```python
# file: custom_ontology.py

from typing import Optional
import pyarrow as pa
from mosaicolabs.models import MosaicoField, MosaicoType, Serializable

class EnvironmentSensor(Serializable):
    """
    Custom sensor reading for Temperature, Humidity, and Pressure.
    """

    # --- 1. Define the Wire Schema ---
    temperature: MosaicoType.float32
    
    humidity: Optional[MosaicoType.float32] = MosaicoField(
            default= None, nullable=True)
    
    pressure: Optional[MosaicoType.float32] = MosaicoField(
            default= None, nullable=True)


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

