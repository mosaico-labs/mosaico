"""
This module defines the fundamental building blocks for spatial representation, including vectors, points, quaternions, and rigid-body transforms.

The module follows a **Two-Tier Architecture** to optimize both internal efficiency and public usability:

* **Internal Structs (`_Struct`)**: Pure data containers that define the physical memory layout and the PyArrow schema. These are intended for embedding within larger composite objects (like a `Pose` or `Transform`) to avoid attaching redundant metadata headers or timestamps to every inner field.
* **Public Classes**: High-level models that combine spatial data with Mosaico's transport and serialization logic. These inherit from the internal structs and inject support for auto-registration ([`Serializable`][mosaicolabs.models.serializable.Serializable]), temporal/spatial context ([`HeaderMixin`][mosaicolabs.models.mixins.HeaderMixin]), and uncertainty tracking ([`CovarianceMixin`][mosaicolabs.models.mixins.CovarianceMixin]).
"""

from typing import Optional
import pyarrow as pa

from ..base_model import BaseModel
from ..serializable import Serializable
from ..mixins import HeaderMixin, CovarianceMixin


# ---------------------------------------------------------------------------
# Vector STRUCT classes
# ---------------------------------------------------------------------------


class _Vector2dStruct(BaseModel):
    """
    The internal data layout for 2D spatial vectors.

    This class serves as the schema definition for (x, y) coordinates.

    Note: Nullability Handling
        All fields are explicitly marked as `nullable=True` in the PyArrow schema.
        This ensures that empty fields are correctly deserialized as `None` rather than
        incorrectly being default-initialized to $0$ by Parquet readers.
    """

    # OPTIONALITY NOTE
    # All fields are explicitly set to `nullable=True`. This prevents Parquet V2
    # readers from incorrectly deserializing a `None` _Vector2dStruct field in a class
    # as a default-initialized object (e.g., getting _Vector2dStruct(0, ...) instead of None).
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector y component"},
            ),
        ]
    )

    x: float
    """
    The Vector X component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector2dStruct` (i.e. [`Vector2d`][mosaicolabs.models.data.Vector2d], [`Point2d`][mosaicolabs.models.data.Point2d])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector2d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector2d.Q.x.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector2d.Q.x.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Point2d.Q.x.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    y: float
    """
    The Vector Y component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector2dStruct` (i.e. [`Vector2d`][mosaicolabs.models.data.Vector2d], [`Point2d`][mosaicolabs.models.data.Point2d])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector2d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector2d.Q.y.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector2d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Point2d.Q.y.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Creates a struct instance from a raw list.

        Args:
            data: A list containing exactly 2 float values: [x, y].

        Raises:
            ValueError: If the input list does not have a length of 2.
        """
        if len(data) != 2:
            raise ValueError("expected 2 values")
        return cls(x=data[0], y=data[1])


class _Vector3dStruct(BaseModel):
    """
    The internal data layout for 3D spatial vectors.

    This class serves as the schema definition for (x, y, z) coordinates.
    It is used as a nested component in composite models like [`Pose`][mosaicolabs.models.data.geometry.Pose]
    or [`Transform`][mosaicolabs.models.data.geometry.Transform].

    Note: Nullability Handling
        All fields are explicitly marked as `nullable=True` in the PyArrow schema.
        This ensures that empty fields are correctly deserialized as `None` rather than
        incorrectly being default-initialized to $0$ by Parquet readers.
    """

    # OPTIONALITY NOTE
    # All fields are explicitly set to `nullable=True`. This prevents Parquet V2
    # readers from incorrectly deserializing a `None` _Vector3dStruct field in a class
    # as a default-initialized object (e.g., getting _Vector3dStruct(0, ...) instead of None).
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector y component"},
            ),
            pa.field(
                "z",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector z component"},
            ),
        ]
    )

    x: float
    """
    The Vector X component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector3dStruct` (i.e. [`Vector3d`][mosaicolabs.models.data.Vector2d], [`Point3d`][mosaicolabs.models.data.Point2d])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector3d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector3d.Q.x.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector3d.Q.x.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Point3d.Q.x.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    y: float
    """
    The Vector Y component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector3dStruct` (i.e. [`Vector3d`][mosaicolabs.models.data.Vector2d], [`Point3d`][mosaicolabs.models.data.Point2d])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector3d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector3d.Q.y.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector3d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Point3d.Q.y.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    z: float
    """
    The Vector Z component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector3dStruct` (i.e. [`Vector3d`][mosaicolabs.models.data.Vector2d], [`Point3d`][mosaicolabs.models.data.Point2d])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector3d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector3d.Q.z.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector3d.Q.z.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Point3d.Q.z.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Creates a struct instance from a raw list.

        Args:
            data: A list containing exactly 3 float values: [x, y, z].

        Raises:
            ValueError: If the input list does not have a length of 3.
        """
        if len(data) != 3:
            raise ValueError("expected 3 values")
        return cls(x=data[0], y=data[1], z=data[2])


class _Vector4dStruct(BaseModel):
    """
    The internal data layout for 4D spatial vectors.

    This class serves as the schema definition for (x, y, z, w) coordinates.
    It is used as a nested component in composite models like [`Pose`][mosaicolabs.models.data.geometry.Pose]
    or [`Transform`][mosaicolabs.models.data.geometry.Transform].

    Note: Nullability Handling
        All fields are explicitly marked as `nullable=True` in the PyArrow schema.
        This ensures that empty fields are correctly deserialized as `None` rather than
        incorrectly being default-initialized to $0$ by Parquet readers.
    """

    # OPTIONALITY NOTE
    # All fields are explicitly set to `nullable=True`. This prevents Parquet V2
    # readers from incorrectly deserializing a `None` _Vector4dStruct field in a class
    # as a default-initialized object (e.g., getting _Vector4dStruct(0, ...) instead of None).
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector y component"},
            ),
            pa.field(
                "z",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector z component"},
            ),
            pa.field(
                "w",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vector w component"},
            ),
        ]
    )

    x: float
    """
    The Vector X component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector4dStruct` (i.e. [`Vector4d`][mosaicolabs.models.data.Vector4d], [`Quaternion`][mosaicolabs.models.data.Quaternion])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector4d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector4d.Q.x.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.x.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Quaternion.Q.x.leq(0.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    y: float
    """
    The Vector Y component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector4dStruct` (i.e. [`Vector4d`][mosaicolabs.models.data.Vector4d], [`Quaternion`][mosaicolabs.models.data.Quaternion])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector4d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector4d.Q.y.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Quaternion.Q.y.leq(0.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    z: float
    """
    The Vector Z component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector4dStruct` (i.e. [`Vector4d`][mosaicolabs.models.data.Vector4d], [`Quaternion`][mosaicolabs.models.data.Quaternion])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector4d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector4d.Q.z.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.z.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Quaternion.Q.z.leq(0.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    w: float
    """
    The Vector W component
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico class derived by `_Vector4dStruct` (i.e. [`Vector4d`][mosaicolabs.models.data.Vector4d], [`Quaternion`][mosaicolabs.models.data.Quaternion])
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector4d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Vector4d.Q.w.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.w.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        
            # Filter for a specific component value.
            qresponse = client.query(QueryOntologyCatalog(Quaternion.Q.w.leq(0.707)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Creates a struct instance from a raw list.

        Args:
            data: A list containing exactly 4 float values: [x, y, z, w].

        Raises:
            ValueError: If the input list does not have a length of 4.
        """
        if len(data) != 4:
            raise ValueError("expected 4 values")
        return cls(x=data[0], y=data[1], z=data[2], w=data[3])


# ---------------------------------------------------------------------------
# Public vector classes
# ---------------------------------------------------------------------------


class Vector2d(
    _Vector2dStruct,  # Inherits fields (x, y)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    A public 2D Vector for platform-wide transmission.

    This class combines the [x, y] coordinates with full Mosaico transport logic.
    It is used to represent quantities such as velocity, acceleration, or directional forces.

    Attributes:
        x: Vector X component.
        y: Vector Y component.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 2x2 covariance matrix representing
            the uncertainty of the vector measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector2d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Vector2d.Q.y.leq(123.4))
                .with_expression(Vector2d.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector2d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


class Vector3d(
    _Vector3dStruct,  # Inherits fields (x, y, z)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    A public 3D Vector for platform-wide transmission.

    This class combines the [x, y, z] coordinates with full Mosaico transport logic.
    It is used to represent quantities such as velocity, acceleration, or directional forces.

    Attributes:
        x: Vector X component.
        y: Vector Y component.
        z: Vector Z component.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 3x3 covariance matrix representing
            the uncertainty of the vector measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector3d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Vector3d.Q.y.leq(123.4))
                .with_expression(Vector3d.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector3d.Q.z.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


class Vector4d(
    _Vector4dStruct,  # Inherits fields (x, y, z, w)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    A public 4D Vector for platform-wide transmission.

    This class combines the [x, y, z, w] coordinates with full Mosaico transport logic.
    It is used to represent quantities such as velocity, acceleration, or directional forces.

    Attributes:
        x: Vector X component.
        y: Vector Y component.
        z: Vector Z component.
        w: Vector W component.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 4x4 covariance matrix representing
            the uncertainty of the vector measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Vector4d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.y.leq(123.4))
                .with_expression(Vector4d.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Vector4d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


class Point2d(
    _Vector2dStruct,  # Inherits fields (x, y)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Semantically represents a specific location (Point) in 2D space.

    Structurally identical to a 2D Vector, but distinguished within the Mosaico API
    to clarify intent in spatial operations. Use this class for
    2D coordinate data that requires Mosaico transport logic.

    Attributes:
        x: Point X coordinate.
        y: Point Y coordinate.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 2x2 covariance matrix representing
            the uncertainty of the point measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Point2d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Point2d.Q.y.leq(123.4))
                .with_expression(Point2d.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Point2d.Q.y.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


class Point3d(
    _Vector3dStruct,  # Inherits fields (x, y, z)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Semantically represents a specific location (Point) in 3D space.

    The `Point3d` class is used to instantiate a 3D coordinate message for
    transmission over the platform. It is structurally identical
    to a 3D Vector but is used to denote state rather than direction.

    Attributes:
        x: Point X coordinate.
        y: Point Y coordinate.
        z: Point Z coordinate.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 3x3 covariance matrix representing
            the uncertainty of the point measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Point3d, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Point3d.Q.y.leq(123.4))
                .with_expression(Point3d.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Point3d.Q.z.leq(123.4), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


class Quaternion(
    _Vector4dStruct,  # Inherits fields (x, y, z, w)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a rotation in 3D space using normalized quaternions.

    Structurally identical to a 4D vector [x, y, z, w], but semantically denotes
    an orientation. This representation avoids the gimbal lock
    issues associated with Euler angles.

    Attributes:
        x: Vector X component.
        y: Vector Y component.
        z: Vector Z component.
        w: Vector W component.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 4x4 covariance matrix representing
            the uncertainty of the quaternion measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Quaternion, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Quaternion.Q.w.leq(0.707))
                .with_expression(Quaternion.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Quaternion.Q.w.leq(0.707), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    pass


# ---------------------------------------------------------------------------
# Composite Structures
# ---------------------------------------------------------------------------


class Transform(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a rigid-body transformation between two coordinate frames.

    A transform consists of a translation followed by a rotation. It is
    typically used to describe the kinematic relationship between components
    (e.g., "Camera to Robot Base").

    Attributes:
        translation: A `Vector3d` describing the linear shift.
        rotation: A `Quaternion` describing the rotational shift.
        target_frame_id: The identifier of the destination coordinate frame.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 7x7 composed covariance matrix representing
            the uncertainty of the Translation+Rotation.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Transform, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific component value.
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.translation.x.gt(5.0))
                .with_expression(Transform.Q.rotation.w.lt(0.707))
                .with_expression(Transform.Q.header.stamp.sec.between([1770282868, 1770290127]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.translation.x.gt(5.0), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "translation",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D translation vector"},
            ),
            pa.field(
                "rotation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "Quaternion representing rotation."},
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=True,
                metadata={"description": "Target frame identifier."},
            ),
        ]
    )

    translation: Vector3d
    """
    The 3D translation vector component.

    ### Querying with the **`.Q` Proxy**
    Translation components are queryable through the `translation` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Transform.Q.translation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Transform.Q.translation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Transform.Q.translation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Transform, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Find transforms where the linear X-translation exceeds 5 meters
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.translation.x.gt(5.0))
                .with_expression(Transform.Q.translation.z.lt(150.3))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.translation.x.gt(5.0), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    rotation: Quaternion
    """
    The rotation quaternion component (x, y, z, w).

    ### Querying with the **`.Q` Proxy**
    Rotation components are queryable through the `rotation` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Transform.Q.rotation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Transform.Q.rotation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Transform.Q.rotation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Transform.Q.rotation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Transform, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for specific orientation states
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.rotation.w.geq(0.707))
                .with_expression(Transform.Q.rotation.z.lt(0.4))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.rotation.x.gt(5.0), include_timestamp_range=True)
                .with_expression(Transform.Q.rotation.z.lt(0.4))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    target_frame_id: Optional[str] = None
    """
    Target coordinate frame identifier.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Transform.Q.target_frame_id` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Transform, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for specific target frame id
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.target_frame_id.eq("camera_link"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Transform.Q.target_frame_id.eq("camera_link"), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """


class Pose(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents the position and orientation of an object in a global or local frame.

    While similar to a [`Transform`][mosaicolabs.models.data.geometry.Transform], a
    `Pose` semantically denotes the **state** of an object (its current location
    and heading) rather than the mathematical shift between two frames.

    Attributes:
        position: A `Point3d` representing the object's coordinates.
        orientation: A `Quaternion` representing the object's heading.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 7x7 composed covariance matrix representing
            the uncertainty of the Translation+Rotation.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pose, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter Poses with position X-component AND orientation W-component
            qresponse = client.query(
                QueryOntologyCatalog(Pose.Q.position.x.gt(5.0))
                .with_expression(Pose.Q.orientation.w.lt(0.707))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Pose.Q.position.x.gt(5.0), include_timestamp_range=True)
                .with_expression(Pose.Q.orientation.w.lt(0.707))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```

    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "position",
                Point3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D translation vector"},
            ),
            pa.field(
                "orientation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "Quaternion representing rotation."},
            ),
        ]
    )

    position: Point3d
    """
    The 3D position vector component.

    ### Querying with the **`.Q` Proxy**
    Position components are queryable through the `position` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Pose.Q.position.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Pose.Q.position.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Pose.Q.position.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pose, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find poses where the linear X-position exceeds 5 meters
            qresponse = client.query(
                QueryOntologyCatalog(Pose.Q.position.x.gt(123450.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Pose.Q.position.x.gt(5.0), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    orientation: Quaternion
    """
    The orientation quaternion component (x, y, z, w).

    ### Querying with the **`.Q` Proxy**
    Rotation components are queryable through the `orientation` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Pose.Q.orientation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Pose.Q.orientation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Pose.Q.orientation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Pose.Q.orientation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pose, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for specific orientation states
            qresponse = client.query(
                QueryOntologyCatalog(Pose.Q.orientation.w.geq(0.707))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(include_timestamp_range=True)
                .with_expression(Pose.Q.orientation.w.geq(0.707))
                .with_expression(Pose.Q.orientation.x.lt(0.1))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """
