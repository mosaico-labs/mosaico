"""
Mixins Module.

This module provides helper classes used to inject standard
fields (header, covariance and variance) into ontology models via composition.
"""

from typing import List, Optional
import pyarrow as pa

from .base_model import BaseModel
from .header import Header

# ---- HeaderMixin ----


class HeaderMixin(BaseModel):
    """
    A mixin that injects a standard `header` field into any inheriting ontology model.

    The `HeaderMixin` is used to add standard metadata (such as acquisition timestamps
    or frame IDs) to a sensor model through composition. It ensures that the
    underlying PyArrow schema remains synchronized with the Pydantic data model.

    ### Dynamic Schema Injection
    This mixin uses the `__init_subclass__` hook to perform a **Schema Append** operation:

    1. It inspects the child class's existing `__msco_pyarrow_struct__`.
    2. It appends a `header` field of type [`Header`][mosaicolabs.models.Header].
    3. It reconstructs the final `pa.struct` for the class.

    Important: Collision Safety
        The mixin performs a collision check during class definition. If the child
        class already defines a `header` field in its PyArrow struct, a `ValueError`
        will be raised to prevent schema corruption.

    Attributes:
        header: An optional [`Header`][mosaicolabs.models.Header] object containing standard metadata.

    ### Querying with the **`.Q` Proxy** {: #queryability }
    When constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog],
    the `header` component is fully queryable across any model inheriting from this mixin.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.header.seq` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `<Model>.Q.header.stamp.sec` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `<Model>.Q.header.stamp.nanosec` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `<Model>.Q.header.frame_id` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico ontology class (e.g., `IMU`, `GPS`, `Floating64`)
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, Floating64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.header.stamp.sec.lt(1770282868))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter primitive Floating64 telemetry by frame identifier
            qresponse = client.query(
                QueryOntologyCatalog(Floating64.Q.header.frame_id.eq("robot_base"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    header: Optional[Header] = None
    """
    An optional metadata header providing temporal and spatial context to the ontology model.

    This field is injected into the model via composition, ensuring that sensor data is 
    paired with standard acquisition attributes like sequence IDs and high-precision 
    timestamps.

    ### Querying with the **`.Q` Proxy**
    Check the documentation of the [`HeaderMixin`][mosaicolabs.models.HeaderMixin--queryability] to construct a valid expression for the 
    [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder involving the `header` component.
    """

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include 'header'.

        This method is triggered at class definition time.

        Raises:
            ValueError: If a field named 'header' already exists in the child's schema.
        """
        super().__init_subclass__(**kwargs)

        # Define the PyArrow field definition for the header
        _HEADER_FIELD = pa.field(
            "header",
            Header.__msco_pyarrow_struct__,
            nullable=True,
            metadata={"description": "The standard metadata header (optional)."},
        )

        # Retrieve existing schema fields from the child class
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "header" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'header' schema key."
            )

        # Append and Update
        new_fields = current_pa_fields + [_HEADER_FIELD]
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)


# ---- CovarianceMixin ----


class CovarianceMixin(BaseModel):
    """
    A mixin that adds uncertainty fields (`covariance` and `covariance_type`) to data models.

    This is particularly useful for complex sensors like IMUs, Odometry, or GNSS
    receivers that provide multidimensional uncertainty matrices along with
    their primary measurements.

    ### Dynamic Schema Injection
    This mixin uses the `__init_subclass__` hook to perform a **Schema Append** operation:

    1. It inspects the child class's existing `__msco_pyarrow_struct__`.
    2. It appends a `covariance` and `covariance_type` fields.
    3. It reconstructs the final `pa.struct` for the class.

    Important: Collision Safety
        The mixin performs a collision check during class definition. If the child
        class already defines a `covariance` or `covariance_type` field in its PyArrow struct, a `ValueError`
        will be raised to prevent schema corruption.

    Attributes:
        covariance: Optional list of 64-bit floats representing the flattened matrix.
        covariance_type: Optional 16-bit integer representing the covariance enum.

    ### Querying with the **`.Q` Proxy** {: #queryability }
    When constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog],
    the class fields are queryable across any model inheriting from this mixin, according to the following table:

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.covariance_type` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `<Model>.Q.covariance` | ***Non-Queryable*** | None |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico ontology class (e.g., `IMU`, `GPS`, `Floating64`)
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from `HeaderMixin`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            # `FROM_CALIBRATED_PROCEDURE` is some enum value defined by the user
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.covariance_type.eq(FROM_CALIBRATED_PROCEDURE))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    covariance: Optional[List[float]] = None
    """
    Optional list of 64-bit floats representing the flattened matrix.
    
    ### Querying with the **`.Q` Proxy**

    Note: Non-Queryable
        The field is not queryable with the **`.Q` Proxy**.
    """

    covariance_type: Optional[int] = None
    """
    Optional 16-bit integer representing the covariance enum.

    This field is injected into the model via composition, ensuring that sensor data is 
    paired with the optional covariance type attribute.

    ### Querying with the **`.Q` Proxy**
    Check the documentation of the [`CovarianceMixin`][mosaicolabs.models.CovarianceMixin--queryability] to construct a valid expression for the 
    [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder involving the `covariance_type` component.
    """

    def __init_subclass__(cls, **kwargs):
        """
        Dynamically appends covariance-related fields to the child class's PyArrow struct.

        Raises:
            ValueError: If 'covariance' or 'covariance_type' keys collide with existing fields.
        """
        super().__init_subclass__(**kwargs)

        # Define the fields to inject
        _FIELDS = [
            pa.field(
                "covariance",
                pa.list_(value_type=pa.float64()),
                nullable=True,
                metadata={
                    "description": "The covariance matrix (flattened) of the data."
                },
            ),
            pa.field(
                "covariance_type",
                pa.int16(),
                nullable=True,
                metadata={
                    "description": "Enum integer representing the covariance parameterization."
                },
            ),
        ]

        # Retrieve existing schema fields
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "covariance" in existing_pa_names or "covariance_type" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'covariance' or 'covariance_type' schema keys."
            )

        # Append and Update
        new_fields = current_pa_fields + _FIELDS
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)


# ---- VarianceMixin ----


class VarianceMixin(BaseModel):
    """
    A mixin that adds 1-dimensional uncertainty fields (`variance` and `variance_type`).

    Recommended for sensors with scalar uncertain outputs, such as ultrasonic
    rangefinders, temperature sensors, or individual encoders.

    ### Dynamic Schema Injection
    This mixin uses the `__init_subclass__` hook to perform a **Schema Append** operation:

    1. It inspects the child class's existing `__msco_pyarrow_struct__`.
    2. It appends a `variance` and `variance_type` field.
    3. It reconstructs the final `pa.struct` for the class.

    Important: Collision Safety
        The mixin performs a collision check during class definition. If the child
        class already defines a `variance` or `variance_type` field in its PyArrow struct, a `ValueError`
        will be raised to prevent schema corruption.

    ### Querying with the **`.Q` Proxy** {: #queryability }
    When constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog],
    the class fields are queryable across any model inheriting from this mixin, according to the following table:

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.variance` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `<Model>.Q.variance_type` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico ontology class (e.g., `IMU`, `GPS`, `Floating64`)
        or any custom user-defined [`Serializable`][mosaicolabs.models.Serializable] class that inherits
        from [`HeaderMixin`][mosaicolabs.models.HeaderMixin].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.variance.lt(0.76))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter IMU data by a specific acquisition second
            # `FROM_CALIBRATED_PROCEDURE` is some enum value defined by the user
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.variance_type.eq(FROM_CALIBRATED_PROCEDURE))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    variance: Optional[float] = None
    """
    Optional 64-bit float representing the variance of the data.

    This field is injected into the model via composition, ensuring that sensor data is 
    paired with the optional variance attribute.

    ### Querying with the **`.Q` Proxy**
    Check the documentation of the [`VarianceMixin`][mosaicolabs.models.VarianceMixin--queryability] to construct a valid expression for the 
    [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder involving the `variance` component.
    """

    variance_type: Optional[int] = None
    """
    Optional 16-bit integer representing the variance parameterization.

    This field is injected into the model via composition, ensuring that sensor data is 
    paired with the optional covariance type attribute.

    ### Querying with the **`.Q` Proxy**
    Check the documentation of the [`VarianceMixin`][mosaicolabs.models.VarianceMixin--queryability] to construct a valid expression for the 
    [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder involving the `variance_type` component.
    """

    def __init_subclass__(cls, **kwargs):
        """
        Dynamically appends variance-related fields to the child class's PyArrow struct.

        Raises:
            ValueError: If 'variance' or 'variance_type' keys collide with existing fields.
        """
        super().__init_subclass__(**kwargs)

        # Define the fields to inject
        _FIELDS = [
            pa.field(
                "variance",
                pa.float64(),
                nullable=True,
                metadata={"description": "The variance of the data."},
            ),
            pa.field(
                "variance_type",
                pa.int16(),
                nullable=True,
                metadata={
                    "description": "Enum integer representing the variance parameterization."
                },
            ),
        ]

        # Retrieve existing schema fields
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "variance" in existing_pa_names or "variance_type" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'variance' or 'variance_type' schema keys."
            )

        # Append and Update
        new_fields = current_pa_fields + _FIELDS
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)
