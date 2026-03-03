"""
Mixins Module.

This module provides helper classes used to inject standard
fields (covariance and variance) into ontology models via composition.
"""

from typing import List, Optional

import pyarrow as pa

from .base_model import BaseModel

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
        The `<Model>` placeholder adapts based on how the `CovarianceMixin` is integrated into your data structure:

        * **Direct Inheritance**: Represents any class (e.g., [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d],
            [`Quaternion`][mosaicolabs.models.data.geometry.Quaternion]) that inherits directly from `CovarianceMixin`.
        * **Composition (Nested Fields)**: When a complex model (like [`IMU`][mosaicolabs.models.sensors.IMU]) contains fields that are themselves covariance-aware,
            the proxy allows you to "drill down" to that specific attribute. For example, since [`IMU.acceleration`][mosaicolabs.models.sensors.IMU.acceleration] is a `Vector3d`,
            you access its covariance type via `IMU.Q.acceleration.covariance_type`.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            # `FROM_CALIBRATED_PROCEDURE` is some enum value defined by the user
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.covariance_type.eq(FROM_CALIBRATED_PROCEDURE))
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

    The `covariance_type` field is fully queryable via the **`.Q` Proxy**. The `<Model>` placeholder
    in the path represents any Mosaico class that exposes covariance information, either directly or through its internal fields.

    | Field Access Path | Queryable Type | Supported Operators |
    | --- | --- | --- |
    | `<Model>.Q.covariance_type` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    The `<Model>` placeholder adapts based on how the `CovarianceMixin` is integrated into your data structure:

    * **Direct Inheritance**: Represents any class (e.g., [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d], 
        [`Quaternion`][mosaicolabs.models.data.geometry.Quaternion]) that inherits directly from `CovarianceMixin`.
    * **Composition (Nested Fields)**: When a complex model (like [`IMU`][mosaicolabs.models.sensors.IMU]) contains fields that are themselves covariance-aware, 
        the proxy allows you to "drill down" to that specific attribute. For example, since [`IMU.acceleration`][mosaicolabs.models.sensors.IMU.acceleration] is a `Vector3d`, 
        you access its covariance type via `IMU.Q.acceleration.covariance_type`.

    Example: Filtering by Calibration Type
        This example demonstrates searching for data segments where the acceleration was derived from a specific calibrated procedure.

        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        # Assume FROM_CALIBRATED_PROCEDURE is a user-defined integer constant
        with MosaicoClient.connect("localhost", 6726) as client:
            # Target the covariance_type nested within the acceleration field
            qbuilder = QueryOntologyCatalog(
                IMU.Q.acceleration.covariance_type.eq(FROM_CALIBRATED_PROCEDURE)
            )

            results = client.query(qbuilder)

            if results:
                for item in results:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Matching Topics: {[topic.name for topic in item.topics]}")

        ```
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
        The `<Model>` placeholder adapts based on how the `VarianceMixin` is integrated into the data structure:

        * **Direct Inheritance**: Used for classes like [`Pressure`][mosaicolabs.models.sensors.Pressure] or [`Temperature`][mosaicolabs.models.sensors.Temperature] that inherit directly from `VarianceMixin` to represent 1D uncertainty.
        * **Composition (Nested Access)**: If a complex model contains a field that is a subclass of `VarianceMixin`, the proxy allows you to traverse the hierarchy to that specific attribute.


    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pressure, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter Pressure data by a specific variance value
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.variance.lt(0.76))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter Pressure data by a specific variance type
            # `FROM_CALIBRATED_PROCEDURE` is some enum value defined by the user
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.variance_type.eq(FROM_CALIBRATED_PROCEDURE))
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
    The `variance` field is queryable with the **`.Q` Proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.variance` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    The `<Model>` placeholder adapts based on how the `VarianceMixin` is integrated into the data structure:

    * **Direct Inheritance**: Used for classes like [`Pressure`][mosaicolabs.models.sensors.Pressure] or [`Temperature`][mosaicolabs.models.sensors.Temperature] that inherit directly from `VarianceMixin` to represent 1D uncertainty.
    * **Composition (Nested Access)**: If a complex model contains a field that is a subclass of `VarianceMixin`, the proxy allows you to traverse the hierarchy to that specific attribute.

    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pressure, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter Pressure data by a specific acquisition second
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.variance.lt(0.76))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    variance_type: Optional[int] = None
    """
    Optional 16-bit integer representing the variance parameterization.

    This field is injected into the model via composition, ensuring that sensor data is 
    paired with the optional covariance type attribute.

    ### Querying with the **`.Q` Proxy**

    The `variance_type` field is fully queryable via the **`.Q` Proxy**. 
    
    | Field Access Path | Queryable Type | Supported Operators |
    | --- | --- | --- |
    | `<Model>.Q.variance_type` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    The `<Model>` placeholder adapts based on how the `VarianceMixin` is integrated into the data structure:

    * **Direct Inheritance**: Used for classes like [`Pressure`][mosaicolabs.models.sensors.Pressure] or [`Temperature`][mosaicolabs.models.sensors.Temperature] that inherit directly from `VarianceMixin` to represent 1D uncertainty.
    * **Composition (Nested Access)**: If a complex model contains a field that is a subclass of `VarianceMixin`, the proxy allows you to traverse the hierarchy to that specific attribute.

    Example: Filtering by Precision
        The following examples demonstrate how to filter sensor data based on the magnitude of the variance or the specific procedure used to calculate it.

        ```python
        from mosaicolabs import MosaicoClient, Pressure, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter by variance threshold
            results = client.query(QueryOntologyCatalog(
                Pressure.Q.variance.lt(0.76)
            ))

            if results:
                for item in results:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

        ```
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
