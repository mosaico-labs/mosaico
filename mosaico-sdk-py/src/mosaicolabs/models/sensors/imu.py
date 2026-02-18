"""
IMU Ontology Module.

This module defines the `IMU` model for Inertial Measurement Units.
It aggregates data from accelerometers and gyroscopes.

"""

from typing import Optional
import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable
from ..data import Quaternion, Vector3d


class IMU(Serializable, HeaderMixin):
    """
    Inertial Measurement Unit data.

    This model aggregates raw or estimated motion data from accelerometers and
    gyroscopes, providing a high-frequency snapshot of an object's inertial state.

    Attributes:
        acceleration: Linear acceleration vector [ax, ay, az] in $m/s^2$.
        angular_velocity: Angular velocity vector [wx, wy, wz] in $rad/s$.
        orientation: Optional estimated orientation expressed as a quaternion.
        header: Standard metadata providing temporal and spatial reference.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter IMU data based
    on physical thresholds or metadata within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find high-acceleration events (e.g., impacts) on the X-axis
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.x.gt(15.0))
                .with_expression(IMU.Q.angular_velocity.z.gt(1.0)),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.x.gt(15.0), include_timestamp_range=True)
                .with_expression(IMU.Q.angular_velocity.z.gt(1.0)),
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

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "acceleration",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Linear acceleration vector [ax, ay, az] in m/s^2."
                },
            ),
            pa.field(
                "angular_velocity",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Angular velocity vector [wx, wy, wz] in rad/s."
                },
            ),
            pa.field(
                "orientation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Estimated orientation [qx, qy, qz, qw] (optional)."
                },
            ),
        ]
    )

    acceleration: Vector3d
    """
    Linear acceleration component.

    ### Querying with the **`.Q` Proxy**
    Acceleration components are queryable through the `acceleration` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `IMU.Q.acceleration.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.acceleration.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.acceleration.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for high-impact events
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.z.gt(19.6))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.z.gt(19.6), include_timestamp_range=True)
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

    angular_velocity: Vector3d
    """
    Angular velocity component.

    ### Querying with the **`.Q` Proxy**
    Angular velocities components are queryable through the `angular_velocity` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `IMU.Q.angular_velocity.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.angular_velocity.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.angular_velocity.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for high-turns events
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.angular_velocity.z.gt(1.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.angular_velocity.z.gt(1.0), include_timestamp_range=True)
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

    orientation: Optional[Quaternion] = None
    """
    Estimated orientation [qx, qy, qz, qw] (optional).
        
    ### Querying with the **`.Q` Proxy**
    Estimated orientation components are queryable through the `orientation` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `IMU.Q.orientation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.orientation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.orientation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `IMU.Q.orientation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for orientation component values
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.orientation.z.gt(0.707))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.orientation.z.gt(0.707), include_timestamp_range=True)
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
