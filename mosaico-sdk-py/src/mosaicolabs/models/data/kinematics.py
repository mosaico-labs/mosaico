"""
Kinematics Data Structures.

This module defines structures for analyzing motion:
1.  **Velocity (Twist)**: Linear and angular speed.
2.  **Acceleration**: Linear and angular acceleration.
3.  **MotionState**: A complete snapshot of an object's kinematics (Pose + Velocity + Acceleration).

These can be assigned to Message.data field to send data to the platform.
"""

from typing import Optional
import pyarrow as pa
from pydantic import model_validator

from ..serializable import Serializable
from ..mixins import HeaderMixin, CovarianceMixin
from .geometry import Pose, Vector3d


class Velocity(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents 6-Degree-of-Freedom Velocity, commonly referred to as a Twist.

    The `Velocity` class describes the instantaneous motion of an object, split into
    linear and angular components.

    Attributes:
        linear: Optional [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d] linear velocity vector.
        angular: Optional [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d] angular velocity vector.
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 3x3 covariance matrix representing
            the uncertainty of the point measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    Note: Input Validation
        A valid `Velocity` object must contain at least a `linear` or an `angular`
        component; providing neither will raise a `ValueError`.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Velocity, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter Velocities with linear X-component AND angular Z-component
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.linear.x.gt(5.0))
                    .with_expression(Velocity.Q.angular.z.lt(10))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.linear.x.gt(5.0), include_timestamp_range=True)
                .with_expression(Velocity.Q.angular.z.lt(10))
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
                "linear",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D linear velocity vector"},
            ),
            pa.field(
                "angular",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D angular velocity vector"},
            ),
        ]
    )

    linear: Optional[Vector3d] = None
    """
    3D linear velocity vector

    ### Querying with the **`.Q` Proxy**
    Linear components are queryable through the `linear` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Velocity.Q.linear.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Velocity.Q.linear.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Velocity.Q.linear.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Velocity, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find velocities where the linear X component exceeds 25 m/s
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.linear.x.gt(25.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.linear.x.gt(5.0), include_timestamp_range=True)
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

    angular: Optional[Vector3d] = None
    """
    3D angular velocity vector

    ### Querying with the **`.Q` Proxy**
    Angular components are queryable through the `angular` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Velocity.Q.angular.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Velocity.Q.angular.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Velocity.Q.angular.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Velocity, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find velocities where the angular X component exceeds 2 rad//s
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.angular.x.gt(2.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Velocity.Q.angular.x.gt(2.0), include_timestamp_range=True)
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

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Velocity":
        """
        Ensures the velocity object is not empty.

        Raises:
            ValueError: If both `linear` and `angular` are None.
        """
        if self.linear is None and self.angular is None:
            raise ValueError("User must provide at least 'linear' or 'angular'.")
        return self


class Acceleration(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents 6-Degree-of-Freedom Acceleration.

    This class provides a standardized way to transmit linear and angular
    acceleration data to the platform.

    Attributes:
        linear: Optional 3D linear acceleration vector ($a_x, a_y, a_z$).
        angular: Optional 3D angular acceleration vector ($\alpha_x, \alpha_y, \alpha_z$).
        header: Optional metadata header providing acquisition context.
        covariance: Optional flattened 3x3 covariance matrix representing
            the uncertainty of the point measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    Note: Input Validation
        Similar to the [`Velocity`][mosaicolabs.models.data.kinematics.Velocity] class, an `Acceleration` instance requires
        at least one non-null component (`linear` or `angular`).

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Acceleration, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter Accelerations with linear X-component AND angular Z-component
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.linear.x.gt(5.0))
                    .with_expression(Acceleration.Q.angular.z.lt(10))
            )
            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.linear.x.gt(5.0), include_timestamp_range=True)
                .with_expression(Acceleration.Q.angular.z.lt(10))
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
                "linear",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D linear acceleration vector"},
            ),
            pa.field(
                "angular",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D angular acceleration vector"},
            ),
        ]
    )

    linear: Optional[Vector3d] = None
    """
    3D linear acceleration vector

    ### Querying with the **`.Q` Proxy**
    Linear components are queryable through the `linear` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Acceleration.Q.linear.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Acceleration.Q.linear.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Acceleration.Q.linear.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Acceleration, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find accelerations where the linear X component exceeds 5 m/s^2
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.linear.x.gt(5.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.linear.x.gt(5.0), include_timestamp_range=True)
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

    angular: Optional[Vector3d] = None
    """
    3D angular acceleration vector

    ### Querying with the **`.Q` Proxy**
    Angular components are queryable through the `angular` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Acceleration.Q.angular.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Acceleration.Q.angular.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Acceleration.Q.angular.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Acceleration, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find accelerations where the angular X component exceeds 1 rad/s^2
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.angular.x.gt(1.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Acceleration.Q.angular.x.gt(1.0), include_timestamp_range=True)
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

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Acceleration":
        """
        Ensures the acceleration object is not empty.

        Raises:
            ValueError: If both `linear` and `angular` are None.
        """
        if self.linear is None and self.angular is None:
            raise ValueError("User must provide at least 'linear' or 'angular'.")
        return self


class MotionState(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Aggregated Kinematic State.

    `MotionState` groups [`Pose`][mosaicolabs.models.data.geometry.Pose],
    [`Velocity`][mosaicolabs.models.data.kinematics.Velocity], and optional
    [`Acceleration`][mosaicolabs.models.data.kinematics.Acceleration] into a
    single atomic update.

    This is the preferred structure for:

    * **Trajectory Tracking**: Recording the high-fidelity path of a robot or vehicle.
    * **State Estimation**: Logging the output of Kalman filters or SLAM algorithms.
    * **Ground Truth**: Storing reference data from simulation environments.

    Attributes:
        pose: The 6D pose representing current position and orientation.
        velocity: The 6D velocity (Twist).
        target_frame_id: A string identifier for the target coordinate frame.
        acceleration: Optional 6D acceleration.
        header: Standard metadata header for temporal synchronization.
        covariance: Optional flattened NxN composed covariance matrix representing
            the uncertainty of the Pose+Velocity+[Acceleration] measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MotionState, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter MotionStates with position X-component AND angular velocity Z-component
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.pose.position.x.gt(123456.9))
                    .with_expression(MotionState.Q.velocity.angular.z.lt(10))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.pose.position.x.gt(123456.9), include_timestamp_range=True)
                .with_expression(MotionState.Q.velocity.angular.z.lt(10))
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
                "pose",
                Pose.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "6D pose with optional time and covariance info."
                },
            ),
            pa.field(
                "velocity",
                Velocity.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "6D velocity with optional time and covariance info."
                },
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=False,
                metadata={"description": "Target frame identifier."},
            ),
            pa.field(
                "acceleration",
                Acceleration.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "6D acceleration with optional time and covariance info."
                },
            ),
        ]
    )

    pose: Pose
    """
    The 6D pose representing current position and orientation.

    ### Querying with the **`.Q` Proxy**
    Pose components are queryable through the `pose` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MotionState.Q.pose.position.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.position.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.position.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.orientation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.orientation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.orientation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.pose.orientation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MotionState, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter snapshots where the object is beyond a specific X-coordinate
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.pose.position.x.gt(500.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.pose.position.x.gt(500.0), include_timestamp_range=True)
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

    velocity: Velocity
    """
    The 6D velocity (Twist) describing instantaneous motion.

    ### Querying with the **`.Q` Proxy**
    Velocity components are queryable through the `velocity` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MotionState.Q.velocity.linear.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.velocity.linear.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.velocity.linear.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.velocity.angular.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.velocity.angular.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.velocity.angular.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MotionState, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find states where linear velocity in X exceeds 2.5 m/s
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.velocity.linear.x.gt(2.5))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.velocity.linear.x.gt(5.0), include_timestamp_range=True)
                .with_expression(MotionState.Q.velocity.angular.z.lt(10))
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

    target_frame_id: str
    """
    Identifier for the destination coordinate frame.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MotionState.Q.target_frame_id` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MotionState, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find states where target_frame_id is some link
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.target_frame_id.eq("moving_base"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            
        ```
    """

    acceleration: Optional[Acceleration] = None
    """
    Optional 6D acceleration components.

    ### Querying with the **`.Q` Proxy**
    Acceleration components are queryable through the `acceleration` field prefix if present.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MotionState.Q.acceleration.linear.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.acceleration.linear.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.acceleration.linear.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.acceleration.angular.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.acceleration.angular.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MotionState.Q.acceleration.angular.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, MotionState, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find states where centripetal acceleration exceeds 5 m/s^2
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.acceleration.linear.y.gt(5.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(MotionState.Q.acceleration.linear.y.gt(5.0), include_timestamp_range=True)
                .with_expression(MotionState.Q.acceleration.angular.z.lt(10))
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
