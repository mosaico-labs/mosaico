"""
Geometry Messages Adaptation Module.

This module provides specialized adapters for translating ROS `geometry_msgs` into the
standardized Mosaico Ontology. It implements recursive unwrapping to handle common
ROS patterns, such as "Stamped" envelopes and covariance wrappers, ensuring that
spatial data is normalized before ingestion.
"""

from typing import Any, Optional, Tuple, Type
from mosaicolabs.models.data import (
    Point3d,
    Pose,
    Quaternion,
    Transform,
    Vector3d,
    ForceTorque,
    Acceleration,
    Velocity,
)
from mosaicolabs.models import Message

from ..adapter_base import ROSAdapterBase
from ..ros_message import ROSMessage
from ..ros_bridge import register_adapter

from .helpers import _make_header, _validate_msgdata


@register_adapter
class PoseAdapter(ROSAdapterBase[Pose]):
    """
    Adapter for translating ROS Pose-related messages to Mosaico `Pose`.

    This adapter follows the "Adaptation, Not Just Parsing" philosophy by actively
    unwrapping nested ROS structures and normalizing them into strongly-typed
    Mosaico `Pose` objects.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Pose`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Pose.html)
    - [`geometry_msgs/msg/PoseStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseStamped.html)
    - [`geometry_msgs/msg/PoseWithCovariance`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseWithCovariance.html)
    - [`geometry_msgs/msg/PoseWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PoseWithCovarianceStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'pose'` keys. If found (as in `PoseStamped`),
    it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        # Internal usage within the ROS Bridge
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/pose",
            msg_type="geometry_msgs/msg/PoseStamped",
            data={
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
            }
        }
        # Automatically resolves to a flat Mosaico Pose with attached metadata
        mosaico_pose = PoseAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Pose",
        "geometry_msgs/msg/PoseStamped",
        "geometry_msgs/msg/PoseWithCovariance",
        "geometry_msgs/msg/PoseWithCovarianceStamped",
    )

    __mosaico_ontology_type__: Type[Pose] = Pose
    _REQUIRED_KEYS = ("position", "orientation")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg: The source ROS message yielded by the loader.
            **kwargs: Additional context for the translation.

        Returns:
            A Mosaico `Message` containing the normalized `Pose` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Pose:
        """
        Recursively parses a dictionary to extract a `Pose` object.

        Strategy:

        -  **Recurse**: If a 'pose' key is found, dive deeper into the structure.
        -  **Leaf Node**: At the base level, map 'position' and 'orientation' to
           [`Point3d`][mosaicolabs.models.data.Point3d] and
           [`Quaternion`][mosaicolabs.models.data.Quaternion].
        -  **Metadata Binding**: Headers and covariances are attached during
           recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
            }
            # Automatically resolves to a flat Mosaico Pose with attached metadata
            mosaico_pose = PoseAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Pose: The constructed Mosaico Pose object.

        Raises:
            ValueError: If the recursive 'pose' key exists but is not a dict, or if required keys are missing.
        """
        out_pose: Optional[Pose] = None

        # Recursive Step: Unwrap nested types (PoseWithCovariance, PoseStamped, PoseWithCovarianceStamped)
        # Look for a 'pose' key which indicates a wrapper structure
        pose_dict = ros_data.get("pose")
        if pose_dict:
            if not isinstance(pose_dict, dict):
                raise ValueError(
                    f"Invalid type for 'pose' value in ros message: expected 'dict' found '{type(pose_dict).__name__}'"
                )

            # Recurse to process the inner dictionary
            out_pose = cls.from_dict(pose_dict)

            # While unwinding recursion, attach metadata found at this level
            out_pose.header = _make_header(ros_data.get("header"))
            out_pose.covariance = ros_data.get("covariance")
            return out_pose

        # Base Case: We are at the leaf node (no nested 'pose' key)
        if not out_pose:
            _validate_msgdata(cls, ros_data)
            return Pose(
                position=PointAdapter.from_dict(ros_data["position"]),
                orientation=QuaternionAdapter.from_dict(ros_data["orientation"]),
            )


@register_adapter
class TwistAdapter(ROSAdapterBase[Velocity]):
    """
    Adapter for translating ROS Twist-related messages to Mosaico `Velocity`.

    Commonly referred to as a "Twist," this model captures the instantaneous motion
    of an object split into linear and angular components.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Twist`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Twist.html)
    - [`geometry_msgs/msg/TwistStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TwistStamped.html)
    - [`geometry_msgs/msg/TwistWithCovariance`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TwistWithCovariance.html)
    - [`geometry_msgs/msg/TwistWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TwistWithCovarianceStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'twist'` keys. If found (as in `TwistStamped`),
    it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg= ROSMessage(
            timestamp=1700000000000,
            topic="/cmd_vel",
            msg_type="geometry_msgs/msg/TwistStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "twist": {
                    "linear": {"x": 5.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 1.0}
            },
            "covariance": [0.1] * 36
        )
        # Automatically resolves to a flat Mosaico Velocity with attached metadata
        mosaico_velocity = TwistAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Twist",
        "geometry_msgs/msg/TwistStamped",
        "geometry_msgs/msg/TwistWithCovariance",
        "geometry_msgs/msg/TwistWithCovarianceStamped",
    )

    __mosaico_ontology_type__: Type[Velocity] = Velocity
    _REQUIRED_KEYS = ("linear", "angular")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Velocity` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Velocity:
        """
        Recursively parses the ROS data dictionary to extract a `Velocity` (Twist).

        Strategy:
        -  **Recurse**: If a 'twist' key is found, dive deeper into the structure.
        -  **Leaf Node**: At the base level, map 'linear' and 'angular' to
           [`Vector3`][mosaicolabs.models.data.geometry.Vector3d].
        -  **Metadata Binding**: Headers and covariances are attached during
           recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "twist": {
                    "linear": {"x": 5.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 1.0}
                },
                "covariance": [0.1] * 36
            }
            # Automatically resolves to a flat Mosaico Velocity with attached metadata
            mosaico_velocity = TwistAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Velocity: The constructed Mosaico Velocity object.

        Raises:
            ValueError: If the recursive 'twist' key exists but is not a dict, or if required keys are missing.
        """
        out_twist: Optional[Velocity] = None

        # Recursive Step: Unwrap nested types
        twist_dict = ros_data.get("twist")
        if twist_dict:
            if not isinstance(twist_dict, dict):
                raise ValueError(
                    f"Invalid type for 'twist' value in ros message: expected 'dict' found '{type(twist_dict).__name__}'"
                )

            out_twist = cls.from_dict(twist_dict)

            # Apply metadata from wrapper levels
            out_twist.header = _make_header(ros_data.get("header"))
            out_twist.covariance = ros_data.get("covariance")
            return out_twist

        # Base Case: Leaf node
        if not out_twist:
            _validate_msgdata(cls, ros_data)

            return Velocity(
                linear=Vector3Adapter.from_dict(ros_data["linear"]),
                angular=Vector3Adapter.from_dict(ros_data["angular"]),
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class AccelAdapter(ROSAdapterBase[Acceleration]):
    """
    Adapter for translating ROS Accel-related messages to Mosaico `Acceleration`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Accel`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Accel.html)
    - [`geometry_msgs/msg/AccelStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/AccelStamped.html)
    - [`geometry_msgs/msg/AccelWithCovariance`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/AccelWithCovariance.html)
    - [`geometry_msgs/msg/AccelWithCovarianceStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/AccelWithCovarianceStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'accel'` keys. If found (as in `AccelStamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/accel",
            timestamp=17000,
            msg_type="geometry_msgs/msg/AccelStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "accel": {
                    "linear": {"x": 5.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 1.0}
                },
                "covariance": [0.1] * 36
            }
        # Automatically resolves to a flat Mosaico Acceleration with attached metadata
        mosaico_acceleration = AccelAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Accel",
        "geometry_msgs/msg/AccelStamped",
        "geometry_msgs/msg/AccelWithCovariance",
        "geometry_msgs/msg/AccelWithCovarianceStamped",
    )

    __mosaico_ontology_type__: Type[Acceleration] = Acceleration
    _REQUIRED_KEYS = ("linear", "angular")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Acceleration` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Acceleration:
        """
        Recursively parses the ROS data dictionary to extract an `Acceleration`.

        Strategy:
        -  **Recurse**: If a 'accel' key is found, dive deeper into the structure.
        -  **Leaf Node**: At the base level, map 'linear' and 'angular' to
           [`Vector3`][mosaicolabs.models.data.geometry.Vector3d].
        -  **Metadata Binding**: Headers and covariances are attached during
           recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "accel": {
                    "linear": {"x": 5.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 1.0}
                },
                "covariance": [0.1] * 36
            }
            # Automatically resolves to a flat Mosaico Acceleration with attached metadata
            mosaico_acceleration = AccelAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Acceleration: The constructed Mosaico Acceleration object.

        Raises:
            ValueError: If the recursive 'accel' key exists but is not a dict, or if required keys are missing.
        """
        out_accel: Optional[Acceleration] = None

        # Recursive Step: Unwrap nested types
        accel_dict = ros_data.get("accel")
        if accel_dict:
            if not isinstance(accel_dict, dict):
                raise ValueError(
                    f"Invalid type for 'accel' value in ros message: expected 'dict' found '{type(accel_dict).__name__}'"
                )

            out_accel = cls.from_dict(accel_dict)

            # Apply metadata from wrapper levels
            out_accel.header = _make_header(ros_data.get("header"))
            out_accel.covariance = ros_data.get("covariance")
            return out_accel

        # Base Case: Leaf node
        if not out_accel:
            _validate_msgdata(cls, ros_data)

            return Acceleration(
                linear=Vector3Adapter.from_dict(ros_data["linear"]),
                angular=Vector3Adapter.from_dict(ros_data["angular"]),
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class Vector3Adapter(ROSAdapterBase[Vector3d]):
    """
    Adapter for translating ROS Vector3 messages to Mosaico [`Vector3d`][mosaicolabs.models.data.Vector3d].

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Vector3`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3.html)
    - [`geometry_msgs/msg/Vector3Stamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Vector3Stamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'vector'` keys. If found (as in `Vector3Stamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/vector3",
            timestamp=17000,
            msg_type="geometry_msgs/msg/Vector3Stamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "vector": {"x": 5.0, "y": 0.0, "z": 0.0},
            }
        # Automatically resolves to a flat Mosaico Vector3 with attached metadata
        mosaico_vector3 = Vector3Adapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Vector3",
        "geometry_msgs/msg/Vector3Stamped",
    )

    __mosaico_ontology_type__: Type[Vector3d] = Vector3d
    _REQUIRED_KEYS = ("x", "y", "z")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Vector3d` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Vector3d:
        """
        Recursively parses the ROS data to extract a `Vector3d`.

        Strategy:
        -  **Recurse**: If a 'vector' key is found, dive deeper into the structure.
        -  **Leaf Node**: At the base level, map 'x', 'y' and 'z' to
           [`Vector3d`][mosaicolabs.models.data.Vector3d].
        -  **Metadata Binding**: Headers and covariances are attached during
           recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "vector": {"x": 5.0, "y": 0.0, "z": 0.0},
            }
            # Automatically resolves to a flat Mosaico Vector3d with attached metadata
            mosaico_vector3d = Vector3Adapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Vector3d: The constructed Mosaico Vector3d object.

        Raises:
            ValueError: If the recursive 'vector' key exists but is not a dict, or if required keys are missing.
        """
        out_vec3: Optional[Vector3d] = None

        # Recursive Step: Unwrap nested types (Vector3dStamped usually has 'vector')
        vec3_dict = ros_data.get("vector")
        if vec3_dict:
            if not isinstance(vec3_dict, dict):
                raise ValueError(
                    f"Invalid type for 'vector' value in ros message: expected 'dict' found '{type(vec3_dict).__name__}'"
                )

            out_vec3 = cls.from_dict(vec3_dict)

            # Apply metadata
            out_vec3.header = _make_header(ros_data.get("header"))
            return out_vec3

        # Base Case: Leaf node
        if not out_vec3:
            _validate_msgdata(cls, ros_data)
            return Vector3d(
                x=ros_data["x"],
                y=ros_data["y"],
                z=ros_data["z"],
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class PointAdapter(ROSAdapterBase[Point3d]):
    """
    Adapter for translating ROS Point messages to Mosaico `Point3d`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Point`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Point.html)
    - [`geometry_msgs/msg/PointStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PointStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'point'` keys. If found (as in `PointStamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/point",
            timestamp=17000,
            msg_type="geometry_msgs/msg/PointStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "point": {"x": 5.0, "y": 0.0, "z": 0.0},
            }
        # Automatically resolves to a flat Mosaico Point3d with attached metadata
        mosaico_point3d = PointAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Point",
        "geometry_msgs/msg/PointStamped",
    )

    __mosaico_ontology_type__: Type[Point3d] = Point3d
    _REQUIRED_KEYS = ("x", "y", "z")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Point3d` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Point3d:
        """
        Recursively parses the ROS data to extract a `Point3d`.

        Strategy:
            -  **Recurse**: If a 'point' key is found, dive deeper into the structure.
            -  **Leaf Node**: At the base level, map 'x', 'y' and 'z' to
               [`Point3d`][mosaicolabs.models.data.Point3d].
            -  **Metadata Binding**: Headers and covariances are attached during
               recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "point": {"x": 5.0, "y": 0.0, "z": 0.0},
            }
            # Automatically resolves to a flat Mosaico Point3d with attached metadata
            mosaico_point3d = PointAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Point3d: The constructed Mosaico Point3d object.

        Raises:
            ValueError: If the recursive 'point' key exists but is not a dict, or if required keys are missing.
        """
        out_point: Optional[Point3d] = None

        # Recursive Step: Unwrap nested types (PointStamped uses 'point')
        point_dict = ros_data.get("point")
        if point_dict:
            if not isinstance(point_dict, dict):
                raise ValueError(
                    f"Invalid type for 'point' value in ros message: expected 'dict' found '{type(point_dict).__name__}'"
                )

            out_point = cls.from_dict(point_dict)

            # Apply metadata
            out_point.header = _make_header(ros_data.get("header"))
            return out_point

        # Base Case: Leaf node
        if not out_point:
            _validate_msgdata(cls, ros_data)
            return Point3d(
                x=ros_data["x"],
                y=ros_data["y"],
                z=ros_data["z"],
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class QuaternionAdapter(ROSAdapterBase[Quaternion]):
    """
    Adapter for translating ROS Quaternion messages to Mosaico `Quaternion`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Quaternion`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Quaternion.html)
    - [`geometry_msgs/msg/QuaternionStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/QuaternionStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'quaternion'` keys. If found (as in `QuaternionStamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/quaternion",
            timestamp=17000,
            msg_type="geometry_msgs/msg/QuaternionStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "quaternion": {"x": 5.0, "y": 0.0, "z": 0.0, "w": 1.0},
            }
        # Automatically resolves to a flat Mosaico Quaternion with attached metadata
        mosaico_quaternion = QuaternionAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Quaternion",
        "geometry_msgs/msg/QuaternionStamped",
    )

    __mosaico_ontology_type__: Type[Quaternion] = Quaternion
    _REQUIRED_KEYS = ("x", "y", "z", "w")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Quaternion` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Quaternion:
        """
        Recursively parses the ROS data to extract a `Quaternion`.

        Strategy:
            -  **Recurse**: If a 'quaternion' key is found, dive deeper into the structure.
            -  **Leaf Node**: At the base level, map 'x', 'y', 'z' and 'w' to
               [`Quaternion`][mosaicolabs.models.data.Quaternion].
            -  **Metadata Binding**: Headers and covariances are attached during
               recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "quaternion": {"x": 5.0, "y": 0.0, "z": 0.0, "w": 1.0},
            }
            # Automatically resolves to a flat Mosaico Quaternion with attached metadata
            mosaico_quaternion = QuaternionAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Quaternion: The constructed Mosaico Quaternion object.

        Raises:
            ValueError: If the recursive 'quaternion' key exists but is not a dict, or if required keys are missing.
        """
        out_quat: Optional[Quaternion] = None

        # Recursive Step: Unwrap nested types (QuaternionStamped uses 'quaternion')
        quat_dict = ros_data.get("quaternion")
        if quat_dict:
            if not isinstance(quat_dict, dict):
                raise ValueError(
                    f"Invalid type for 'quaternion' value in ros message: expected 'dict' found '{type(quat_dict).__name__}'"
                )

            out_quat = cls.from_dict(quat_dict)

            # Apply metadata
            out_quat.header = _make_header(ros_data.get("header"))
            return out_quat

        # Base Case: Leaf node
        if not out_quat:
            _validate_msgdata(cls, ros_data)
            return Quaternion(
                x=ros_data["x"],
                y=ros_data["y"],
                z=ros_data["z"],
                w=ros_data["w"],
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class TransformAdapter(ROSAdapterBase[Transform]):
    """
    Adapter for translating ROS Transform messages to Mosaico `Transform`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/TransformStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TransformStamped.html)
    - [`geometry_msgs/msg/Transform`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Transform.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'transform'` keys. If found (as in `TransformStamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/transform",
            timestamp=17000,
            msg_type="geometry_msgs/msg/TransformStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "transform": {"translation": {"x": 5.0, "y": 0.0, "z": 0.0}, "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}},
            }
        # Automatically resolves to a flat Mosaico Transform with attached metadata
        mosaico_transform = TransformAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/TransformStamped",
        "geometry_msgs/msg/Transform",
    )

    __mosaico_ontology_type__: Type[Transform] = Transform
    _REQUIRED_KEYS = ("translation", "rotation")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Transform` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Transform:
        """
        Parses ROS Transform data. Handles both nested 'transform' field (from Stamped)
        and flat structure.

        Strategy:
            -  **Recurse**: If a 'transform' key is found, dive deeper into the structure.
            -  **Leaf Node**: At the base level, map 'translation' and 'rotation' to
               [`Transform`][mosaicolabs.models.data.Transform].
            -  **Metadata Binding**: Headers and covariances are attached during
               recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "transform": {"translation": {"x": 5.0, "y": 0.0, "z": 0.0}, "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}},
            }
            # Automatically resolves to a flat Mosaico Transform with attached metadata
            mosaico_transform = TransformAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Transform: The constructed Mosaico Transform object.

        Raises:
            ValueError: If the recursive 'transform' key exists but is not a dict, or if required keys are missing.
        """
        out_transf: Optional[Transform] = None

        # Recursive Step: Unwrap nested types (TransformStamped)
        transf_dict = ros_data.get("transform")
        if transf_dict:
            if not isinstance(transf_dict, dict):
                raise ValueError(
                    f"Invalid type for 'transform' value in ros message: expected 'dict' found '{type(transf_dict).__name__}'"
                )

            out_transf = cls.from_dict(transf_dict)

            # Apply metadata
            out_transf.header = _make_header(ros_data.get("header"))
            out_transf.target_frame_id = ros_data.get("child_frame_id")
            return out_transf

        # Base Case: Leaf node
        if not out_transf:
            _validate_msgdata(cls, ros_data)

            return Transform(
                translation=Vector3Adapter.from_dict(ros_data["translation"]),
                rotation=QuaternionAdapter.from_dict(ros_data["rotation"]),
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_adapter
class WrenchAdapter(ROSAdapterBase[ForceTorque]):
    """
    Adapter for translating ROS Wrench messages to Mosaico `ForceTorque`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Wrench`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Wrench.html)
    - [`geometry_msgs/msg/WrenchStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/WrenchStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested `'wrench'` keys. If found (as in `WrenchStamped`), it recurses to the leaf node while collecting metadata like headers and
    covariance matrices along the way.

    Example:
        ```python
        ros_msg = ROSMessage(
            topic="/wrench",
            timestamp=17000,
            msg_type="geometry_msgs/msg/WrenchStamped",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "wrench": {"force": {"x": 5.0, "y": 0.0, "z": 0.0}, "torque": {"x": 0.0, "y": 0.0, "z": 0.0}},
            }
        # Automatically resolves to a flat Mosaico ForceTorque with attached metadata
        mosaico_wrench = WrenchAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/WrenchStamped",
        "geometry_msgs/msg/Wrench",
    )

    __mosaico_ontology_type__: Type[ForceTorque] = ForceTorque
    _REQUIRED_KEYS = ("force", "torque")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `ForceTorque` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> ForceTorque:
        """
        Parses ROS ForceTorque data. Handles both nested 'wrench' field (from Stamped)
        and flat structure.

        Strategy:
            -  **Recurse**: If a 'wrench' key is found, dive deeper into the structure.
            -  **Leaf Node**: At the base level, map 'force' and 'torque' to
               [`ForceTorque`][mosaicolabs.models.data.ForceTorque].
            -  **Metadata Binding**: Headers and covariances are attached during
               recursion unwinding.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "wrench": {"force": {"x": 5.0, "y": 0.0, "z": 0.0}, "torque": {"x": 0.0, "y": 0.0, "z": 0.0}},
            }
            # Automatically resolves to a flat Mosaico ForceTorque with attached metadata
            mosaico_wrench = WrenchAdapter.from_dict(ros_data)
            ```
        """
        out_ft: Optional[ForceTorque] = None

        # Recursive Step: Unwrap nested types (TransformStamped)
        wrench_dict = ros_data.get("wrench")
        if wrench_dict:
            if not isinstance(wrench_dict, dict):
                raise ValueError(
                    f"Invalid type for 'wrench' value in ros message: expected 'dict' found '{type(wrench_dict).__name__}'"
                )

            out_ft = cls.from_dict(wrench_dict)

            # Apply metadata
            out_ft.header = _make_header(ros_data.get("header"))
            return out_ft

        # Base Case: Leaf node
        if not out_ft:
            _validate_msgdata(cls, ros_data)

            return ForceTorque(
                force=Vector3Adapter.from_dict(ros_data["force"]),
                torque=Vector3Adapter.from_dict(ros_data["torque"]),
            )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None
