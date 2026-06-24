"""
Geometry Messages Adaptation Module.

This module provides specialized adapters for translating ROS `geometry_msgs` into the
standardized Mosaico Ontology. It implements recursive unwrapping to handle common
ROS patterns, such as "Stamped" envelopes and covariance wrappers, ensuring that
spatial data is normalized before ingestion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

import numpy as np
from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs.models.core import Message
from mosaicolabs.models.data import (
    Acceleration,
    ForceTorque,
    Inertia,
    Point3d,
    Polygon,
    Pose,
    Quaternion,
    Transform,
    Vector3d,
    Velocity,
)

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .helpers import _is_valid_covariance, _is_valid_header, _validate_msgdata
from .std_msgs import HeaderAdapter


@register_default_adapter(is_default=True)
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
        -  **Metadata Binding**: Covariances are attached during
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
            out_pose.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            ros_covariance = ros_data.get("covariance")
            if ros_covariance:
                covariance = None
                if _is_valid_covariance(ros_covariance):
                    covariance = ros_covariance

                out_pose.covariance = covariance

            return out_pose

        # Base Case: We are at the leaf node (no nested 'pose' key)
        if not out_pose:
            _validate_msgdata(cls, ros_data)
            return Pose(
                position=PointAdapter.from_dict(ros_data["position"]),
                orientation=QuaternionAdapter.from_dict(ros_data["orientation"]),
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Pose],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Pose`` (or a ``Message`` wrapping one) into the
        corresponding ROS geometry message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Pose``
        - ``geometry_msgs/msg/PoseStamped``
        - ``geometry_msgs/msg/PoseWithCovariance``
        - ``geometry_msgs/msg/PoseWithCovarianceStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Pose`` instance, or a raw ``Pose``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Pose`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        pose_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPose = typestore.types["geometry_msgs/msg/Pose"]
        RosPoseStamped = typestore.types["geometry_msgs/msg/PoseStamped"]
        RosPoseWithCovariance = typestore.types["geometry_msgs/msg/PoseWithCovariance"]
        RosPoseWithCovarianceStamped = typestore.types[
            "geometry_msgs/msg/PoseWithCovarianceStamped"
        ]

        pose = RosPose(
            position=PointAdapter.to_ros(pose_data.position, typestore),
            orientation=QuaternionAdapter.to_ros(pose_data.orientation, typestore),
        )

        # In case covariance is None, a flatted 6x6 full of zeros is provided
        pose_covariance = pose_data.covariance or [0.0] * 36

        if resolved_rosmsg_type == "geometry_msgs/msg/Pose":
            return pose
        elif resolved_rosmsg_type == "geometry_msgs/msg/PoseStamped":
            return RosPoseStamped(
                pose=pose, header=HeaderAdapter.to_ros(ms_header, typestore)
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/PoseWithCovariance":
            return RosPoseWithCovariance(
                pose=pose, covariance=np.asarray(pose_covariance, dtype=np.float64)
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/PoseWithCovarianceStamped":
            pose_w_cov = RosPoseWithCovariance(
                pose=pose, covariance=np.asarray(pose_covariance, dtype=np.float64)
            )
            return RosPoseWithCovarianceStamped(
                pose=pose_w_cov, header=HeaderAdapter.to_ros(ms_header, typestore)
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )


@register_default_adapter(is_default=True)
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
        -  **Metadata Binding**: Covariances are attached during
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

            # While unwinding recursion, attach metadata found at this level
            out_twist.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            # Apply metadata from wrapper levels
            ros_covariance = ros_data.get("covariance")
            if ros_covariance:
                covariance = None
                if _is_valid_covariance(ros_covariance):
                    covariance = ros_covariance

                out_twist.covariance = covariance
            return out_twist

        # Base Case: Leaf node
        if not out_twist:
            _validate_msgdata(cls, ros_data)

            return Velocity(
                linear=Vector3Adapter.from_dict(ros_data["linear"]),
                angular=Vector3Adapter.from_dict(ros_data["angular"]),
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Velocity],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Velocity`` (or a ``Message`` wrapping one) into the
        corresponding ROS Twist message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Twist``
        - ``geometry_msgs/msg/TwistStamped``
        - ``geometry_msgs/msg/TwistWithCovariance``
        - ``geometry_msgs/msg/TwistWithCovarianceStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Velocity`` instance, or a raw ``Velocity``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Twist`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        velocity_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosTwist = typestore.types["geometry_msgs/msg/Twist"]
        RosTwistStamped = typestore.types["geometry_msgs/msg/TwistStamped"]
        RosTwistWithCovariance = typestore.types[
            "geometry_msgs/msg/TwistWithCovariance"
        ]
        RosTwistWithCovarianceStamped = typestore.types[
            "geometry_msgs/msg/TwistWithCovarianceStamped"
        ]

        # In case covariance is None, a flatted 6x6 full of zeros is provided
        twist_linear = velocity_data.linear or Vector3d(x=0, y=0, z=0)
        twist_angular = velocity_data.angular or Vector3d(x=0, y=0, z=0)
        twist_covariance = velocity_data.covariance or [0.0] * 36

        twist = RosTwist(
            linear=Vector3Adapter.to_ros(twist_linear, typestore),
            angular=Vector3Adapter.to_ros(twist_angular, typestore),
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Twist":
            return twist
        elif resolved_rosmsg_type == "geometry_msgs/msg/TwistStamped":
            return RosTwistStamped(
                twist=twist, header=HeaderAdapter.to_ros(ms_header, typestore)
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/TwistWithCovariance":
            return RosTwistWithCovariance(
                twist=twist,
                covariance=np.asarray(twist_covariance, dtype=np.float64),
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/TwistWithCovarianceStamped":
            twist_w_cov = RosTwistWithCovariance(
                twist=twist,
                covariance=np.asarray(twist_covariance, dtype=np.float64),
            )
            return RosTwistWithCovarianceStamped(
                twist=twist_w_cov, header=HeaderAdapter.to_ros(ms_header, typestore)
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
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
        -  **Metadata Binding**: Covariances are attached during
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

            # While unwinding recursion, attach metadata found at this level
            out_accel.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            # Apply metadata from wrapper levels
            ros_covariance = ros_data.get("covariance")
            if ros_covariance:
                covariance = None
                if _is_valid_covariance(ros_covariance):
                    covariance = ros_covariance

                out_accel.covariance = covariance

            return out_accel

        # Base Case: Leaf node
        if not out_accel:
            _validate_msgdata(cls, ros_data)

            return Acceleration(
                linear=Vector3Adapter.from_dict(ros_data["linear"]),
                angular=Vector3Adapter.from_dict(ros_data["angular"]),
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Acceleration],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Acceleration`` (or a ``Message`` wrapping one) into the
        corresponding ROS Accel message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Accel``
        - ``geometry_msgs/msg/AccelStamped``
        - ``geometry_msgs/msg/AccelWithCovariance``
        - ``geometry_msgs/msg/AccelWithCovarianceStamped``

        Args:
            mosaico_data: A ``Message`` wrapping an ``Acceleration`` instance, or a raw ``Acceleration``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Accel`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        accel_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosAccel = typestore.types["geometry_msgs/msg/Accel"]
        RosAccelStamped = typestore.types["geometry_msgs/msg/AccelStamped"]
        RosAccelWithCovariance = typestore.types[
            "geometry_msgs/msg/AccelWithCovariance"
        ]
        RosAccelWithCovarianceStamped = typestore.types[
            "geometry_msgs/msg/AccelWithCovarianceStamped"
        ]

        # In case covariance is None, a flatted 6x6 full of zeros is provided
        accel_linear = accel_data.linear or Vector3d(x=0.0, y=0.0, z=0.0)
        accel_angular = accel_data.angular or Vector3d(x=0.0, y=0.0, z=0.0)
        accel_covariance = accel_data.covariance or [0.0] * 36

        accel = RosAccel(
            linear=Vector3Adapter.to_ros(accel_linear, typestore),
            angular=Vector3Adapter.to_ros(accel_angular, typestore),
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Accel":
            return accel
        elif resolved_rosmsg_type == "geometry_msgs/msg/AccelStamped":
            return RosAccelStamped(
                accel=accel, header=HeaderAdapter.to_ros(ms_header, typestore)
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/AccelWithCovariance":
            return RosAccelWithCovariance(
                accel=accel, covariance=np.asarray(accel_covariance, dtype=np.float64)
            )
        elif resolved_rosmsg_type == "geometry_msgs/msg/AccelWithCovarianceStamped":
            accel_w_cov = RosAccelWithCovariance(
                accel=accel, covariance=np.asarray(accel_covariance, dtype=np.float64)
            )
            return RosAccelWithCovarianceStamped(
                accel=accel_w_cov, header=HeaderAdapter.to_ros(ms_header, typestore)
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
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
            out_vec3.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

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
    def to_ros(
        cls,
        mosaico_data: Union[Message, Vector3d],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Vector3d`` (or a ``Message`` wrapping one) into the
        corresponding ROS Vector3 message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Vector3``
        - ``geometry_msgs/msg/Vector3Stamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Vector3d`` instance, or a raw ``Vector3d``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Vector3`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        vector3d_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosVector3 = typestore.types["geometry_msgs/msg/Vector3"]
        RosVector3Stamped = typestore.types["geometry_msgs/msg/Vector3Stamped"]

        vector = RosVector3(
            x=vector3d_data.x,
            y=vector3d_data.y,
            z=vector3d_data.z,
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Vector3":
            return vector
        elif resolved_rosmsg_type == "geometry_msgs/msg/Vector3Stamped":
            return RosVector3Stamped(
                vector=vector, header=HeaderAdapter.to_ros(ms_header, typestore)
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
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

            # While unwinding recursion, attach metadata found at this level
            out_point.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            # Apply metadata
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
    def to_ros(
        cls,
        mosaico_data: Union[Message, Point3d],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Point3d`` (or a ``Message`` wrapping one) into the
        corresponding ROS Point message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Point``
        - ``geometry_msgs/msg/PointStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Point3d`` instance, or a raw ``Point3d``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Point`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        point_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPoint = typestore.types["geometry_msgs/msg/Point"]
        RosPointStamped = typestore.types["geometry_msgs/msg/PointStamped"]

        point = RosPoint(x=point_data.x, y=point_data.y, z=point_data.z)

        if resolved_rosmsg_type == "geometry_msgs/msg/Point":
            return point
        elif resolved_rosmsg_type == "geometry_msgs/msg/PointStamped":
            return RosPointStamped(
                header=HeaderAdapter.to_ros(ms_header, typestore), point=point
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
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

            # While unwinding recursion, attach metadata found at this level
            out_quat.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            # Apply metadata
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
    def to_ros(
        cls,
        mosaico_data: Union[Message, Quaternion],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Quaternion`` (or a ``Message`` wrapping one) into the
        corresponding ROS Quaternion message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Quaternion``
        - ``geometry_msgs/msg/QuaternionStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Quaternion`` instance, or a raw ``Quaternion``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Quaternion`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        quaternion_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosQuaternion = typestore.types["geometry_msgs/msg/Quaternion"]
        RosQuaternionStamped = typestore.types["geometry_msgs/msg/QuaternionStamped"]

        quaternion = RosQuaternion(
            x=quaternion_data.x,
            y=quaternion_data.y,
            z=quaternion_data.z,
            w=quaternion_data.w,
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Quaternion":
            return quaternion
        elif resolved_rosmsg_type == "geometry_msgs/msg/QuaternionStamped":
            return RosQuaternionStamped(
                header=HeaderAdapter.to_ros(ms_header, typestore), quaternion=quaternion
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
class TransformAdapter(ROSAdapterBase[Transform]):
    """
    Adapter for translating ROS Transform messages to Mosaico `Transform`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Transform`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Transform.html)
    - [`geometry_msgs/msg/TransformStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/TransformStamped.html)

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
        "geometry_msgs/msg/Transform",
        "geometry_msgs/msg/TransformStamped",
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

            # While unwinding recursion, attach metadata found at this level
            ms_header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )
            out_transf.header = ms_header

            child_frame_id = ros_data.get("child_frame_id")
            if child_frame_id and child_frame_id != "":
                out_transf.target_frame_id = child_frame_id

            return out_transf

        # Base Case: Leaf node
        if not out_transf:
            _validate_msgdata(cls, ros_data)

            return Transform(
                translation=Vector3Adapter.from_dict(ros_data["translation"]),
                rotation=QuaternionAdapter.from_dict(ros_data["rotation"]),
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Transform],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Transform`` (or a ``Message`` wrapping one) into the
        corresponding ROS Transform message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Transform``
        - ``geometry_msgs/msg/TransformStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Transform`` instance, or a raw ``Transform``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Transform`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        transform_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        target_frame_id = transform_data.target_frame_id or ""

        RosTransform = typestore.types["geometry_msgs/msg/Transform"]
        RosTransformStamped = typestore.types["geometry_msgs/msg/TransformStamped"]

        transform = RosTransform(
            translation=Vector3Adapter.to_ros(transform_data.translation, typestore),
            rotation=QuaternionAdapter.to_ros(transform_data.rotation, typestore),
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Transform":
            return transform
        elif resolved_rosmsg_type == "geometry_msgs/msg/TransformStamped":
            return RosTransformStamped(
                header=HeaderAdapter.to_ros(ms_header, typestore),
                child_frame_id=target_frame_id,
                transform=transform,
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
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
        "geometry_msgs/msg/Wrench",
        "geometry_msgs/msg/WrenchStamped",
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

            # While unwinding recursion, attach metadata found at this level
            out_ft.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            # Apply metadata
            return out_ft

        # Base Case: Leaf node
        if not out_ft:
            _validate_msgdata(cls, ros_data)

            return ForceTorque(
                force=Vector3Adapter.from_dict(ros_data["force"]),
                torque=Vector3Adapter.from_dict(ros_data["torque"]),
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, ForceTorque],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``ForceTorque`` (or a ``Message`` wrapping one) into the
        corresponding ROS Wrench message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Wrench``
        - ``geometry_msgs/msg/WrenchStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``ForceTorque`` instance, or a raw ``ForceTorque``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Wrench`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        force_torque_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosWrench = typestore.types["geometry_msgs/msg/Wrench"]
        RosWrenchStamped = typestore.types["geometry_msgs/msg/WrenchStamped"]

        wrench = RosWrench(
            force=Vector3Adapter.to_ros(force_torque_data.force, typestore),
            torque=Vector3Adapter.to_ros(force_torque_data.torque, typestore),
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Wrench":
            return wrench
        elif resolved_rosmsg_type == "geometry_msgs/msg/WrenchStamped":
            return RosWrenchStamped(
                wrench=wrench, header=HeaderAdapter.to_ros(ms_header, typestore)
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
class PolygonAdapter(ROSAdapterBase[Polygon]):
    """
    Adapter for translating ROS Polygon messages to Mosaico `Polygon`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Polygon`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Polygon.html)
    - [`geometry_msgs/msg/PolygonStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/PolygonStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested 'polygon' keys (as in PolygonStamped) and recursively unwraps to the base structure.

    Example:
    ```python
    ros_msg = ROSMessage(
        topic="/polygon",
        timestamp=17000,
        msg_type="geometry_msgs/msg/Polygon",
        data={
            "points": [
                {"x": 1.0, "y": 2.0, "z": 0.0},
                {"x": 3.0, "y": 4.0, "z": 0.0},
            ]
        }
    )

    mosaico_polygon = PolygonAdapter.translate(ros_msg)
    ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Polygon",
        "geometry_msgs/msg/PolygonStamped",
    )

    __mosaico_ontology_type__: Type[Polygon] = Polygon
    _REQUIRED_KEYS = ("points",)

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Polygon` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Polygon:
        """
        Parses ROS Polygon data. Handles both nested ('PolygonStamped') and flat structures.

        Strategy:
            - **Recurse**: If a 'polygon' key is found, unwrap and process the inner structure.
            - **Leaf Node**: Convert the list of ROS points into Mosaico `Point3d` objects
            and construct a `Polygon`.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Polygon: The constructed Mosaico Polygon object.
        """
        out_poly: Optional[Polygon] = None

        # Recursive Step (PolygonStamped)
        poly_dict = ros_data.get("polygon")
        if poly_dict:
            if not isinstance(poly_dict, dict):
                raise ValueError(
                    f"Invalid type for 'polygon': expected dict, got {type(poly_dict).__name__}"
                )

            out_poly = cls.from_dict(poly_dict)

            # While unwinding recursion, attach metadata found at this level
            out_poly.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            return out_poly

        # Base Case
        if not out_poly:
            _validate_msgdata(cls, ros_data)

            points = [PointAdapter.from_dict(p) for p in ros_data["points"]]

            return Polygon(points=points)

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Polygon],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Polygon`` (or a ``Message`` wrapping one) into the
        corresponding ROS Polygon message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Polygon``
        - ``geometry_msgs/msg/PolygonStamped``

        Args:
            mosaico_data: A ``Message`` wrapping a ``Polygon`` instance, or a raw ``Polygon``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Polygon`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        polygon_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPolygon = typestore.types["geometry_msgs/msg/Polygon"]
        RosPolygonStamped = typestore.types["geometry_msgs/msg/PolygonStamped"]

        ros_points: Any = [
            PointAdapter.to_ros(point3d, typestore) for point3d in polygon_data.points
        ]

        if resolved_rosmsg_type == "geometry_msgs/msg/Polygon":
            return RosPolygon(points=ros_points)
        elif resolved_rosmsg_type == "geometry_msgs/msg/PolygonStamped":
            return RosPolygonStamped(
                polygon=RosPolygon(points=ros_points),
                header=HeaderAdapter.to_ros(ms_header, typestore),
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None


@register_default_adapter(is_default=True)
class InertiaAdapter(ROSAdapterBase[Inertia]):
    """
    Adapter for translating ROS Inertia messages to Mosaico `Inertia`.

    **Supported ROS Types:**

    - [`geometry_msgs/msg/Inertia`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/Inertia.html)
    - [`geometry_msgs/msg/InertiaStamped`](https://docs.ros2.org/foxy/api/geometry_msgs/msg/InertiaStamped.html)

    **Recursive Unwrapping Strategy:**
    The adapter checks for nested 'inertia' keys (as in InertiaStamped) and recursively unwraps to the base structure.

    Example:
    ```python
    ros_msg = ROSMessage(
        topic="/inertia",
        timestamp=17000,
        msg_type="geometry_msgs/msg/Inertia",
        data={
            "m": 10.0,
            "com": {"x": 0.0, "y": 0.0, "z": 0.0},
            "ixx": 1.0,
            "ixy": 0.0,
            "ixz": 0.0,
            "iyy": 1.0,
            "iyz": 0.0,
            "izz": 1.0,
        }
    )

    mosaico_inertia = InertiaAdapter.translate(ros_msg)
    ```
    """

    ros_msgtype: str | Tuple[str, ...] = (
        "geometry_msgs/msg/Inertia",
        "geometry_msgs/msg/InertiaStamped",
    )

    __mosaico_ontology_type__: Type[Inertia] = Inertia

    _REQUIRED_KEYS = (
        "m",
        "com",
        "ixx",
        "ixy",
        "ixz",
        "iyy",
        "iyz",
        "izz",
    )

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Inertia` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Inertia:
        """
        Parses ROS Inertia data. Handles both nested ('InertiaStamped') and flat structures.

        Strategy:
            - **Recurse**: If an 'inertia' key is found, unwrap and process the inner structure.
            - **Leaf Node**:
                - Map 'com' to a Mosaico `Vector3d`.
                - Construct the inertia tensor from scalar components (ixx, ixy, etc.).
                - Build the `Inertia` object.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Inertia: The constructed Mosaico Inertia object.
        """
        out_inertia: Optional[Inertia] = None

        # Recursive Step (InertiaStamped)
        inertia_dict = ros_data.get("inertia")
        if inertia_dict:
            if not isinstance(inertia_dict, dict):
                raise ValueError(
                    f"Invalid type for 'inertia': expected dict, got {type(inertia_dict).__name__}"
                )

            out_inertia = cls.from_dict(inertia_dict)

            # While unwinding recursion, attach metadata found at this level
            out_inertia.header = (
                HeaderAdapter.from_dict(ros_data["header"])
                if _is_valid_header(ros_data.get("header"))
                else None
            )

            return out_inertia

        # Base Case
        if not out_inertia:
            _validate_msgdata(cls, ros_data)

            center_of_mass = Vector3Adapter.from_dict(ros_data["com"])

            inertia_tensor = [
                ros_data["ixx"],
                ros_data["ixy"],
                ros_data["ixz"],
                ros_data["iyy"],
                ros_data["iyz"],
                ros_data["izz"],
            ]

            return Inertia(
                mass=ros_data["m"],
                center_of_mass=center_of_mass,
                inertia=inertia_tensor,
            )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Inertia],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Inertia`` (or a ``Message`` wrapping one) into the
        corresponding ROS Inertia message.

        Supported output types (selectable via *ros_msg_type*):

        - ``geometry_msgs/msg/Inertia``
        - ``geometry_msgs/msg/InertiaStamped``

        Args:
            mosaico_data: A ``Message`` wrapping an ``Inertia`` instance, or a raw ``Inertia``.
            typestore: The rosbags typestore for target type resolution.
            ros_msg_type: Override for the output ROS type. Defaults to
                ``geometry_msgs/msg/Inertia`` if ``None``.

        Returns:
            The constructed ROS message, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        inertia_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosInertia = typestore.types["geometry_msgs/msg/Inertia"]
        RosInertiaStamped = typestore.types["geometry_msgs/msg/InertiaStamped"]

        inertia = RosInertia(
            m=inertia_data.mass,
            com=Vector3Adapter.to_ros(inertia_data.center_of_mass, typestore),
            ixx=inertia_data.inertia[0],
            ixy=inertia_data.inertia[1],
            ixz=inertia_data.inertia[2],
            iyy=inertia_data.inertia[3],
            iyz=inertia_data.inertia[4],
            izz=inertia_data.inertia[5],
        )

        if resolved_rosmsg_type == "geometry_msgs/msg/Inertia":
            return inertia
        elif resolved_rosmsg_type == "geometry_msgs/msg/InertiaStamped":
            return RosInertiaStamped(
                header=HeaderAdapter.to_ros(ms_header, typestore), inertia=inertia
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        return None
