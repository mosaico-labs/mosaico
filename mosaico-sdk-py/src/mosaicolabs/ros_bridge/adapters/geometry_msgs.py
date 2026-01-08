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

    Handles multiple ROS message types:
    - `geometry_msgs/msg/Pose`
    - `geometry_msgs/msg/PoseStamped`
    - `geometry_msgs/msg/PoseWithCovariance`
    - `geometry_msgs/msg/PoseWithCovarianceStamped`

    Logic includes recursive unwrapping to handle nested structures (e.g., `PoseWithCovariance` wrapping a `Pose`).
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
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Pose` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Pose:
        """
        Recursively parses the ROS data dictionary to extract a `Pose`.

        Strategy:
        -  Checks for a nested 'pose' key (used in Stamped/WithCovariance types).
            If found, it recurses to unwrap the inner structure.
        -  If no 'pose' key is found, it expects 'position' and 'orientation' keys
            (the flat structure of a standard ROS Pose).

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

    Handles multiple ROS message types:
    - `geometry_msgs/msg/Twist`
    - `geometry_msgs/msg/TwistStamped`
    - `geometry_msgs/msg/TwistWithCovariance`
    - `geometry_msgs/msg/TwistWithCovarianceStamped`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Velocity:
        """
        Recursively parses the ROS data dictionary to extract a `Velocity` (Twist).

        Follows the same recursive unwrapping strategy as PoseAdapter.
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

    Handles:
    - `geometry_msgs/msg/Accel`
    - `geometry_msgs/msg/AccelStamped`
    - `geometry_msgs/msg/AccelWithCovariance`
    - `geometry_msgs/msg/AccelWithCovarianceStamped`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Acceleration:
        """
        Recursively parses the ROS data dictionary to extract an `Acceleration`.
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
    Adapter for translating ROS Vector3 messages to Mosaico `Vector3d`.

    Handles:
    - `geometry_msgs/msg/Vector3`
    - `geometry_msgs/msg/Vector3Stamped`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Vector3d:
        """
        Recursively parses the ROS data to extract a `Vector3d`.
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

    Handles:
    - `geometry_msgs/msg/Point`
    - `geometry_msgs/msg/PointStamped`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Point3d:
        """
        Recursively parses the ROS data to extract a `Point3d`.
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

    Handles:
    - `geometry_msgs/msg/Quaternion`
    - `geometry_msgs/msg/QuaternionStamped`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
                message_header=ros_msg.header.translate() if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Quaternion:
        """
        Recursively parses the ROS data to extract a `Quaternion`.
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

    Handles:
    - `geometry_msgs/msg/TransformStamped`
    - `geometry_msgs/msg/Transform`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                message_header=ros_msg.header.translate() if ros_msg.header else None,
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Transform:
        """
        Parses ROS Transform data. Handles both nested 'transform' field (from Stamped)
        and flat structure.
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

    Handles:
    - `geometry_msgs/msg/WrenchStamped`
    - `geometry_msgs/msg/Wrench`
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                message_header=ros_msg.header.translate() if ros_msg.header else None,
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> ForceTorque:
        """
        Parses ROS ForceTorque data. Handles both nested 'wrench' field (from Stamped)
        and flat structure.
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
