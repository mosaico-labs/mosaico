import math
import sys
from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

import numpy as np
from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs import Serializable
from mosaicolabs.models import Message
from mosaicolabs.models.data import ROI, Point3d, Quaternion, Vector2d, Vector3d
from mosaicolabs.models.futures import (
    LaserScan,
    MultiEchoLaserScan,
)
from mosaicolabs.models.sensors import (
    GPS,
    IMU,
    CameraInfo,
    CompressedImage,
    GPSStatus,
    Image,
    Joy,
    Magnetometer,
    NMEASentence,
    RobotJoint,
)

from ..adapter_base import ROSAdapterBase
from ..data_ontology import BatteryState, PointCloud2, PointField, PointFieldDataType
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .geometry_msgs import (
    QuaternionAdapter,
    Vector3Adapter,
)
from .helpers import _is_valid_covariance, _validate_msgdata, _validate_required_fields


@register_default_adapter(is_default=True)
class CameraInfoAdapter(ROSAdapterBase[CameraInfo]):
    """
    Adapter for translating ROS CameraInfo messages to Mosaico `CameraInfo`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/CameraInfo`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CameraInfo.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/camera_info",
            msg_type="sensor_msgs/msg/CameraInfo",
            data=
            {
                "height": 480,
                "width": 640,
                "binning_x": 1,
                "binning_y": 1,
                "roi": {
                    "x_offset": 0,
                    "y_offset": 0,
                    "height": 480,
                    "width": 640,
                    "do_rectify": False,
                },
                "distortion_model": "plumb_bob",
                "d": [0.0, 0.0, 0.0, 0.0, 0.0],
                "k": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
                "p": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "r": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
            }
        )
        # Automatically resolves to a flat Mosaico CameraInfo with attached metadata
        mosaico_camera_info = CameraInfoAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/CameraInfo"

    __mosaico_ontology_type__: Type[CameraInfo] = CameraInfo
    _REQUIRED_KEYS = (
        "height",
        "width",
        "binning_x",
        "binning_y",
        "roi",
        "distortion_model",
        "d",
        "k",
        "p",
        "r",
    )

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `CameraInfo` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> CameraInfo:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "height": 480,
                "width": 640,
                "binning_x": 1,
                "binning_y": 1,
                "roi": {
                    "x_offset": 0,
                    "y_offset": 0,
                    "height": 480,
                    "width": 640,
                    "do_rectify": False,
                },
                "distortion_model": "plumb_bob",
                "d": [0.0, 0.0, 0.0, 0.0, 0.0],
                "k": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
                "p": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "r": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
            }
            # Automatically resolves to a flat Mosaico CameraInfo with attached metadata
            mosaico_camera_info = CameraInfoAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            CameraInfo: The constructed Mosaico CameraInfo object.

        Raises:
            ValueError: If the recursive 'roi' key exists but is not a dict, or if required keys are missing.
        """
        # validate case insensitive keys (specific for this message - ROS1/2 variations)
        _validate_msgdata(cls, ros_data, case_insensitive=True)

        binning = None
        if ros_data["binning_x"] > 1 and ros_data["binning_y"] > 1:
            binning = Vector2d(x=ros_data["binning_x"], y=ros_data["binning_y"])

        roi = ROIAdapter.from_dict(ros_data["roi"])
        if (
            roi.offset.x == 0
            and roi.offset.y == 0
            and roi.height == 0
            and roi.width == 0
            and roi.do_rectify is False
        ):
            roi = None

        # Manage differences between ROS1 and ROS2s
        return CameraInfo(
            height=ros_data["height"],
            width=ros_data["width"],
            binning=binning,
            distortion_model=ros_data["distortion_model"],
            distortion_parameters=ros_data.get("d") or ros_data.get("D"),
            intrinsic_parameters=ros_data.get("k") or ros_data.get("K"),
            projection_parameters=ros_data.get("p") or ros_data.get("P"),
            rectification_parameters=ros_data.get("r") or ros_data.get("R"),
            roi=roi,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, CameraInfo],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``CameraInfo`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/CameraInfo`` message.

        Handles both ROS 1 (uppercase field names ``D``, ``K``, ``R``, ``P``) and
        ROS 2 (lowercase ``d``, ``k``, ``r``, ``p``) field conventions.

        Args:
            mosaico_data: A ``Message`` wrapping a ``CameraInfo`` instance, or a raw ``CameraInfo``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/CameraInfo`` is supported.

        Returns:
            A ``sensor_msgs/msg/CameraInfo`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        camera_info_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosCameraInfo = typestore.types["sensor_msgs/msg/CameraInfo"]

        if resolved_rosmsg_type == "sensor_msgs/msg/CameraInfo":
            if set(["D", "K", "R", "P"]).issubset(RosCameraInfo.__dataclass_fields__):
                ros_dep_data = {  # ROS1
                    "D": np.asarray(
                        camera_info_data.distortion_parameters, dtype=np.float64
                    ),
                    "K": np.asarray(
                        camera_info_data.intrinsic_parameters, dtype=np.float64
                    ),
                    "R": np.asarray(
                        camera_info_data.rectification_parameters, dtype=np.float64
                    ),
                    "P": np.asarray(
                        camera_info_data.projection_parameters, dtype=np.float64
                    ),
                }
            else:
                ros_dep_data = {  # ROS2
                    "d": np.asarray(
                        camera_info_data.distortion_parameters, dtype=np.float64
                    ),
                    "k": np.asarray(
                        camera_info_data.intrinsic_parameters, dtype=np.float64
                    ),
                    "r": np.asarray(
                        camera_info_data.rectification_parameters, dtype=np.float64
                    ),
                    "p": np.asarray(
                        camera_info_data.projection_parameters, dtype=np.float64
                    ),
                }

            binning = camera_info_data.binning or Vector2d(x=0.0, y=0.0)
            camera_roi = camera_info_data.roi or ROI(
                offset=Vector2d(x=0.0, y=0.0), height=0, width=0, do_rectify=False
            )

            return RosCameraInfo(
                header=ms_header.to_ros(typestore),
                height=camera_info_data.height,
                width=camera_info_data.width,
                distortion_model=camera_info_data.distortion_model,
                **ros_dep_data,
                binning_x=int(binning.x),
                binning_y=int(binning.y),
                roi=ROIAdapter.to_ros(camera_roi, typestore),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


class NavSatStatusAdapter(ROSAdapterBase[GPSStatus]):
    """
    Adapter for translating ROS NavSatStatus messages to Mosaico `GPSStatus`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/NavSatStatus`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatStatus.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/gps_status",
            msg_type="sensor_msgs/msg/NavSatStatus",
            data=
            {
                "status": 0,
                "service": 1,
            }
        )
        # Automatically resolves to a flat Mosaico GPSStatus with attached metadata
        mosaico_gps_status = NavSatStatusAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/NavSatStatus"

    __mosaico_ontology_type__: Type[GPSStatus] = GPSStatus
    _REQUIRED_KEYS = ("status", "service")
    _SCHEMA_METADATA_KEYS_PREFIX = ("STATUS_", "SERVICE_")

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `GPSStatus` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> GPSStatus:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "status": 0,
                "service": 1,
            }
            # Automatically resolves to a flat Mosaico GPSStatus with attached metadata
            mosaico_gps_status = NavSatStatusAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            GPSStatus: The constructed Mosaico GPSStatus object.

        Raises:
            ValueError: If the recursive 'roi' key exists but is not a dict, or if required keys are missing.
        """
        _validate_msgdata(cls, ros_data)
        return GPSStatus(status=ros_data["status"], service=ros_data["service"])

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, GPSStatus],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``GPSStatus`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/NavSatStatus`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``GPSStatus`` instance, or a raw ``GPSStatus``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/NavSatStatus`` is supported.

        Returns:
            A ``sensor_msgs/msg/NavSatStatus`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        gps_status_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosNavSatStatus = typestore.types["sensor_msgs/msg/NavSatStatus"]

        if resolved_rosmsg_type == "sensor_msgs/msg/NavSatStatus":
            return RosNavSatStatus(
                status=gps_status_data.status, service=gps_status_data.service
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        schema_mdata = None
        for schema_mdata_prefix in cls._SCHEMA_METADATA_KEYS_PREFIX:
            if not schema_mdata:
                schema_mdata = {}
            schema_mdata.update(
                {
                    key: val
                    for key, val in ros_data.items()
                    if key.startswith(schema_mdata_prefix)
                }
            )
        return schema_mdata if schema_mdata else None


@register_default_adapter(is_default=True)
class GPSAdapter(ROSAdapterBase[GPS]):
    """
    Adapter for translating ROS NavSatFix messages to Mosaico `GPS`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/NavSatFix`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatFix.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/gps",
            msg_type="sensor_msgs/msg/NavSatFix",
            data=
            {
                "latitude": 45.5,
                "longitude": -122.5,
                "altitude": 100.0,
                "status": {
                    "status": 0,
                    "service": 1,
                },
            }
        )
        # Automatically resolves to a flat Mosaico GPS with attached metadata
        mosaico_gps = GPSAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/NavSatFix"

    __mosaico_ontology_type__: Type[GPS] = GPS
    _REQUIRED_KEYS = ("latitude", "longitude", "altitude", "status")
    _SCHEMA_METADATA_KEYS_PREFIX = ("COVARIANCE_TYPE_",)

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `GPS` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> GPS:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "latitude": 45.5,
                "longitude": -122.5,
                "altitude": 100.0,
                "status": {
                    "status": 0,
                    "service": 1,
                },
            }
            # Automatically resolves to a flat Mosaico GPS with attached metadata
            mosaico_gps = GPSAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            GPS: The constructed Mosaico GPS object.

        Raises:
            ValueError: If the recursive 'roi' key exists but is not a dict, or if required keys are missing.
        """
        _validate_msgdata(cls, ros_data)

        covariance = None
        covariance_type = None
        ros_covariance = ros_data.get("position_covariance")
        if _is_valid_covariance(ros_covariance):
            covariance = ros_covariance
            covariance_type = ros_data.get("position_covariance_type")

        # valid when status.status >= STATUS_FIX (-1)
        status = None
        if ros_data["status"]["status"] >= -1:
            status = NavSatStatusAdapter.from_dict(ros_data["status"])

        return GPS(
            position=Point3d(
                x=ros_data["latitude"],
                y=ros_data["longitude"],
                z=ros_data["altitude"],
                covariance=covariance,
                covariance_type=covariance_type,
            ),
            status=status,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, GPS],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``GPS`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/NavSatFix`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``GPS`` instance, or a raw ``GPS``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/NavSatFix`` is supported.

        Returns:
            A ``sensor_msgs/msg/NavSatFix`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        gps_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosNavSatFix = typestore.types["sensor_msgs/msg/NavSatFix"]

        # NOTE: # status is valid when status >= STATUS_FIX (0).
        gps_status = gps_data.status or GPSStatus(status=-1, service=0)
        position_cov = gps_data.position.covariance or [0.0] * 9
        covariance_type = gps_data.position.covariance_type or 0

        if resolved_rosmsg_type == "sensor_msgs/msg/NavSatFix":
            return RosNavSatFix(
                header=ms_header.to_ros(typestore),
                status=NavSatStatusAdapter.to_ros(gps_status, typestore),
                latitude=gps_data.position.x,
                longitude=gps_data.position.y,
                altitude=gps_data.position.z,
                position_covariance=np.asarray(position_cov, dtype=np.float64),
                position_covariance_type=covariance_type,
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        schema_mdata = {}
        for schema_mdata_prefix in cls._SCHEMA_METADATA_KEYS_PREFIX:
            schema_mdata.update(
                {
                    key: val
                    for key, val in ros_data.items()
                    if key.startswith(schema_mdata_prefix)
                }
            )

        status = ros_data.get("status")
        if status:
            schema_mdata.update({"status": NavSatStatusAdapter.schema_metadata(status)})

        return schema_mdata if schema_mdata else None


@register_default_adapter(is_default=True)
class IMUAdapter(ROSAdapterBase[IMU]):
    """
    Adapter for translating ROS Imu messages to Mosaico `IMU`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/Imu`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Imu.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/imu",
            msg_type="sensor_msgs/msg/Imu",
            data=
            {
                "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 0.0},
                "angular_velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "orientation_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "linear_acceleration_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "angular_velocity_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "header": {"seq": 0, "stamp": {"sec": 0, "nanosec": 0}, "frame_id": "robot_link"},
            }
        )
        # Automatically resolves to a flat Mosaico IMU with attached metadata
        mosaico_imu = IMUAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/Imu"

    __mosaico_ontology_type__: Type[IMU] = IMU
    # These are the fields required by the data platform. The remaining data can be None
    _REQUIRED_KEYS = ("linear_acceleration", "angular_velocity")

    @staticmethod
    def _is_data_available(covariance_list: Optional[List[float]]) -> bool:
        """Checks if an element is provided by the message, e.g. an orientation data is present.
        this is made by checking if the element 0 of the 9-element ROS covariance list equals -1."""
        # ROS often uses tp set covariance_list[0]=-1 to tell if a data is provided in the message
        if not covariance_list:
            return False
        return covariance_list[0] != -1

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `IMU` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> IMU:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 0.0},
                "angular_velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "orientation_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "linear_acceleration_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "angular_velocity_covariance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "header": {"seq": 0, "stamp": {"sec": 0, "nanosec": 0}, "frame_id": "robot_link"},
            }
            # Automatically resolves to a flat Mosaico IMU with attached metadata
            mosaico_imu = IMUAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            IMU: The constructed Mosaico IMU object.

        Raises:
            ValueError: If the recursive 'roi' key exists but is not a dict, or if required keys are missing.
        """
        _validate_msgdata(cls, ros_data)
        # Mandatory Field Conversions (as before)
        accel = Vector3Adapter.from_dict(ros_data["linear_acceleration"])
        angular_vel = Vector3Adapter.from_dict(ros_data["angular_velocity"])

        # Optional Field Conversions (Attitude)
        # Check if the orientation is valid
        orientation = None
        ros_orientation_cov = ros_data.get("orientation_covariance")
        if cls._is_data_available(ros_orientation_cov):
            ori_dict = ros_data.get("orientation")
            orientation = QuaternionAdapter.from_dict(ori_dict) if ori_dict else None
        if orientation and _is_valid_covariance(ros_orientation_cov):
            orientation.covariance = ros_orientation_cov

        # Optional Field Conversions (Covariance)
        ros_acc_cov = ros_data.get("linear_acceleration_covariance")
        if _is_valid_covariance(ros_acc_cov):
            # ROS covariance is a 9-element array (row-major 3x3).
            # Vector9d is assumed to take these 9 elements directly.
            accel.covariance = ros_acc_cov

        ros_ang_vel_cov = ros_data.get("angular_velocity_covariance")
        if _is_valid_covariance(ros_ang_vel_cov):
            angular_vel.covariance = ros_ang_vel_cov

        return IMU(
            acceleration=accel,
            angular_velocity=angular_vel,
            orientation=orientation,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, IMU],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``IMU`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/Imu`` message.

        Args:
            mosaico_data: A ``Message`` wrapping an ``IMU`` instance, or a raw ``IMU``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/Imu`` is supported.

        Returns:
            A ``sensor_msgs/msg/Imu`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        imu_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosImu = typestore.types["sensor_msgs/msg/Imu"]

        # NOTE: # If you have no estimate for one of the data elements,
        # please set element 0 of the associated covariance matrix to -1
        imu_orientation = imu_data.orientation or Quaternion(
            x=0, y=0, z=0, w=0, covariance=[-1] * 9
        )

        orientation_cov = imu_orientation.covariance or [0.0] * 9
        ang_vel_cov = imu_data.angular_velocity.covariance or [0.0] * 9
        lin_acc_cov = imu_data.acceleration.covariance or [0.0] * 9

        if resolved_rosmsg_type == "sensor_msgs/msg/Imu":
            return RosImu(
                header=ms_header.to_ros(typestore),
                orientation=QuaternionAdapter.to_ros(imu_orientation, typestore),
                angular_velocity=Vector3Adapter.to_ros(
                    imu_data.angular_velocity, typestore
                ),
                linear_acceleration=Vector3Adapter.to_ros(
                    imu_data.acceleration, typestore
                ),
                orientation_covariance=np.asarray(orientation_cov, dtype=np.float64),
                angular_velocity_covariance=np.asarray(ang_vel_cov, dtype=np.float64),
                linear_acceleration_covariance=np.asarray(
                    lin_acc_cov, dtype=np.float64
                ),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class NMEASentenceAdapter(ROSAdapterBase[NMEASentence]):
    """
    Adapter for translating ROS NMEASentence messages to Mosaico `NMEASentence`.

    **Supported ROS Types:**

    - [`nmea_msgs/msg/Sentence`](https://docs.ros2.org/foxy/api/nmea_msgs/msg/Sentence.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/gps/fix",
            msg_type="sensor_msgs/msg/GPSFix",
            data=
            {
                "sentence": "GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47",
            }
        )
        # Automatically resolves to a flat Mosaico GPS with attached metadata
        mosaico_gps = NMEASentenceAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "nmea_msgs/msg/Sentence"

    __mosaico_ontology_type__: Type[NMEASentence] = NMEASentence

    _REQUIRED_KEYS = ("sentence",)

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `NMEASenetence` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> NMEASentence:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "sentence": "GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47",
            }
            # Automatically resolves to a flat Mosaico NMEASentence with attached metadata
            mosaico_nmea_sentence = NMEASentenceAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            NMEASentence: The constructed Mosaico NMEASentence object.
        """
        _validate_msgdata(cls, ros_data)
        return NMEASentence(sentence=ros_data["sentence"])

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, NMEASentence],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``NMEASentence`` (or a ``Message`` wrapping one) into a
        ``nmea_msgs/msg/Sentence`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``NMEASentence`` instance, or a raw ``NMEASentence``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``nmea_msgs/msg/Sentence`` is supported.

        Returns:
            A ``nmea_msgs/msg/Sentence`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        nmea_sentence_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosSentence = typestore.types["nmea_msgs/msg/Sentence"]

        if resolved_rosmsg_type == "nmea_msgs/msg/Sentence":
            return RosSentence(
                header=ms_header.to_ros(typestore),
                sentence=nmea_sentence_data.sentence,
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class ImageAdapter(ROSAdapterBase[Image]):
    """
    Adapter for translating ROS Image messages to Mosaico `Image`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/Image`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/Image.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/image",
            msg_type="sensor_msgs/msg/Image",
            data=
            {
                "data": [...],
                "width": 1,
                "height": 1,
                "step": 4,
                "encoding": "bgr8",
            }
        )
        # Automatically resolves to a flat Mosaico Image with attached metadata
        mosaico_image = ImageAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/Image"

    __mosaico_ontology_type__: Type[Image] = Image

    _REQUIRED_KEYS = ("data", "width", "height", "step", "encoding")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Image` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(
        cls,
        ros_data: dict,
        **kwargs: Any,
    ) -> Image:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "data": [...],
                "width": 1,
                "height": 1,
                "step": 4,
                "encoding": "bgr8",
            }
            # Automatically resolves to a flat Mosaico Image with attached metadata
            mosaico_image = ImageAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Image: The constructed Mosaico Image object.
        """
        _validate_msgdata(cls, ros_data)

        return Image.from_linear_pixels(
            data=ros_data["data"],
            # if .get is None, the encode function will use a default format internally
            format=kwargs.get("output_format"),
            width=ros_data["width"],
            height=ros_data["height"],
            stride=ros_data["step"],
            is_bigendian=ros_data.get("is_bigendian"),
            encoding=ros_data["encoding"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Image],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``Image`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/Image`` message.

        Args:
            mosaico_data: A ``Message`` wrapping an ``Image`` instance, or a raw ``Image``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/Image`` is supported.

        Returns:
            A ``sensor_msgs/msg/Image`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        image_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        is_bigendian = image_data.is_bigendian or sys.byteorder == "big"

        # Filling the data
        RosImage = typestore.types["sensor_msgs/msg/Image"]

        if resolved_rosmsg_type == "sensor_msgs/msg/Image":
            return RosImage(
                header=ms_header.to_ros(typestore),
                height=image_data.height,
                width=image_data.width,
                encoding=image_data.encoding,
                is_bigendian=int(is_bigendian),
                step=image_data.stride,
                data=np.frombuffer(
                    bytes(image_data.to_linear_pixels()), dtype=np.uint8
                ),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class CompressedImageAdapter(ROSAdapterBase[CompressedImage]):
    """
    Adapter for translating ROS CompressedImage messages to Mosaico `CompressedImage`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/CompressedImage`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/CompressedImage.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/compressed_image",
            msg_type="sensor_msgs/msg/CompressedImage",
            data=
            {
                "data": [...],
                "format": "jpeg",
            }
        )
        # Automatically resolves to a flat Mosaico CompressedImage with attached metadata
        mosaico_compressed_image = CompressedImageAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/CompressedImage"

    __mosaico_ontology_type__: Type[CompressedImage] = CompressedImage
    _REQUIRED_KEYS = ("data", "format")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `CompressedImage` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(
        cls,
        ros_data: dict,
    ) -> CompressedImage:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "data": [...],
                "format": "jpeg",
            }
            # Automatically resolves to a flat Mosaico CompressedImage with attached metadata
            mosaico_compressed_image = CompressedImageAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            CompressedImage: The constructed Mosaico CompressedImage object.
        """
        _validate_msgdata(cls, ros_data)

        return CompressedImage(data=bytes(ros_data["data"]), format=ros_data["format"])

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, CompressedImage],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``CompressedImage`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/CompressedImage`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``CompressedImage`` instance, or a raw ``CompressedImage``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/CompressedImage`` is supported.

        Returns:
            A ``sensor_msgs/msg/CompressedImage`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        compressed_image_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosCompressedImage = typestore.types["sensor_msgs/msg/CompressedImage"]

        if resolved_rosmsg_type == "sensor_msgs/msg/CompressedImage":
            return RosCompressedImage(
                header=ms_header.to_ros(typestore),
                format=compressed_image_data.format,
                data=np.frombuffer(compressed_image_data.data, dtype=np.uint8),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class ROIAdapter(ROSAdapterBase[ROI]):
    """
    Adapter for translating ROS RegionOfInterest messages to Mosaico `ROI`.

    **Supported ROS Types:**

    - [`sensor_msgs/RegionOfInterest`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/RegionOfInterest.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/roi",
            msg_type="sensor_msgs/msg/RegionOfInterest",
            data=
            {
                "height": 1,
                "width": 1,
                "x_offset": 0,
                "y_offset": 0,
            }
        )
        # Automatically resolves to a flat Mosaico ROI with attached metadata
        mosaico_roi = ROIAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/RegionOfInterest"

    __mosaico_ontology_type__: Type[ROI] = ROI

    _REQUIRED_KEYS = ("height", "width", "x_offset", "y_offset")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `ROI` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> ROI:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "height": 1,
                "width": 1,
                "x_offset": 0,
                "y_offset": 0,
            }
            # Automatically resolves to a flat Mosaico ROI with attached metadata
            mosaico_roi = ROIAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            ROI: The constructed Mosaico ROI object.
        """
        _validate_msgdata(cls, ros_data)

        return ROI(
            offset=Vector2d(x=ros_data["x_offset"], y=ros_data["y_offset"]),
            height=ros_data["height"],
            width=ros_data["width"],
            do_rectify=ros_data.get("do_rectify"),
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, ROI],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``ROI`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/RegionOfInterest`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``ROI`` instance, or a raw ``ROI``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/RegionOfInterest`` is supported.

        Returns:
            A ``sensor_msgs/msg/RegionOfInterest`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        roi_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        do_rectify = roi_data.do_rectify or False

        # Filling the data
        RosRegionOfInterest = typestore.types["sensor_msgs/msg/RegionOfInterest"]

        if resolved_rosmsg_type == "sensor_msgs/msg/RegionOfInterest":
            return RosRegionOfInterest(
                x_offset=int(roi_data.offset.x),
                y_offset=int(roi_data.offset.y),
                height=roi_data.height,
                width=roi_data.width,
                do_rectify=do_rectify,
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class BatteryStateAdapter(ROSAdapterBase[BatteryState]):
    """
    Adapter for translating ROS BatteryState messages to Mosaico `BatteryState`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/BatteryState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/battery_state",
            msg_type="sensor_msgs/msg/BatteryState",
            data=
            {
                "voltage": 12.6,
                "capacity": 100,
                "cell_temperature": 25,
                "cell_voltage": [12.6],
                "location": "battery",
                "charge": 100,
                "current": 0,
                "design_capacity": 100,
                "location": "battery",
                "percentage": 100,
                "power_supply_health": "good",
                "power_supply_status": "charging",
                "power_supply_technology": "li-ion",
                "present": True,
                "serial_number": "1234567890",
                "temperature": 25,
            }
        )
        # Automatically resolves to a flat Mosaico BatteryState with attached metadata
        mosaico_battery_state = BatteryStateAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/BatteryState"

    __mosaico_ontology_type__: Type[BatteryState] = BatteryState
    _REQUIRED_KEYS = (
        "voltage",
        "capacity",
        "cell_temperature",
        "cell_voltage",
        "location",
        "charge",
        "current",
        "design_capacity",
        "location",
        "percentage",
        "power_supply_health",
        "power_supply_status",
        "power_supply_technology",
        "present",
        "serial_number",
        "temperature",
    )
    _SCHEMA_METADATA_KEYS_PREFIX = ("POWER_SUPPLY_",)

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `BatteryState` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> BatteryState:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data=
            {
                "voltage": 12.6,
                "capacity": 100,
                "cell_temperature": 25,
                "cell_voltage": [12.6],
                "location": "battery",
                "charge": 100,
                "current": 0,
                "design_capacity": 100,
                "location": "battery",
                "percentage": 100,
                "power_supply_health": "good",
                "power_supply_status": "charging",
                "power_supply_technology": "li-ion",
                "present": True,
                "serial_number": "1234567890",
                "temperature": 25,
            }
            # Automatically resolves to a flat Mosaico BatteryState with attached metadata
            mosaico_battery_state = BatteryStateAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            BatteryState: The constructed Mosaico BatteryState object.
        """
        _validate_msgdata(cls, ros_data)

        temperature = (
            ros_data["temperature"] if not math.isnan(ros_data["temperature"]) else None
        )
        current = ros_data["current"] if not math.isnan(ros_data["current"]) else None
        charge = ros_data["charge"] if not math.isnan(ros_data["charge"]) else None
        capacity = (
            ros_data["capacity"] if not math.isnan(ros_data["capacity"]) else None
        )
        design_capacity = (
            ros_data["design_capacity"]
            if not math.isnan(ros_data["design_capacity"])
            else None
        )
        cell_voltage = ros_data["cell_voltage"] if ros_data["cell_voltage"] else None
        cell_temperature = (
            ros_data["cell_temperature"] if ros_data["cell_temperature"] else None
        )

        return BatteryState(
            voltage=ros_data["voltage"],
            temperature=temperature,
            current=current,
            charge=charge,
            capacity=capacity,
            design_capacity=design_capacity,
            percentage=ros_data["percentage"],
            power_supply_status=ros_data["power_supply_status"],
            power_supply_health=ros_data["power_supply_health"],
            power_supply_technology=ros_data["power_supply_technology"],
            present=ros_data["present"],
            cell_voltage=cell_voltage,
            cell_temperature=cell_temperature,
            location=ros_data["location"],
            serial_number=ros_data["serial_number"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, BatteryState],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``BatteryState`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/BatteryState`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``BatteryState`` instance, or a raw ``BatteryState``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/BatteryState`` is supported.

        Returns:
            A ``sensor_msgs/msg/BatteryState`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        battery_state_data, header = cls.unpack_mosaico_msg(mosaico_data)

        # (If unmeasured NaN)
        battery_temperature = battery_state_data.temperature or math.nan
        battery_current = battery_state_data.current or math.nan
        battery_charge = battery_state_data.charge or math.nan
        battery_capacity = battery_state_data.capacity or math.nan
        battery_design_capacity = battery_state_data.design_capacity or math.nan
        battery_cell_voltage = battery_state_data.cell_voltage or []
        battery_cell_temperature = battery_state_data.cell_temperature or []

        # Filling the data
        RosBatteryState = typestore.types["sensor_msgs/msg/BatteryState"]

        if resolved_rosmsg_type == "sensor_msgs/msg/BatteryState":
            return RosBatteryState(
                header=header.to_ros(typestore),
                voltage=battery_state_data.voltage,
                temperature=battery_temperature,
                current=battery_current,
                charge=battery_charge,
                capacity=battery_capacity,
                design_capacity=battery_design_capacity,
                percentage=battery_state_data.percentage,
                power_supply_status=battery_state_data.power_supply_status,
                power_supply_health=battery_state_data.power_supply_health,
                power_supply_technology=battery_state_data.power_supply_technology,
                present=battery_state_data.present,
                cell_voltage=np.asarray(battery_cell_voltage, dtype=np.float32),
                cell_temperature=np.asarray(battery_cell_temperature, dtype=np.float32),
                location=battery_state_data.location,
                serial_number=battery_state_data.serial_number,
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        schema_mdata = {}
        for schema_mdata_prefix in cls._SCHEMA_METADATA_KEYS_PREFIX:
            schema_mdata.update(
                {
                    key: val
                    for key, val in ros_data.items()
                    if key.startswith(schema_mdata_prefix)
                }
            )

        status = ros_data.get("status")
        if status:
            schema_mdata.update({"status": NavSatStatusAdapter.schema_metadata(status)})

        return schema_mdata if schema_mdata else None


@register_default_adapter(is_default=True)
class RobotJointAdapter(ROSAdapterBase[RobotJoint]):
    """
    Adapter for translating ROS JointState messages to Mosaico `RobotJoint`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/JointState`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/JointState.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/joint_states",
            msg_type="sensor_msgs/msg/JointState",
            data={
                "header": {
                    "stamp": {
                        "sec": 17000,
                        "nanosec": 0,
                    },
                    "frame_id": "",
                },
                "name": ["joint1", "joint2"],
                "position": [0.0, 0.0],
                "velocity": [0.0, 0.0],
                "effort": [0.0, 0.0],
            },
        )
        # Automatically resolves to a flat Mosaico RobotJoint with attached metadata
        mosaico_robot_joint = RobotJointAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/JointState"
    __mosaico_ontology_type__: Type[RobotJoint] = RobotJoint
    _REQUIRED_KEYS = ("name", "position", "velocity", "effort")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `RobotJoint` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> RobotJoint:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data={
                "header": {
                    "stamp": {
                        "sec": 17000,
                        "nanosec": 0,
                    },
                    "frame_id": "",
                },
                "name": ["joint1", "joint2"],
                "position": [0.0, 0.0],
                "velocity": [0.0, 0.0],
                "effort": [0.0, 0.0],
            }
            # Automatically resolves to a flat Mosaico RobotJoint with attached metadata
            mosaico_robot_joint = RobotJointAdapter.from_dict(ros_data)
            ```
        """
        _validate_msgdata(cls, ros_data)
        return RobotJoint(
            names=ros_data["name"],
            positions=ros_data["position"],
            velocities=ros_data["velocity"],
            efforts=ros_data["effort"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, RobotJoint],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``RobotJoint`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/JointState`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``RobotJoint`` instance, or a raw ``RobotJoint``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/JointState`` is supported.

        Returns:
            A ``sensor_msgs/msg/JointState`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        robot_joint_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosJointState = typestore.types["sensor_msgs/msg/JointState"]

        if resolved_rosmsg_type == "sensor_msgs/msg/JointState":
            return RosJointState(
                header=ms_header.to_ros(typestore),
                name=robot_joint_data.names,
                position=np.asarray(robot_joint_data.positions, dtype=np.float64),
                velocity=np.asarray(robot_joint_data.velocities, dtype=np.float64),
                effort=np.asarray(robot_joint_data.efforts, dtype=np.float64),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


PointCloudModel = TypeVar(
    "PointCloudModel",
    bound=Serializable,
)


class PointCloudAdapterBase(ROSAdapterBase[PointCloudModel]):
    """
    Base adapter for translating ROS PointCloud2 message to Mosaico specific ontology.
    """

    ros_msgtype: str = "sensor_msgs/msg/PointCloud2"

    _REQUIRED_KEYS = (
        "height",
        "width",
        "fields",
        "is_bigendian",
        "point_step",
        "row_step",
        "data",
        "is_dense",
    )

    _REQUIRED_FIELDS: list[str] = []

    _FROM_POINTCLOUD_MAP = {
        PointFieldDataType.INT8: (np.dtype(np.int8).itemsize, np.int8),
        PointFieldDataType.UINT8: (np.dtype(np.uint8).itemsize, np.uint8),
        PointFieldDataType.INT16: (np.dtype(np.int16).itemsize, np.int16),
        PointFieldDataType.UINT16: (np.dtype(np.uint16).itemsize, np.uint16),
        PointFieldDataType.INT32: (np.dtype(np.int32).itemsize, np.int32),
        PointFieldDataType.UINT32: (np.dtype(np.uint32).itemsize, np.uint32),
        PointFieldDataType.FLOAT32: (np.dtype(np.float32).itemsize, np.float32),
        PointFieldDataType.FLOAT64: (np.dtype(np.float64).itemsize, np.float64),
    }

    _TO_POINTCLOUD_MAP = {
        np.int8: (np.dtype(np.int8).itemsize, PointFieldDataType.INT8),
        np.uint8: (np.dtype(np.uint8).itemsize, PointFieldDataType.UINT8),
        np.int16: (np.dtype(np.int16).itemsize, PointFieldDataType.INT16),
        np.uint16: (np.dtype(np.uint16).itemsize, PointFieldDataType.UINT16),
        np.int32: (np.dtype(np.int32).itemsize, PointFieldDataType.INT32),
        np.uint32: (np.dtype(np.uint32).itemsize, PointFieldDataType.UINT32),
        np.float32: (np.dtype(np.float32).itemsize, PointFieldDataType.FLOAT32),
        np.float64: (np.dtype(np.float64).itemsize, PointFieldDataType.FLOAT64),
    }

    @classmethod
    def _extract_pa_list_type(cls, field_name: str):
        """
        Introspects the PyArrow list element type annotation for a named field on
        the Mosaico ontology model.

        Used by :meth:`encode` to determine the NumPy dtype when serializing each
        PointCloud field into the binary buffer.

        Args:
            field_name: The model attribute name to inspect (e.g. ``"x"``, ``"intensity"``).

        Returns:
            The PyArrow ``list_`` annotation object for the field
            (e.g. ``pa.list_(pa.float32())``).

        Raises:
            NotImplementedError: If the field annotation is a Union with multiple
                non-None types.
        """
        field_info = cls.__mosaico_ontology_type__.model_fields[field_name]

        # Required field: Pydantic extracts the Annotated metadata directly.
        if field_info.metadata:
            return field_info.metadata[0]

        origin = get_origin(field_info.annotation)
        args = get_args(field_info.annotation)

        if origin is Union:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return non_none[0].__metadata__[0]
            raise NotImplementedError(
                f"Union with multiple types is not supported: {args}"
            )

        return None

    @classmethod
    def decode(cls, ros_data: dict) -> dict[str, list]:
        """
        Deserialize the binary buffer of a ROS [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html) message into named field arrays.

        Args:
            ros_data: Raw ROS message payload. Relevant keys: `data`, `height`, `width`,
                    `fields`, `is_bigendian`, `point_step`.

        Returns:
            Dictionary mapping each field name to a list of decoded values. Returns empty lists for all fields if `height * width == 0`.

        Raises:
            ValueError: If a field exposes an unsupported `datatype`.

        """
        _validate_msgdata(cls, ros_data)

        height = ros_data["height"]
        width = ros_data["width"]
        fields = [PointField(**f) for f in ros_data["fields"]]
        is_bigendian = ros_data["is_bigendian"]
        point_step = ros_data["point_step"]
        data = bytes(ros_data["data"])

        num_points = height * width

        if num_points == 0:
            return {field.name: [] for field in fields}

        endian_prefix = ">" if is_bigendian else "<"

        raw = np.frombuffer(bytes(data), dtype=np.uint8).reshape(num_points, point_step)

        result = {}
        for field in fields:
            itemsize, np_dtype = cls._FROM_POINTCLOUD_MAP.get(
                field.datatype, (None, None)
            )

            if np_dtype is None:
                raise ValueError(
                    f"field datatype = {field.datatype} not supported. Supported data types: {cls._FROM_POINTCLOUD_MAP.items()}"
                )

            dtype = np.dtype(np_dtype).newbyteorder(endian_prefix)

            field_raw = np.ascontiguousarray(
                raw[:, field.offset : field.offset + itemsize * field.count]
            )
            values = field_raw.view(dtype)
            if field.count == 1:
                values = values.reshape(num_points)

            result[field.name] = values.astype(np_dtype, copy=False)

        return {name: arr.tolist() for name, arr in result.items()}

    @classmethod
    def encode(cls, pcl_model: dict[str, list]) -> dict:
        """
        Serializes a dictionary of named field arrays into the binary layout required
        by a ``sensor_msgs/msg/PointCloud2`` message. This is the inverse operation of :meth:`decode`.

        Args:
            pcl_model: Mapping from field name to a list of scalar values, one per
                point. ``None``-valued fields should be excluded before calling
                (e.g., via ``model_dump(exclude_none=True)``).

        Returns:
            A dictionary with all keys required to populate a ``PointCloud2`` message:
            ``height``, ``width``, ``fields``, ``is_bigendian``, ``point_step``,
            ``row_step``, ``data``, and ``is_dense``.
        """

        is_bigendian = sys.byteorder == "big"
        out: dict = {
            "height": 1,
            "width": None,
            "fields": [],
            "is_bigendian": is_bigendian,
            "point_step": None,
            "row_step": None,
            "data": None,
            "is_dense": True,
        }

        pcl_field_names = pcl_model.keys()
        pcl_field_values = pcl_model.values()

        n_points = len(next(iter(pcl_field_values)))
        out["width"] = n_points

        # Dictionary necessary to later setup the points as np.array structure
        point_field_dict: dict = {"names": [], "formats": [], "offsets": []}

        offset = 0
        for field_name in pcl_field_names:
            pa_datatype = cls._extract_pa_list_type(field_name)
            np_type = pa_datatype.value_type.to_pandas_dtype()

            point_field_dict["names"].append(field_name)
            point_field_dict["formats"].append(np_type)
            point_field_dict["offsets"].append(offset)

            bytesize, pf_datatype = cls._TO_POINTCLOUD_MAP[np_type]

            out["fields"].append(
                {
                    "name": field_name,
                    "offset": offset,
                    "datatype": pf_datatype,
                    "count": 1,
                }
            )

            offset += bytesize
        out["point_step"] = offset
        out["row_step"] = offset * n_points  # Only 1 row

        endian_prefix = ">" if is_bigendian else "<"
        dtype = np.dtype(point_field_dict).newbyteorder(endian_prefix)

        points = list(zip(*pcl_field_values))
        buffer = np.array(points, dtype=dtype).view(np.uint8)
        out["data"] = buffer

        return out

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, PointCloudModel],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``PointCloudModel`` subtype (or a ``Message`` wrapping one)
        into a ``sensor_msgs/msg/PointCloud2`` message.

        Fields are serialized via :meth:`encode`. Only non-``None`` model fields are
        included in the output point cloud.

        Args:
            mosaico_data: A ``Message`` wrapping a ``PointCloudModel`` instance, or a
                raw ``PointCloudModel`` directly.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/PointCloud2`` is supported.

        Returns:
            A ``sensor_msgs/msg/PointCloud2`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        pcl_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPointField = typestore.types["sensor_msgs/msg/PointField"]
        RosPointCloud2 = typestore.types["sensor_msgs/msg/PointCloud2"]

        # Exclude None fields; they won't appear in the PointCloud2 fields list.
        model = pcl_data.model_dump(exclude_none=True)
        pcl_dict = cls.encode(model)

        pointcloud = RosPointCloud2(
            header=ms_header.to_ros(typestore),
            height=pcl_dict["height"],
            width=pcl_dict["width"],
            fields=[RosPointField(**field) for field in pcl_dict["fields"]],
            is_bigendian=pcl_dict["is_bigendian"],
            point_step=pcl_dict["point_step"],
            row_step=pcl_dict["row_step"],
            data=pcl_dict["data"],
            is_dense=pcl_dict["is_dense"],
        )

        if resolved_rosmsg_type == "sensor_msgs/msg/PointCloud2":
            return pointcloud

        return None

    @classmethod
    @abstractmethod
    def _build(cls, decoded_fields: dict[str, list]) -> PointCloudModel: ...

    @classmethod
    def from_dict(cls, ros_data: dict) -> PointCloudModel:
        """
        Convert a raw ROS PointCloud2 message dictionary into a typed Mosaico model.

        Args:
            ros_data: Raw ROS message payload as a dictionary.

        Returns:
            A fully populated instance of the target `PointCloudModel` subtype (e.g. `Lidar`, `Radar`, `RGBDCamera`, ...).

        Raises:
            ValueError: If any required message key is missing from `ros_data`,
                or if any required field is absent after decoding.

        Note:
            This method is **not** meant to be overridden in most subclasses.
            Only [`PointCloudAdapter`][mosaicolabs.ros_bridge.adapters.sensor_msgs.PointCloudAdapter] overrides it,
            as it operates at a different abstraction level, returning the raw
            [`PointCloud2`][mosaicolabs.ros_bridge.data_ontology.PointCloud2] message instead of a decoded model.
        """
        decoded_fields = cls.decode(ros_data)
        _validate_required_fields(cls, cls._REQUIRED_FIELDS, decoded_fields)
        return cls._build(decoded_fields)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class PointCloudAdapter(PointCloudAdapterBase[PointCloud2]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `PointCloud2`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    Example:
    ```python
    ros_msg = ROSMessage(
                timestamp=17000,
                topic="/point_cloud",
                msg_type="sensor_msgs/PointCloud2",
                data = {
                    "height": 1,
                    "width": 3,
                    "fields": [
                        {"name": "x", "offset": 0,  "datatype": 7, "count": 1},
                        {"name": "y", "offset": 4,  "datatype": 7, "count": 1},
                        {"name": "z", "offset": 8,  "datatype": 7, "count": 1},
                    ],
                "is_bigendian": False,
                "point_step": 12,
                "row_step": 36,
                "data": ...,
                "is_dense": True,
                }
            )
    # Automatically resolves to a flat Mosaico PointCloud2 with attached metadata
    mosaico_point_cloud = PointCloudAdapter.translate(ros_msg)
    ```
    """

    __mosaico_ontology_type__: Type[PointCloud2] = PointCloud2

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> PointCloud2:
        raise NotImplementedError("PointCloudAdapter uses from_dict directly.")

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `PointCloud2` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> PointCloud2:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
        ```python
        ros_data = {
            "height": 1,           # unorganized point cloud = 1 row
            "width": 3,            # 3 points
            "fields": [
                {
                    "name": "x",
                    "offset": 0,
                    "datatype": 7,  # FLOAT32
                    "count": 1
                },
                {
                    "name": "y",
                    "offset": 4,
                    "datatype": 7,  # FLOAT32
                    "count": 1
                },
                {
                    "name": "z",
                    "offset": 8,
                    "datatype": 7,  # FLOAT32
                    "count": 1
                },
            ],
            "is_bigendian": False,
            "point_step": 12,      # 3 fields * 4 bytes (float32) = 12 bytes per point
            "row_step": 36,        # point_step * width = 12 * 3 = 36 bytes per row
            "data": ...,
            "is_dense": True
            }
        ```
        """

        _validate_msgdata(cls, ros_data)

        return PointCloud2(
            height=ros_data["height"],
            width=ros_data["width"],
            fields=[PointField(**f) for f in ros_data["fields"]],
            is_bigendian=ros_data["is_bigendian"],
            point_step=ros_data["point_step"],
            row_step=ros_data["row_step"],
            data=bytes(ros_data["data"]),
            is_dense=ros_data["is_dense"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, PointCloud2],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``PointCloud2`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/PointCloud2`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``PointCloud2`` instance, or a raw ``PointCloud2``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/PointCloud2`` is supported.

        Returns:
            A ``sensor_msgs/msg/PointCloud2`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        pointcloud_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPointField = typestore.types["sensor_msgs/msg/PointField"]
        RosPointCloud2 = typestore.types["sensor_msgs/msg/PointCloud2"]

        point_fields = [
            RosPointField(
                name=field.name,
                offset=field.offset,
                datatype=field.datatype,
                count=field.count,
            )
            for field in pointcloud_data.fields
        ]

        pose = RosPointCloud2(
            header=ms_header.to_ros(typestore),
            height=pointcloud_data.height,
            width=pointcloud_data.width,
            fields=point_fields,
            is_bigendian=pointcloud_data.is_bigendian,
            point_step=pointcloud_data.point_step,
            row_step=pointcloud_data.row_step,
            data=np.frombuffer(pointcloud_data.data, dtype=np.uint8),
            is_dense=pointcloud_data.is_dense,
        )

        if resolved_rosmsg_type == "sensor_msgs/msg/PointCloud2":
            return pose

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


_LT = TypeVar("_LT", LaserScan, MultiEchoLaserScan)


class LaserScannerAdapterBase(ROSAdapterBase[_LT]):
    """
    Base adapter for translating ROS LaserScan and MultiEchoLaserScan messages to Mosaico `LaserScan` and `MultiEchoLaserScan` .
    """

    _REQUIRED_KEYS = (
        "angle_min",
        "angle_max",
        "angle_increment",
        "time_increment",
        "scan_time",
        "range_min",
        "range_max",
        "ranges",
        "intensities",
    )

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `LaserScan` or `MultiEchoLaserScan` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_default_adapter(is_default=True)
class LaserScanAdapter(LaserScannerAdapterBase[LaserScan]):
    """
    Adapter for translating ROS LaserScan messages to Mosaico `LaserScan`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/LaserScan`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/LaserScan.html)

    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/LaserScan"

    __mosaico_ontology_type__: Type[LaserScan] = LaserScan

    @classmethod
    def from_dict(cls, ros_data: dict) -> LaserScan:
        """
        Create a LaserScan instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "angle_min": -1.57,
            "angle_max":  1.57,
            "angle_increment": 0.01,
            "time_increment": 0.0,
            "scan_time": 0.1,
            "range_min": 0.2,
            "range_max": 10.0,
            "ranges": [1.0, 1.1, 1.2],
            "intensities": [100.0, 110.0, 120.0],
        }
        # Automatically resolves to a flat Mosaico LaserScan with attached data
        mosaico_laser_scan = LaserScanAdapter.from_dict(ros_data)
        ```
        """
        intensities = ros_data["intensities"] if ros_data["intensities"] else None

        _validate_msgdata(cls, ros_data)
        return cls.__mosaico_ontology_type__(
            angle_min=ros_data["angle_min"],
            angle_max=ros_data["angle_max"],
            angle_increment=ros_data["angle_increment"],
            time_increment=ros_data["time_increment"],
            scan_time=ros_data["scan_time"],
            range_min=ros_data["range_min"],
            range_max=ros_data["range_max"],
            ranges=ros_data["ranges"],
            intensities=intensities,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, LaserScan],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``LaserScan`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/LaserScan`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``LaserScan`` instance, or a raw ``LaserScan``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/LaserScan`` is supported.

        Returns:
            A ``sensor_msgs/msg/LaserScan`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        laser_scanner_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosLaserScanner = typestore.types["sensor_msgs/msg/LaserScan"]

        if resolved_rosmsg_type == "sensor_msgs/msg/LaserScan":
            return RosLaserScanner(
                header=ms_header.to_ros(typestore),
                angle_min=laser_scanner_data.angle_min,
                angle_max=laser_scanner_data.angle_max,
                angle_increment=laser_scanner_data.angle_increment,
                time_increment=laser_scanner_data.time_increment,
                scan_time=laser_scanner_data.scan_time,
                range_min=laser_scanner_data.range_min,
                range_max=laser_scanner_data.range_max,
                ranges=np.asarray(laser_scanner_data.ranges, dtype=np.float32),
                intensities=np.asarray(
                    laser_scanner_data.intensities or [], dtype=np.float32
                ),
            )

        return None


@register_default_adapter(is_default=True)
class MultiEchoLaserScanAdapter(LaserScannerAdapterBase[MultiEchoLaserScan]):
    """
    Adapter for translating ROS MultiEchoLaserScan messages to Mosaico `MultiEchoLaserScan`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/MultiEchoLaserScan`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/MultiEchoLaserScan.html)

    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/MultiEchoLaserScan"

    __mosaico_ontology_type__: Type[MultiEchoLaserScan] = MultiEchoLaserScan

    @classmethod
    def from_dict(cls, ros_data: dict) -> MultiEchoLaserScan:
        """
        Create a MultiEchoLaserScan instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "angle_min": -1.57,
            "angle_max":  1.57,
            "angle_increment": 0.01,
            "time_increment": 0.0,
            "scan_time": 0.1,
            "range_min": 0.2,
            "range_max": 10.0,
            "ranges": [{echoes: [1.0, 1.1, 1.2]}, {echoes: [2.0, 2.1, 2.2]}, {echoes: [3.0, 3.1, 3.2]}],
            "intensities": [{echoes: [100.0, 110.0, 120.0]}, {echoes: [200.0, 210.0, 220.0]}, {echoes: [300.0, 310.0, 320.0]}],
        }
        # Automatically resolves to a flat Mosaico MultiEchoLaserScanAdapter with attached data
        mosaico_laser_scan = MultiEchoLaserScanAdapter.from_dict(ros_data)
        ```
        """
        ranges = [x["echoes"] for x in ros_data["ranges"]]
        intensities = (
            [x["echoes"] for x in ros_data["intensities"]]
            if ros_data["intensities"]
            else None
        )

        _validate_msgdata(cls, ros_data)
        return cls.__mosaico_ontology_type__(
            angle_min=ros_data["angle_min"],
            angle_max=ros_data["angle_max"],
            angle_increment=ros_data["angle_increment"],
            time_increment=ros_data["time_increment"],
            scan_time=ros_data["scan_time"],
            range_min=ros_data["range_min"],
            range_max=ros_data["range_max"],
            ranges=ranges,
            intensities=intensities,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, MultiEchoLaserScan],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``MultiEchoLaserScan`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/MultiEchoLaserScan`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``MultiEchoLaserScan`` instance, or a raw ``MultiEchoLaserScan``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/MultiEchoLaserScan`` is supported.

        Returns:
            A ``sensor_msgs/msg/MultiEchoLaserScan`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        multi_laser_scanner_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosLaserEcho = typestore.types["sensor_msgs/msg/LaserEcho"]
        RosMultiEchoLaserScanner = typestore.types["sensor_msgs/msg/MultiEchoLaserScan"]

        ranges_echoes = [
            RosLaserEcho(echoes=np.asarray(scan, dtype=np.float32))
            for scan in multi_laser_scanner_data.ranges
        ]

        intensities_echoes = [
            RosLaserEcho(echoes=np.asarray(scan, dtype=np.float32))
            for scan in multi_laser_scanner_data.intensities or []
        ]

        if resolved_rosmsg_type == "sensor_msgs/msg/MultiEchoLaserScan":
            return RosMultiEchoLaserScanner(
                header=ms_header.to_ros(typestore),
                angle_min=multi_laser_scanner_data.angle_min,
                angle_max=multi_laser_scanner_data.angle_max,
                angle_increment=multi_laser_scanner_data.angle_increment,
                time_increment=multi_laser_scanner_data.time_increment,
                scan_time=multi_laser_scanner_data.scan_time,
                range_min=multi_laser_scanner_data.range_min,
                range_max=multi_laser_scanner_data.range_max,
                ranges=ranges_echoes,
                intensities=intensities_echoes,
            )

        return None


@register_default_adapter(is_default=True)
class MagneticFieldAdapter(ROSAdapterBase[Magnetometer]):
    """
    Adapter for translating ROS MagneticField messages to Mosaico `Magnetometer`.

    **Supported ROS Types:**

    - `sensor_msgs/msg/MagneticField`

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/magnetic_field",
            msg_type="sensor_msgs/msg/MagneticField",
            data={
                "magnetic_field": {
                    "x": 0.12,
                    "y": -0.05,
                    "z": 0.98,
                }
            }
        )

        mosaico_magnetometer = MagneticFieldAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/MagneticField"

    __mosaico_ontology_type__: Type[Magnetometer] = Magnetometer

    _REQUIRED_KEYS = ("magnetic_field",)

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Magnetometer` object.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Magnetometer:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Magnetometer: The constructed Mosaico Magnetometer object.
        """
        _validate_msgdata(cls, ros_data)

        field = ros_data["magnetic_field"]

        mag = Vector3d(
            x=field["x"],
            y=field["y"],
            z=field["z"],
        )

        cov = ros_data.get("magnetic_field_covariance")
        if _is_valid_covariance(cov):
            mag.covariance = cov

        return Magnetometer(magnetic_field=mag)

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Magnetometer],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``Magnetometer`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/MagneticField`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``Magnetometer`` instance, or a raw ``Magnetometer``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/MagneticField`` is supported.

        Returns:
            A ``sensor_msgs/msg/MagneticField`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        magnetic_field_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosMagneticField = typestore.types["sensor_msgs/msg/MagneticField"]
        cov = magnetic_field_data.magnetic_field.covariance or [0.0] * 9

        if resolved_rosmsg_type == "sensor_msgs/msg/MagneticField":
            return RosMagneticField(
                header=ms_header.to_ros(typestore),
                magnetic_field=Vector3Adapter.to_ros(
                    magnetic_field_data.magnetic_field, typestore
                ),
                magnetic_field_covariance=np.asarray(cov, dtype=np.float64),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        MagneticField messages typically do not include additional schema metadata,
        so this returns None unless extended in the future.
        """
        return None


@register_default_adapter(is_default=True)
class JoyAdapter(ROSAdapterBase[Joy]):
    """
    Adapter for translating ROS Joy messages to Mosaico `Joy`.

    **Supported ROS Types:**

    - `sensor_msgs/msg/Joy`

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/joy",
            msg_type="sensor_msgs/msg/Joy",
            data={
                # Axes and buttons are list-based fields (not queryable via `.Q`)
                "axes": [0.0, -1.0, 0.5],
                "buttons": [0, 1, 0, 1],
            }
        )

        mosaico_joy = JoyAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/Joy"

    __mosaico_ontology_type__: Type[Joy] = Joy

    _REQUIRED_KEYS = (
        "axes",
        "buttons",
    )

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Joy` object.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Joy:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Joy: The constructed Mosaico Joy object.
        """
        _validate_msgdata(cls, ros_data)

        return Joy(
            axes=ros_data.get("axes", []),
            buttons=ros_data.get("buttons", []),
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Joy],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``Joy`` (or a ``Message`` wrapping one) into a
        ``sensor_msgs/msg/Joy`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``Joy`` instance, or a raw ``Joy``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``sensor_msgs/msg/Joy`` is supported.

        Returns:
            A ``sensor_msgs/msg/Joy`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        joy_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosJoy = typestore.types["sensor_msgs/msg/Joy"]

        if resolved_rosmsg_type == "sensor_msgs/msg/Joy":
            return RosJoy(
                header=ms_header.to_ros(typestore),
                axes=np.asarray(joy_data.axes, dtype=np.float32),
                buttons=np.asarray(joy_data.buttons, dtype=np.int32),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Joy messages typically do not include additional schema metadata,
        so this returns None unless extended in the future.
        """
        return None
