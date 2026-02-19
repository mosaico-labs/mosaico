from typing import Any, List, Optional, Tuple, Type
from mosaicolabs.models.data import Point3d, Vector2d, ROI
from mosaicolabs.models import Message
from mosaicolabs.models.sensors import (
    CameraInfo,
    GPS,
    GPSStatus,
    NMEASentence,
    CompressedImage,
    Image,
    IMU,
    RobotJoint,
)

from .geometry_msgs import (
    QuaternionAdapter,
    Vector3Adapter,
)
from ..data_ontology import BatteryState
from ..ros_message import ROSMessage
from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_adapter

from .helpers import _make_header, _validate_msgdata


@register_adapter
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

        # Manage differences between ROS1 and ROS2s
        return CameraInfo(
            header=_make_header(ros_data.get("header")),
            height=ros_data["height"],
            width=ros_data["width"],
            binning=Vector2d(
                x=ros_data["binning_x"],
                y=ros_data["binning_y"],
            ),
            distortion_model=ros_data["distortion_model"],
            distortion_parameters=ros_data.get("d") or ros_data.get("D"),
            intrinsic_parameters=ros_data.get("k") or ros_data.get("K"),
            projection_parameters=ros_data.get("p") or ros_data.get("P"),
            rectification_parameters=ros_data.get("r") or ros_data.get("R"),
            roi=ROIAdapter.from_dict(ros_data["roi"]),
        )

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

    - [`sensor_msgs/msg/NavSatFix`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/NavSatFix.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/gps_status",
            msg_type="sensor_msgs/msg/NavSatFix",
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

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/NavSatFix"

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


@register_adapter
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

        return GPS(
            header=_make_header(ros_data.get("header")),
            position=Point3d(
                x=ros_data["latitude"],
                y=ros_data["longitude"],
                z=ros_data["altitude"],
                covariance=ros_data.get("position_covariance"),
                covariance_type=ros_data.get("position_covariance_type"),
            ),
            status=NavSatStatusAdapter.from_dict(ros_data["status"]),
        )

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


@register_adapter
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
    def _is_valid_covariance(covariance_list: Optional[List[float]]) -> bool:
        """Checks if a 9-element ROS covariance list is not the 'all zeros' sentinel."""
        # ROS often uses an all-zero matrix (or a matrix with a special marker)
        # to indicate 'no covariance provided'.
        # Assuming all zeros means invalid/unprovided data.
        if not covariance_list:
            return False
        return any(c != 0.0 for c in covariance_list)

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
        if cls._is_data_available(ros_data.get("orientation_covariance")):
            ori_dict = ros_data.get("orientation")
            orientation = QuaternionAdapter.from_dict(ori_dict) if ori_dict else None
        if orientation and cls._is_valid_covariance(
            ros_data.get("orientation_covariance")
        ):
            orientation.covariance = ros_data.get("orientation_covariance")

        # Optional Field Conversions (Covariance)
        if cls._is_valid_covariance(ros_data.get("linear_acceleration_covariance")):
            # ROS covariance is a 9-element array (row-major 3x3).
            # Vector9d is assumed to take these 9 elements directly.
            accel.covariance = ros_data.get("linear_acceleration_covariance")

        if cls._is_valid_covariance(ros_data.get("angular_velocity_covariance")):
            angular_vel.covariance = ros_data.get("angular_velocity_covariance")

        return IMU(
            header=_make_header(ros_data.get("header")),
            acceleration=accel,
            angular_velocity=angular_vel,
            orientation=orientation,
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_adapter
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
        return NMEASentence(
            header=_make_header(ros_data.get("header")),
            sentence=ros_data["sentence"],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_adapter
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
            header=_make_header(ros_data.get("header")),
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
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_adapter
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

        return CompressedImage(
            header=_make_header(ros_data.get("header")),
            data=bytes(ros_data["data"]),
            format=ros_data["format"],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_adapter
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
            msg_type="sensor_msgs/RegionOfInterest",
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

    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/RegionOfInterest"

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
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


@register_adapter
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

        return BatteryState(
            header=_make_header(ros_data.get("header")),
            voltage=ros_data["voltage"],
            temperature=ros_data["temperature"],
            current=ros_data["current"],
            charge=ros_data["charge"],
            capacity=ros_data["capacity"],
            design_capacity=ros_data["design_capacity"],
            percentage=ros_data["percentage"],
            power_supply_status=ros_data["power_supply_status"],
            power_supply_health=ros_data["power_supply_health"],
            power_supply_technology=ros_data["power_supply_technology"],
            present=ros_data["present"],
            cell_voltage=ros_data["cell_voltage"],
            cell_temperature=ros_data["cell_temperature"],
            location=ros_data["location"],
            serial_number=ros_data["serial_number"],
        )

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


@register_adapter
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
            header=_make_header(ros_data.get("header")),
            names=ros_data["name"],
            positions=ros_data["position"],
            velocities=ros_data["velocity"],
            efforts=ros_data["effort"],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
