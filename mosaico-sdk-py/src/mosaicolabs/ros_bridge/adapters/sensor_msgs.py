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
)

from .geometry_msgs import (
    QuaternionAdapter,
    Vector3Adapter,
)
from ..data_ontology.battery_state import BatteryState
from ..ros_message import ROSMessage
from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_adapter

from .helpers import _make_header, _validate_msgdata


@register_adapter
class CameraInfoAdapter(ROSAdapterBase[CameraInfo]):
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
    def from_dict(cls, ros_data: dict) -> CameraInfo:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict) -> GPSStatus:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict) -> GPS:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict) -> IMU:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict) -> NMEASentence:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                message_header=ros_msg.header.translate() if ros_msg.header else None,
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data, **kwargs),
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(
        cls,
        ros_data: dict,
        **kwargs: Any,
    ) -> Image:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(
        cls,
        ros_data: dict,
    ) -> CompressedImage:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict):
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
    def from_dict(cls, ros_data: dict) -> BatteryState:
        """
        Converts the raw dictionary data into the specific Mosaico type.
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
