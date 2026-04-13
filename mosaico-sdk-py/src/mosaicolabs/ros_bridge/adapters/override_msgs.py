from typing import Any, Optional, Type

from mosaicolabs import Message
from mosaicolabs.models.futures import Lidar, Radar, RGBDCamera, StereoCamera, ToFCamera
from mosaicolabs.ros_bridge.adapters.sensor_msgs import PointCloudAdapterBase
from mosaicolabs.ros_bridge.ros_message import ROSMessage


class LidarAdapter(PointCloudAdapterBase[Lidar]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `Lidar`.
    This Adapter needs to be specified in the field `adapters_override` of `RosInjectionConfig`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    """

    __mosaico_ontology_type__: Type[Lidar] = Lidar
    _REQUIRED_FIELDS = [
        name for name, field in Lidar.model_fields.items() if field.is_required()
    ]

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> Lidar:
        return Lidar(
            x=decoded_fields["x"],
            y=decoded_fields["y"],
            z=decoded_fields["z"],
            intensity=decoded_fields.get("intensity"),
            reflectivity=decoded_fields.get("reflectivity"),
            beam_id=decoded_fields.get("beam_id"),
            range=decoded_fields.get("range"),
            near_ir=decoded_fields.get("near_ir"),
            azimuth=decoded_fields.get("azimuth"),
            elevation=decoded_fields.get("elevation"),
            confidence=decoded_fields.get("confidence"),
            return_type=decoded_fields.get("return_type"),
            point_timestamp=decoded_fields.get("point_timestamp"),
        )

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Lidar` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Lidar:
        """
        Create a Lidar instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "height": 1,
            "width": 3,
            "fields": [...],
            "is_bigendian": False,
            "point_step": 16,
            "row_step": 48,
            "data": [...],
            "is_dense": True,
        }
        # Automatically resolves to a flat Mosaico Lidar with attached data
        mosaico_lidar = LidarAdapter.from_dict(ros_data)
        ```
        """
        return super().from_dict(ros_data)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


class RadarAdapter(PointCloudAdapterBase[Radar]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `Radar`.
    This Adapter needs to be specified in the field `adapters_override` of `RosInjectionConfig`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    """

    __mosaico_ontology_type__: Type[Radar] = Radar
    _REQUIRED_FIELDS = [
        name for name, field in Radar.model_fields.items() if field.is_required()
    ]

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `Radar` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> Radar:
        return Radar(
            x=decoded_fields["x"],
            y=decoded_fields["y"],
            z=decoded_fields["z"],
            range=decoded_fields.get("range"),
            azimuth=decoded_fields.get("azimuth"),
            elevation=decoded_fields.get("elevation"),
            rcs=decoded_fields.get("rcs"),
            snr=decoded_fields.get("snr"),
            doppler_velocity=decoded_fields.get("doppler_velocity"),
            vx=decoded_fields.get("vx"),
            vy=decoded_fields.get("vy"),
            vx_comp=decoded_fields.get("vx_comp"),
            vy_comp=decoded_fields.get("vy_comp"),
            ax=decoded_fields.get("ax"),
            ay=decoded_fields.get("ay"),
            radial_speed=decoded_fields.get("radial_speed"),
        )

    @classmethod
    def from_dict(cls, ros_data: dict) -> Radar:
        """
        Create a Radar instance from a ROS message dictionary.
        Example:
        ```python
        ros_data = {
            "height": 1,
            "width": 3,
            "fields": [...],
            "is_bigendian": False,
            "point_step": 16,
            "row_step": 48,
            "data": [...],
            "is_dense": True,
        }
        # Automatically resolves to a flat Mosaico Radar with attached data
        mosaico_radar = RadarAdapter.from_dict(ros_data)
        ```
        """
        return super().from_dict(ros_data)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


class RGBDCameraAdapter(PointCloudAdapterBase[RGBDCamera]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `RGBDCamera`.
    This Adapter needs to be specified in the field `adapters_override` of `RosInjectionConfig`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    """

    __mosaico_ontology_type__: Type[RGBDCamera] = RGBDCamera
    _REQUIRED_FIELDS = [
        name for name, field in RGBDCamera.model_fields.items() if field.is_required()
    ]

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `RGBDCamera` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> RGBDCamera:
        return RGBDCamera(
            x=decoded_fields["x"],
            y=decoded_fields["y"],
            z=decoded_fields["z"],
            rgb=decoded_fields.get("rgb"),
            intensity=decoded_fields.get("intensity"),
        )

    @classmethod
    def from_dict(cls, ros_data: dict) -> RGBDCamera:
        """
        Create a RGBDCamera instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "height": 1,
            "width": 3,
            "fields": [...],
            "is_bigendian": False,
            "point_step": 16,
            "row_step": 48,
            "data": [...],
            "is_dense": True,
        }
        # Automatically resolves to a flat Mosaico RGBDCamera with attached data
        mosaico_rgbd_camera = RGBDCameraAdapter.from_dict(ros_data)
        ```
        """
        return super().from_dict(ros_data)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


class ToFCameraAdapter(PointCloudAdapterBase[ToFCamera]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `ToFCamera`.
    This Adapter needs to be specified in the field `adapters_override` of `RosInjectionConfig`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    """

    __mosaico_ontology_type__: Type[ToFCamera] = ToFCamera
    _REQUIRED_FIELDS = [
        name for name, field in ToFCamera.model_fields.items() if field.is_required()
    ]

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `ToFCamera` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> ToFCamera:
        return ToFCamera(
            x=decoded_fields["x"],
            y=decoded_fields["y"],
            z=decoded_fields["z"],
            rgb=decoded_fields.get("rgb"),
            intensity=decoded_fields.get("intensity"),
            noise=decoded_fields.get("noise"),
            grayscale=decoded_fields.get("grayscale"),
        )

    @classmethod
    def from_dict(cls, ros_data: dict) -> ToFCamera:
        """
        Create a ToFCamera instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "height": 1,
            "width": 3,
            "fields": [...],
            "is_bigendian": False,
            "point_step": 16,
            "row_step": 48,
            "data": [...],
            "is_dense": True,
        }
        # Automatically resolves to a flat Mosaico ToFCamera with attached data
        mosaico_tof_camera = ToFCameraAdapter.from_dict(ros_data)
        ```
        """
        return super().from_dict(ros_data)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None


class StereoCameraAdapter(PointCloudAdapterBase[StereoCamera]):
    """
    Adapter for translating ROS PointCloud2 messages to Mosaico `StereoCamera`.
    This Adapter needs to be specified in the field `adapters_override` of `RosInjectionConfig`.

    **Supported ROS Types:**

    - [`sensor_msgs/msg/PointCloud2`](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    """

    __mosaico_ontology_type__: Type[StereoCamera] = StereoCamera
    _REQUIRED_FIELDS = [
        name for name, field in StereoCamera.model_fields.items() if field.is_required()
    ]

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `StereoCamera` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def _build(cls, decoded_fields: dict[str, list]) -> StereoCamera:
        return StereoCamera(
            x=decoded_fields["x"],
            y=decoded_fields["y"],
            z=decoded_fields["z"],
            rgb=decoded_fields.get("rgb"),
            intensity=decoded_fields.get("intensity"),
            luma=decoded_fields.get("luma"),
            cost=decoded_fields.get("cost"),
        )

    @classmethod
    def from_dict(cls, ros_data: dict) -> StereoCamera:
        """
        Create a StereoCamera instance from a ROS message dictionary.

        Example:
        ```python
        ros_data = {
            "height": 1,
            "width": 3,
            "fields": [...],
            "is_bigendian": False,
            "point_step": 16,
            "row_step": 48,
            "data": [...],
            "is_dense": True,
        }
        # Automatically resolves to a flat Mosaico StereoCamera with attached data
        mosaico_stereo_camera = StereoCameraAdapter.from_dict(ros_data)
        ```
        """
        return super().from_dict(ros_data)

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
