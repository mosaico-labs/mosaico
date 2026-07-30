from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

import numpy as np

from mosaicolabs.models.core import Message
from mosaicolabs.models.data import MotionState, Point3d, RobotPath
from mosaicolabs.models.futures import (
    GridCells,
    MapMetadata,
    OccupancyGrid,
)

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .builtin_interfaces import TimeAdapter
from .geometry_msgs import PointAdapter, PoseAdapter, TwistAdapter
from .helpers import _is_valid_header, _validate_msgdata
from .std_msgs import HeaderAdapter


@register_default_adapter(is_default=True)
class OdometryAdapter(ROSAdapterBase[MotionState]):
    """
    Adapter for translating ROS Odometry messages to Mosaico `MotionState`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/Odometry`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Odometry.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/odometry",
            msg_type="nav_msgs/msg/Odometry",
            data = {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
                "twist": {
                    "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.0}
                },
                "child_frame_id": "base_link"
            }
        )
        # Automatically resolves to a flat Mosaico MotionState with attached metadata
        mosaico_odometry = OdometryAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "nav_msgs/msg/Odometry"

    __mosaico_ontology_type__: Type[MotionState] = MotionState
    _REQUIRED_KEYS = ("pose", "twist", "child_frame_id")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `MotionState` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> MotionState:
        """
        Parses a dictionary to extract a `MotionState` object.

        Example:
            ```python
            ros_data = {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
                "twist": {
                    "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.0}
                },
                "child_frame_id": "base_link"
            }
            # Automatically resolves to a flat Mosaico MotionState with attached metadata
            mosaico_odometry = OdometryAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            MotionState: The constructed Mosaico MotionState object.

        Raises:
            ValueError: If a required key is missing from `ros_data`.
        """
        _validate_msgdata(cls, ros_data)

        return MotionState(
            target_frame_id=ros_data["child_frame_id"],
            pose=PoseAdapter.from_dict(ros_data["pose"]),
            velocity=TwistAdapter.from_dict(ros_data["twist"]),
            header=HeaderAdapter.from_dict(ros_data["header"])
            if _is_valid_header(ros_data.get("header"))
            else None,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, MotionState],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``MotionState`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/Odometry`` message.

        Args:
            mosaico_data (Union[Message, MotionState]): A ``Message`` wrapping a ``MotionState`` instance, or a raw ``MotionState``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``nav_msgs/msg/Odometry`` is supported.

        Returns:
            MsgType: A ``nav_msgs/msg/Odometry`` instance, or raises an error if:

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
        motion_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosOdometry = typestore.types["nav_msgs/msg/Odometry"]

        if resolved_rosmsg_type == "nav_msgs/msg/Odometry":
            return RosOdometry(
                header=HeaderAdapter.to_ros(ms_header, typestore),
                child_frame_id=motion_data.target_frame_id,
                pose=PoseAdapter.to_ros(
                    motion_data.pose, typestore, "geometry_msgs/msg/PoseWithCovariance"
                ),
                twist=TwistAdapter.to_ros(
                    motion_data.velocity,
                    typestore,
                    "geometry_msgs/msg/TwistWithCovariance",
                ),
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)


@register_default_adapter(is_default=True)
class RobotPathAdapter(ROSAdapterBase[RobotPath]):
    """
    Adapter for translating ROS Path messages to Mosaico `RobotPath`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/Path`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Path.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/path",
            msg_type="nav_msgs/msg/Path",
            data = {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "poses":[
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 17000, "nanosec": 0}},
                        "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    },
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 18000, "nanosec": 0}},
                        "position": {"x": 2.0, "y": 3.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    },
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 19000, "nanosec": 0}},
                        "position": {"x": 3.0, "y": 4.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    }
                ]
            }
        )
        # Automatically resolves to a Mosaico RobotPath with attached metadata
        mosaico_path = RobotPathAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = "nav_msgs/msg/Path"

    __mosaico_ontology_type__: Type[RobotPath] = RobotPath
    _REQUIRED_KEYS = ("poses",)

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `RobotPath` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> RobotPath:
        """
        Parses a dictionary to extract a `RobotPath` object.

        Example:
            ```python
            ros_data = {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "poses":[
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 17000, "nanosec": 0}},
                        "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    },
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 18000, "nanosec": 0}},
                        "position": {"x": 2.0, "y": 3.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    },
                    {
                        "header": {"frame_id": "base_link", "stamp": {"sec": 19000, "nanosec": 0}},
                        "position": {"x": 3.0, "y": 4.0, "z": 0.0},
                        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                    }
                ]
            }
            # Automatically resolves to a Mosaico RobotPath with attached metadata
            mosaico_robot_path = RobotPathAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            RobotPath: The constructed Mosaico RobotPath object.

        Raises:
            ValueError: If the 'poses' key exists but is not a list, or if required keys are missing.
        """
        _validate_msgdata(cls, ros_data)

        poses = ros_data["poses"]
        if not isinstance(poses, list):
            raise ValueError(
                f"Invalid type for 'poses' value in ros message: expected 'list' found '{type(poses).__name__}'"
            )

        return RobotPath(
            poses=[PoseAdapter.from_dict(ros_pose) for ros_pose in poses],
            header=HeaderAdapter.from_dict(ros_data["header"])
            if _is_valid_header(ros_data.get("header"))
            else None,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, RobotPath],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``RobotPath`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/Path`` message.

        Args:
            mosaico_data (Union[Message, RobotPath]): A ``Message`` wrapping a ``RobotPath`` instance, or a raw ``RobotPath``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``nav_msgs/msg/Path`` is supported.

        Returns:
            MsgType: A ``nav_msgs/msg/Path`` instance, or raises an error if:

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
        path_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosPath = typestore.types["nav_msgs/msg/Path"]

        poses: Any = [
            PoseAdapter.to_ros(pose, typestore, "geometry_msgs/msg/PoseStamped")
            for pose in path_data.poses
        ]

        if resolved_rosmsg_type == "nav_msgs/msg/Path":
            return RosPath(
                header=HeaderAdapter.to_ros(ms_header, typestore),
                poses=poses,
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)


@register_default_adapter(is_default=True)
class GridCellsAdapter(ROSAdapterBase[GridCells]):
    """
    Adapter for translating ROS GridCells messages to Mosaico `GridCells`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/GridCells`](https://docs.ros2.org/foxy/api/nav_msgs/msg/GridCells.html)

    Example:
    ```python
    ros_msg = ROSMessage(
        topic="/gridcells",
        timestamp=17000,
        msg_type="nav_msgs/msg/GridCells",
        data={
            "cell_width": 10,
            "cell_height": 10,
            "cells": [
                {
                    "x": 1,
                    "y": 2,
                    "z": 4,
                },
                {
                    "x": 40,
                    "y": 39,
                    "z": 10,
                },
            ]
        }
    )

    mosaico_grid_cells = GridCellsAdapter.translate(ros_msg)
    ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("nav_msgs/msg/GridCells",)

    __mosaico_ontology_type__: Type[GridCells] = GridCells
    _REQUIRED_KEYS = (
        "cell_width",
        "cell_height",
        "cells",
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
            Message: The translated message containing a `GridCells` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> GridCells:
        """
        Parses ROS GridCells data.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            GridCells: The constructed Mosaico GridCells object.
        """
        _validate_msgdata(cls, ros_data)

        return GridCells(
            cell_width=ros_data["cell_width"],
            cell_height=ros_data["cell_height"],
            cells=[
                Point3d(
                    x=point["x"],
                    y=point["y"],
                    z=point["z"],
                )
                for point in ros_data["cells"]
            ],
            header=HeaderAdapter.from_dict(ros_data["header"])
            if _is_valid_header(ros_data.get("header"))
            else None,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, GridCells],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``GridCells`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/GridCells`` message.

        Args:
            mosaico_data (Union[Message, GridCells]): A ``Message`` wrapping a ``GridCells`` instance, or a raw ``GridCells``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``nav_msgs/msg/GridCells`` is supported.

        Returns:
            MsgType: A ``nav_msgs/msg/GridCells`` instance, or raises an error if:

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
        gridcell_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosGridCell = typestore.types["nav_msgs/msg/GridCells"]

        cells: Any = [
            PointAdapter.to_ros(point, typestore) for point in gridcell_data.cells
        ]

        if resolved_rosmsg_type == "nav_msgs/msg/GridCells":
            return RosGridCell(
                header=HeaderAdapter.to_ros(ms_header, typestore),
                cell_width=gridcell_data.cell_width,
                cell_height=gridcell_data.cell_height,
                cells=cells,
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)


@register_default_adapter(is_default=True)
class MapMetadataAdapter(ROSAdapterBase[MapMetadata]):
    """
    Adapter for translating ROS MapMetadata messages to Mosaico `MapMetadata`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/MapMetaData`](https://docs.ros2.org/foxy/api/nav_msgs/msg/MapMetaData.html)

    Example:
    ```python
    ros_msg = ROSMessage(
        topic="/mapmetadata",
        timestamp=17000,
        msg_type="nav_msgs/msg/MapMetaData",
        data={
            "map_load_time": {
                "sec": 100000,
                "nanosec": 1000
            },
            "resolution": 10000,
            "width": 100,
            "height": 100,
            "origin": {
                "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
            }
        }
    )

    mosaico_map_metadata = MapMetadataAdapter.translate(ros_msg)
    ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("nav_msgs/msg/MapMetaData",)

    __mosaico_ontology_type__: Type[MapMetadata] = MapMetadata
    _REQUIRED_KEYS = (
        "map_load_time",
        "resolution",
        "width",
        "height",
        "origin",
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
            Message: The translated message containing a `MapMetadata` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> MapMetadata:
        """
        Parses ROS MapMetadata data.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            MapMetadata: The constructed Mosaico MapMetadata object.
        """
        _validate_msgdata(cls, ros_data)
        return MapMetadata(
            map_load_time=TimeAdapter.from_dict(ros_data["map_load_time"]),
            resolution=ros_data["resolution"],
            width=ros_data["width"],
            height=ros_data["height"],
            origin=PoseAdapter.from_dict(ros_data["origin"]),
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, MapMetadata],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``MapMetadata`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/MapMetaData`` message.

        Args:
            mosaico_data (Union[Message, MapMetadata]): A ``Message`` wrapping a ``MapMetadata`` instance, or a raw ``MapMetadata``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``nav_msgs/msg/MapMetaData`` is supported.

        Returns:
            MsgType: A ``nav_msgs/msg/MapMetaData`` instance, or raises an error if:

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
        map_metadata_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosMapMetadata = typestore.types["nav_msgs/msg/MapMetaData"]

        if resolved_rosmsg_type == "nav_msgs/msg/MapMetaData":
            return RosMapMetadata(
                map_load_time=TimeAdapter.to_ros(
                    map_metadata_data.map_load_time, typestore
                ),
                resolution=map_metadata_data.resolution,
                width=map_metadata_data.width,
                height=map_metadata_data.height,
                origin=PoseAdapter.to_ros(map_metadata_data.origin, typestore),
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)


@register_default_adapter(is_default=True)
class OccupancyGridAdapter(ROSAdapterBase[OccupancyGrid]):
    """
    Adapter for translating ROS OccupancyGrid messages to Mosaico `OccupancyGrid`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/OccupancyGrid`](https://docs.ros2.org/foxy/api/nav_msgs/msg/OccupancyGrid.html)

    Example:
    ```python
    ros_msg = ROSMessage(
        topic="/occupancygrid",
        timestamp=17000,
        msg_type="nav_msgs/msg/OccupancyGrid",
        data={
            "info": {
                "map_load_time": {
                    "sec": 100000,
                    "nanosec": 1000
                },
                "resolution": 4,
                "width": 2,
                "height": 2,
                "origin": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                }
            },
            data: [1, -1, 0.5, 0]
        }
    )

    mosaico_occupancy_grid = OccupancyGridAdapter.translate(ros_msg)
    ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("nav_msgs/msg/OccupancyGrid",)

    __mosaico_ontology_type__: Type[OccupancyGrid] = OccupancyGrid
    _REQUIRED_KEYS = (
        "info",
        "data",
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
            Message: The translated message containing a `OccupancyGrid` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> OccupancyGrid:
        """
        Parses ROS OccupancyGrid data.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            OccupancyGrid: The constructed Mosaico OccupancyGrid object.
        """
        _validate_msgdata(cls, ros_data)
        return OccupancyGrid(
            info=MapMetadataAdapter.from_dict(ros_data["info"]),
            data=ros_data["data"],
            header=HeaderAdapter.from_dict(ros_data["header"])
            if _is_valid_header(ros_data.get("header"))
            else None,
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, OccupancyGrid],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``OccupancyGrid`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/OccupancyGrid`` message.

        Args:
            mosaico_data (Union[Message, OccupancyGrid]): A ``Message`` wrapping a ``OccupancyGrid`` instance, or a raw ``OccupancyGrid``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``nav_msgs/msg/OccupancyGrid`` is supported.

        Returns:
            MsgType: A ``nav_msgs/msg/OccupancyGrid`` instance, or raises an error if:

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
        occupancy_grid_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosOccupancyGrid = typestore.types["nav_msgs/msg/OccupancyGrid"]

        if resolved_rosmsg_type == "nav_msgs/msg/OccupancyGrid":
            return RosOccupancyGrid(
                header=HeaderAdapter.to_ros(ms_header, typestore),
                info=MapMetadataAdapter.to_ros(occupancy_grid_data.info, typestore),
                data=np.asarray(occupancy_grid_data.data, dtype=np.int8),
            )

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)
