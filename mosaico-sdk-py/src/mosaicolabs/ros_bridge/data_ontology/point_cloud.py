from enum import IntEnum
from typing import List

from mosaicolabs.models import MosaicoType
from mosaicolabs.models.serializable import Serializable
from mosaicolabs.models.types import MosaicoField


class PointFieldDataType(IntEnum):
    """
    The data type of a point field.
    """

    INT8 = 1
    UINT8 = 2
    INT16 = 3
    UINT16 = 4
    INT32 = 5
    UINT32 = 6
    FLOAT32 = 7
    FLOAT64 = 8


class PointField(Serializable):
    """
    Represents a point field in a point cloud in ROS2.

    modeled after: [sensor_msgs/msg/PointField](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointField.html)
    """

    name: MosaicoType.string = MosaicoField(description="Name of the field.")
    """The name of the point field."""
    offset: MosaicoType.uint32 = MosaicoField(
        description="Staring position of the field."
    )
    """The Offset from start of point struct."""
    datatype: PointFieldDataType = MosaicoField(description="Datatype of the field.")
    """The data type of the point field, see `PointFieldDatatype`."""
    count: MosaicoType.uint32 = MosaicoField(
        description="Number of elements in the point field."
    )
    """The number of elements in the point field."""


class PointCloud2(Serializable):
    """
     Represents a point cloud in ROS2.

     modeled after: [sensor_msgs/msg/PointCloud2](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    Note:
        This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    height: MosaicoType.uint32 = MosaicoField(
        description="The height of the point cloud."
    )
    """The height of the point cloud."""

    width: MosaicoType.uint32 = MosaicoField(
        description="The width of the point cloud."
    )
    """The width of the point cloud."""

    fields: List[PointField] = MosaicoField(
        description="The fields of the point cloud."
    )
    """The fields of the point cloud."""

    is_bigendian: MosaicoType.bool = MosaicoField(
        description="Whether the data is big-endian."
    )
    """Whether the data is big-endian."""

    point_step: MosaicoType.uint32 = MosaicoField(
        description="Length of a point in bytes."
    )
    """Length of a point in bytes."""

    row_step: MosaicoType.uint32 = MosaicoField(description="Length of a row in bytes.")
    """Length of a row in bytes."""

    data: MosaicoType.binary = MosaicoField(
        description="The point cloud data. Expected size: row_step * height bytes."
    )
    """The point cloud data. Expected size: row_step * height bytes."""

    is_dense: MosaicoType.bool = MosaicoField(
        description="True if there are no invalid points."
    )
    """True if there are no invalid points."""
