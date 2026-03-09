from enum import IntEnum

import pyarrow as pa

from mosaicolabs.models.serializable import Serializable


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

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("offset", pa.uint32()),
            pa.field("datatype", pa.uint8()),
            pa.field("count", pa.uint32()),
        ]
    )

    name: str
    """The name of the point field."""
    offset: int
    """The Offset from start of point struct."""
    datatype: PointFieldDataType
    """The data type of the point field, see `PointFieldDatatype`."""
    count: int
    """The number of elements in the point field."""


class PointCloud2(Serializable):
    """
     Represents a point cloud in ROS2.

     modeled after: [sensor_msgs/msg/PointCloud2](https://docs.ros2.org/foxy/api/sensor_msgs/msg/PointCloud2.html)

    Note:
        This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field("height", pa.uint32()),
            pa.field("width", pa.uint32()),
            pa.field("fields", pa.list_(PointField.__msco_pyarrow_struct__)),
            pa.field("is_bigendian", pa.bool_()),
            pa.field("point_step", pa.uint32()),
            pa.field("row_step", pa.uint32()),
            pa.field("data", pa.binary()),
            pa.field("is_dense", pa.bool_()),
        ]
    )

    height: int
    """The height of the point cloud."""
    width: int
    """The width of the point cloud."""
    fields: list[PointField]
    """The fields of the point cloud."""
    is_bigendian: bool
    """Whether the data is big-endian."""
    point_step: int
    """Length of a point in bytes."""
    row_step: int
    """Length of a row in bytes."""
    data: bytes
    """The point cloud data. Expected size: row_step * height bytes."""
    is_dense: bool
    """True if there are no invalid points."""
