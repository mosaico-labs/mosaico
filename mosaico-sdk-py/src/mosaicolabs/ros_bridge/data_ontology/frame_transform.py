import pyarrow as pa
from typing import List
from mosaicolabs.models.data import Transform
from mosaicolabs.models import Serializable, HeaderMixin


class FrameTransform(Serializable, HeaderMixin):
    """
    Represents a list of transformations between two coordinates.

    modeled after: [tf2_msgs/msg/TFMessage](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html)

    Note:
        This model is not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "transforms",
                pa.list_(value_type=Transform.__msco_pyarrow_struct__),
                nullable=False,
                metadata={"description": "List of coordinate frames transformations."},
            ),
        ]
    )

    transforms: List[Transform]
    """List of coordinate frames transformations."""
