from typing import List

from mosaicolabs.models import Serializable
from mosaicolabs.models.data import Transform
from mosaicolabs.models.types import MosaicoField


class FrameTransform(Serializable):
    """
    Represents a list of transformations between two coordinates.

    modeled after: [tf2_msgs/msg/TFMessage](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html)

    Note:
        This model is not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    transforms: List[Transform] = MosaicoField(
        description="List of coordinate frames transformations."
    )
    """List of coordinate frames transformations."""
