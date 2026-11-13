"""On export, the adapter registered for the ROS message type of a topic hands over to the
adapter of the topic's ontology, since a ROS message type can be stored with more than one."""

from types import SimpleNamespace

from mosaicolabs.bridges.ros.adapters import CompressedImageAdapter, ImageAdapter
from mosaicolabs.bridges.ros.loader import MosaicoToROSLoader
from mosaicolabs.models.sensors import (
    CompressedImage,
    Image,
    ImageSequence,
    ImageSequenceRaw,
)


def _strategy_1(ontology, ros_metadata):
    """The adapter found from the `msgtype` of the topic, or `None`."""
    loader = object.__new__(MosaicoToROSLoader)
    t_handler = SimpleNamespace(ontology_tag=ontology.ontology_tag(), name="/topic")
    return loader._adapter_from_metadata_msgtype(t_handler, ros_metadata)


def test_image_sequence_raw_topic_is_exported_by_image_adapter():
    assert _strategy_1(ImageSequenceRaw, {"msgtype": "sensor_msgs/msg/Image"}) == (
        ImageAdapter,
        "sensor_msgs/msg/Image",
    )


def test_image_topic_is_left_to_its_ontology():
    # An `Image` topic, e.g. ROS images with no stamp, is not exported by `ImageAdapter`,
    # which writes `ImageSequenceRaw`, but by the adapter of `Image`.
    assert _strategy_1(Image, {"msgtype": "sensor_msgs/msg/Image"}) is None


def test_compressed_image_topic_is_exported_by_compressed_image_adapter():
    assert _strategy_1(
        CompressedImage, {"msgtype": "sensor_msgs/msg/CompressedImage"}
    ) == (CompressedImageAdapter, "sensor_msgs/msg/CompressedImage")


def test_image_sequence_topic_is_exported_with_its_format():
    adapter, msgtype = _strategy_1(
        ImageSequence,
        {
            "msgtype": "sensor_msgs/msg/CompressedImage",
            "compressed_format": "bgr8; png compressed bgr8",
        },
    )
    assert msgtype == "sensor_msgs/msg/CompressedImage"
    assert adapter.ontology_data_type() is ImageSequence
    assert adapter._ros_format == "bgr8; png compressed bgr8"
