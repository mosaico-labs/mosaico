"""A topic is created on the server only by its first message that translates: a topic whose
messages all fail to translate is never left empty. The first message of a topic also chooses
its adapter: ROS images with no stamp become `Image`, not `ImageSequenceRaw`."""

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from rosbags.typesys.stores import Stores, get_typestore

from mosaicolabs.bridges.ros.adapters import CompressedImageAdapter, ImageAdapter
from mosaicolabs.bridges.ros.injector import RosbagInjector, ROSInjectionConfig
from mosaicolabs.bridges.ros.ros_message import ROSMessage
from mosaicolabs.enum import TopicLevelErrorPolicy
from mosaicolabs.models.sensors import (
    CompressedImage,
    Image,
    ImageSequence,
    ImageSequenceRaw,
)


class _FakeTopicWriter:
    def __init__(self, on_error):
        self.pushed = []
        self.is_active = True
        self.status = None
        self.on_error = on_error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # As `TopicWriter`: an error is suppressed with `Ignore`.
        return exc_type is not None and self.on_error == TopicLevelErrorPolicy.Ignore

    def push(self, message):
        self.pushed.append(message)


class _FakeSessionWriter:
    def __init__(self):
        self.writers = {}

    def get_topic_writer(self, topic: str):
        return self.writers.get(topic)

    def topic_create(self, topic_name, metadata, ontology_type, on_error):
        writer = _FakeTopicWriter(on_error)
        writer.ontology_type = ontology_type
        writer.metadata = metadata
        self.writers[topic_name] = writer
        return writer


class _FakeUI:
    def update_status(self, topic, status, style):
        pass

    def advance_all(self, topic):
        pass

    def advance_global(self):
        pass


class _FakeAdapter:
    """Translates a message into its `value`, and fails on a message with no `value`."""

    @classmethod
    def adapter_for_topic(cls, first_msg):
        return cls

    @staticmethod
    def schema_metadata(typestore, msg_type, ros_version):
        return None

    @staticmethod
    def ontology_data_type():
        return "Null"

    @staticmethod
    def translate(ros_msg):
        if "value" not in ros_msg.data_field:
            raise ValueError("no capture time: the header of the image has no stamp")
        return ros_msg.data_field["value"]


def _message(value=None):
    data = {"value": value} if value is not None else {"stamp": 0}
    return ROSMessage(
        bag_timestamp_ns=0,
        topic="/camera",
        msg_type="sensor_msgs/msg/Image",
        data=data,
    )


def _ingest(policy, messages):
    """Processes `messages` with the error policy `policy`: returns the session writer."""
    cfg = ROSInjectionConfig(
        file_path=Path("data.mcap"),
        sequence_name="seq",
        adapter_overrides={"/camera": _FakeAdapter},
        topics_on_error=policy,
    )
    injector = RosbagInjector(cfg)
    injector._loader = SimpleNamespace(_typestore=None)
    session_writer = _FakeSessionWriter()
    for message in messages:
        injector._process_message(message, None, session_writer, _FakeUI())
    return session_writer


def test_a_topic_whose_messages_all_fail_is_not_created():
    session_writer = _ingest(TopicLevelErrorPolicy.Ignore, [_message(), _message()])
    assert session_writer.writers == {}


def test_a_topic_is_created_by_its_first_message_that_translates():
    session_writer = _ingest(
        TopicLevelErrorPolicy.Ignore, [_message(), _message(1), _message(), _message(2)]
    )
    assert session_writer.writers["/camera"].pushed == [1, 2]


def test_finalize_ignores_the_topic_after_its_first_failure():
    session_writer = _ingest(TopicLevelErrorPolicy.Finalize, [_message(), _message(1)])
    assert session_writer.writers == {}


def test_raise_raises_without_creating_the_topic():
    with pytest.raises(ValueError, match="no stamp"):
        _ingest(TopicLevelErrorPolicy.Raise, [_message()])


########################## ROS images with no stamp ##########################


def _image(sec: int, topic: str = "/camera/image_raw"):
    """A 1x1 `mono8` ROS image, stamped at `sec` seconds (0: no stamp)."""
    return ROSMessage(
        bag_timestamp_ns=1,
        topic=topic,
        msg_type="sensor_msgs/msg/Image",
        data={
            "header": {"frame_id": "camera", "stamp": {"sec": sec, "nanosec": 0}},
            "height": 1,
            "width": 1,
            "encoding": "mono8",
            "is_bigendian": 0,
            "step": 1,
            "data": np.array([7], dtype=np.uint8),
        },
    )


def _ingest_images(policy, messages):
    """Processes ROS `messages` with `ImageAdapter`: returns the session writer."""
    cfg = ROSInjectionConfig(
        file_path=Path("data.mcap"),
        sequence_name="seq",
        topics_on_error=policy,
    )
    injector = RosbagInjector(cfg)
    injector._loader = SimpleNamespace(
        _typestore=get_typestore(Stores.LATEST),
        resolve_adapter=lambda topic: ImageAdapter,
    )
    session_writer = _FakeSessionWriter()
    for message in messages:
        injector._process_message(message, None, session_writer, _FakeUI())
    return session_writer


def test_images_with_a_stamp_are_image_sequence_raw():
    session_writer = _ingest_images(TopicLevelErrorPolicy.Raise, [_image(1), _image(2)])
    writer = session_writer.writers["/camera/image_raw"]
    assert writer.ontology_type is ImageSequenceRaw
    assert [
        m.get_data(ImageSequenceRaw).capture_timestamp_ns for m in writer.pushed
    ] == [
        1_000_000_000,
        2_000_000_000,
    ]


def test_images_with_no_stamp_are_image_with_the_default_policy():
    # With `Raise`, the default, the bag is ingested as before `ImageSequenceRaw`.
    session_writer = _ingest_images(TopicLevelErrorPolicy.Raise, [_image(0), _image(0)])
    writer = session_writer.writers["/camera/image_raw"]
    assert writer.ontology_type is Image
    assert len(writer.pushed) == 2
    assert all(m.get_data(Image) is not None for m in writer.pushed)


def test_the_first_image_chooses_the_ontology_of_the_whole_topic():
    # A stamped image after an unstamped first one is still an `Image`.
    session_writer = _ingest_images(TopicLevelErrorPolicy.Raise, [_image(0), _image(5)])
    writer = session_writer.writers["/camera/image_raw"]
    assert writer.ontology_type is Image
    assert all(m.get_data(Image) is not None for m in writer.pushed)


def test_each_topic_chooses_its_own_ontology():
    session_writer = _ingest_images(
        TopicLevelErrorPolicy.Raise,
        [
            _image(0, "/a/image_raw"),
            _image(1, "/b/image_raw"),
            _image(2, "/a/image_raw"),
        ],
    )
    assert session_writer.writers["/a/image_raw"].ontology_type is Image
    assert session_writer.writers["/b/image_raw"].ontology_type is ImageSequenceRaw


def test_an_unstamped_image_after_a_stamped_first_one_still_raises():
    with pytest.raises(Exception, match="no capture time"):
        _ingest_images(TopicLevelErrorPolicy.Raise, [_image(1), _image(0)])


######################## ROS CompressedImage PNGs ############################

_PNG = b"\x89PNG\r\n\x1a\n" + bytes(8)


def _compressed(sec: int, ros_format: str, data: bytes = _PNG):
    return ROSMessage(
        bag_timestamp_ns=1,
        topic="/camera/image/compressed",
        msg_type="sensor_msgs/msg/CompressedImage",
        data={
            "header": {"frame_id": "camera", "stamp": {"sec": sec, "nanosec": 0}},
            "format": ros_format,
            "data": np.frombuffer(data, dtype=np.uint8),
        },
    )


def _ingest_compressed(policy, messages):
    cfg = ROSInjectionConfig(
        file_path=Path("data.mcap"),
        sequence_name="seq",
        topics_on_error=policy,
    )
    injector = RosbagInjector(cfg)
    injector._loader = SimpleNamespace(
        _typestore=get_typestore(Stores.LATEST),
        resolve_adapter=lambda topic: CompressedImageAdapter,
    )
    session_writer = _FakeSessionWriter()
    for message in messages:
        injector._process_message(message, None, session_writer, _FakeUI())
    return session_writer.writers["/camera/image/compressed"]


def test_png_topic_is_image_sequence_with_its_format_in_metadata():
    fmt = "bgr8; png compressed bgr8"
    writer = _ingest_compressed(
        TopicLevelErrorPolicy.Raise, [_compressed(1, fmt), _compressed(2, fmt)]
    )
    assert writer.ontology_type is ImageSequence
    assert writer.metadata["_ros_"]["compressed_format"] == fmt
    assert [m.get_data(ImageSequence).capture_timestamp_ns for m in writer.pushed] == [
        1_000_000_000,
        2_000_000_000,
    ]


def test_png_topic_without_stamp_is_compressed_image():
    writer = _ingest_compressed(
        TopicLevelErrorPolicy.Raise, [_compressed(0, "png"), _compressed(3, "png")]
    )
    assert writer.ontology_type is CompressedImage
    assert "compressed_format" not in writer.metadata["_ros_"]


def test_jpeg_topic_is_compressed_image():
    writer = _ingest_compressed(
        TopicLevelErrorPolicy.Raise, [_compressed(1, "jpeg", bytes(16))]
    )
    assert writer.ontology_type is CompressedImage


def test_a_later_image_with_another_format_raises():
    with pytest.raises(Exception, match="differs from the format"):
        _ingest_compressed(
            TopicLevelErrorPolicy.Raise,
            [_compressed(1, "bgr8; png compressed bgr8"), _compressed(2, "png")],
        )
