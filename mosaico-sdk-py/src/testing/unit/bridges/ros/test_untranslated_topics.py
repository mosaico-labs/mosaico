"""A topic is created on the server only by its first message that translates: a topic whose
messages all fail to translate (e.g. ROS images with no stamp) is never left empty."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from mosaicolabs.bridges.ros.injector import RosbagInjector, ROSInjectionConfig
from mosaicolabs.bridges.ros.ros_message import ROSMessage
from mosaicolabs.enum import TopicLevelErrorPolicy


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
