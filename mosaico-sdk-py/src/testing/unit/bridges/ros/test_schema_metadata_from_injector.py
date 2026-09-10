from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from mosaicolabs.bridges.ros.injector import RosbagInjector, ROSInjectionConfig
from mosaicolabs.bridges.ros.ros_message import ROSMessage


class _FakeSessionWriter:
    """These tests only exercise metadata construction, not translation/serialization."""

    def __init__(self):
        self.created_metadata: Optional[dict] = None

    def get_topic_writer(self, topic: str):
        return None

    def topic_create(self, topic_name, metadata, ontology_type, on_error):
        self.created_metadata = metadata


class _FakeUI:
    def update_status(self, topic, status, style):
        pass

    def advance_all(self, topic):
        pass


class _FakeAdapter:
    @staticmethod
    def schema_metadata(typestore, msg_type, ros_version) -> Optional[dict]:
        return {"_ros_": {"msgtype": msg_type}}

    @staticmethod
    def ontology_data_type():
        return "Null"


class _FakeAdapterNoSchema(_FakeAdapter):
    @staticmethod
    def schema_metadata(typestore, msg_type, ros_version) -> Optional[dict]:
        return None


def _make_ros_msg(topic="/imu", msg_type="sensor_msgs/msg/Imu"):
    return ROSMessage(
        bag_timestamp_ns=0,
        topic=topic,
        msg_type=msg_type,
        data={"x": 1},
    )


def _make_injector(file_path=Path("data.mcap"), **cfg_overrides):
    cfg = ROSInjectionConfig(
        file_path=file_path,
        sequence_name="seq",
        **cfg_overrides,
    )
    injector = RosbagInjector(cfg)
    # A real ROSLoader isn't needed: adapter resolution goes through
    # `adapter_overrides`, which is checked before `self._loader.resolve_adapter()`.
    injector._loader = SimpleNamespace(_typestore=None)
    return injector


def _process(injector: RosbagInjector, ros_msg: ROSMessage):
    session_writer = _FakeSessionWriter()
    injector._process_message(ros_msg, None, session_writer, _FakeUI())
    return session_writer.created_metadata


def test_ros_namespace_always_wins_over_conflicting_topic_metadata():
    injector = _make_injector(
        adapter_overrides={"/imu": _FakeAdapter},
        topic_metadata={
            "/imu": {"_ros_": {"msgtype": "hacked"}, "extra": "kept"},
        },
    )

    metadata = _process(injector, _make_ros_msg())
    assert metadata is not None

    assert metadata["_ros_"]["msgtype"] == "sensor_msgs/msg/Imu"
    assert metadata["extra"] == "kept"


def test_source_file_is_nested_under_ros_namespace():
    injector = _make_injector(
        file_path=Path("recording_01.mcap"),
        adapter_overrides={"/imu": _FakeAdapter},
    )

    metadata = _process(injector, _make_ros_msg())
    assert metadata is not None
    assert metadata["_ros_"]["source_file"] == "recording_01.mcap"
    assert "source_file" not in metadata


def test_non_conflicting_topic_metadata_keys_are_preserved():
    injector = _make_injector(
        adapter_overrides={"/imu": _FakeAdapter},
        topic_metadata={"/imu": {"vendor": "acme"}},
    )

    metadata = _process(injector, _make_ros_msg())
    assert metadata is not None
    assert metadata["vendor"] == "acme"
    assert metadata["_ros_"]["msgtype"] == "sensor_msgs/msg/Imu"


def test_topic_metadata_for_other_topics_is_not_applied():
    injector = _make_injector(
        adapter_overrides={"/imu": _FakeAdapter},
        topic_metadata={"/gps": {"vendor": "acme"}},
    )

    metadata = _process(injector, _make_ros_msg(topic="/imu"))
    assert metadata is not None
    assert "vendor" not in metadata


def test_missing_schema_metadata_still_produces_ros_namespace_with_source_file():
    injector = _make_injector(adapter_overrides={"/imu": _FakeAdapterNoSchema})

    metadata = _process(injector, _make_ros_msg())
    assert metadata is not None
    assert metadata["_ros_"] == {"source_file": "data.mcap"}


def test_topic_metadata_dict_is_not_mutated_by_processing():
    """Regression test: `_process_message` must not mutate the caller's `topic_metadata`
    dict in place, since `ROSInjectionConfig` is reused across the whole run."""
    user_topic_metadata = {"/imu": {"vendor": "acme"}}
    injector = _make_injector(
        adapter_overrides={"/imu": _FakeAdapter},
        topic_metadata=user_topic_metadata,
    )

    _process(injector, _make_ros_msg())

    assert user_topic_metadata == {"/imu": {"vendor": "acme"}}
