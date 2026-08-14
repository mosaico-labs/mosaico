import pytest

from mosaicolabs.ros_bridge.adapter_base import RosSchemaMetadata


def test_to_dict_wraps_fields_under_reserved_key():
    meta = RosSchemaMetadata(msgtype="sensor_msgs/msg/Imu")

    assert meta.to_dict() == {"_ros_": {"msgtype": "sensor_msgs/msg/Imu"}}


def test_update_merges_and_returns_self_for_chaining():
    meta = RosSchemaMetadata(msgtype="sensor_msgs/msg/Imu")

    result = meta.update(source_file="a.mcap")

    assert result is meta
    assert meta.to_dict() == {
        "_ros_": {"msgtype": "sensor_msgs/msg/Imu", "source_file": "a.mcap"}
    }


def test_update_overwrites_existing_field():
    meta = RosSchemaMetadata(source_file="a.mcap")

    meta.update(source_file="b.mcap")

    assert meta.to_dict() == {"_ros_": {"source_file": "b.mcap"}}


def test_merge_into_creates_namespace_when_absent():
    meta = RosSchemaMetadata(msgtype="sensor_msgs/msg/Imu")
    target = {"lens": "wide-angle"}

    result = meta.merge_into(target)

    assert result is target
    assert target == {
        "lens": "wide-angle",
        "_ros_": {"msgtype": "sensor_msgs/msg/Imu"},
    }


def test_merge_into_updates_existing_namespace_without_clobbering_other_keys():
    meta = RosSchemaMetadata(source_file="a.mcap")
    target = {"_ros_": {"msgtype": "sensor_msgs/msg/Imu"}}

    meta.merge_into(target)

    assert target == {
        "_ros_": {"msgtype": "sensor_msgs/msg/Imu", "source_file": "a.mcap"}
    }


@pytest.mark.parametrize(
    "metadata",
    [None, {}, {"other_key": "value"}],
)
def test_extract_returns_empty_dict_when_no_ros_block(metadata):
    assert RosSchemaMetadata.extract(metadata) == {}


def test_extract_returns_ros_block_contents():
    metadata = {"_ros_": {"msgtype": "sensor_msgs/msg/Imu"}, "other_key": "value"}

    assert RosSchemaMetadata.extract(metadata) == {"msgtype": "sensor_msgs/msg/Imu"}


@pytest.mark.parametrize("metadata", [None, {}, {"_ros_": {}}])
def test_from_dict_handles_missing_or_empty_ros_block(metadata):
    meta = RosSchemaMetadata.from_dict(metadata)

    assert meta.to_dict() == {"_ros_": {}}


def test_from_dict_round_trips_existing_ros_block():
    original = {"_ros_": {"msgtype": "sensor_msgs/msg/Imu", "msgdef": "..."}}

    meta = RosSchemaMetadata.from_dict(original)
    meta.update(source_file="a.mcap")

    assert meta.to_dict() == {
        "_ros_": {
            "msgtype": "sensor_msgs/msg/Imu",
            "msgdef": "...",
            "source_file": "a.mcap",
        }
    }
    # The original dict passed in must not be mutated by from_dict/update.
    assert original == {"_ros_": {"msgtype": "sensor_msgs/msg/Imu", "msgdef": "..."}}


def test_key_constant_is_the_single_source_of_truth():
    assert RosSchemaMetadata.KEY == "_ros_"
    assert list(RosSchemaMetadata(a=1).to_dict().keys()) == [RosSchemaMetadata.KEY]
