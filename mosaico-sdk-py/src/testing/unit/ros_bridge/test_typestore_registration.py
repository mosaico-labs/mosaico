import shutil
from pathlib import Path

import pytest
from rosbags.typesys import Stores, get_typestore

from mosaicolabs.ros_bridge.injector import (
    RosbagInjector,
    ROSInjectionConfig,
)
from mosaicolabs.ros_bridge.sequence_extractor import (
    ROSExtractorConfig,
    ROSSequenceExtractor,
)


@pytest.mark.parametrize(
    "ros_distro",
    [distro for distro in Stores],
)
def test_standard_typestore_registration_injector(ros_distro: Stores):
    """
    Test that the standard ROS message types are registered in the typestore.
    """
    inj = RosbagInjector(
        config=ROSInjectionConfig(
            file_path=Path("test.bag"),
            sequence_name="test",
            ros_distro=ros_distro,
        )
    )

    assert inj._typestore is not None
    assert inj._typestore.types.keys() == get_typestore(ros_distro).types.keys()


@pytest.mark.parametrize(
    "ros_distro",
    [distro for distro in Stores],
)
def test_custom_typestore_registration_injector(ros_distro: Stores):
    """
    Test that custom ROS message types are registered in the typestore.
    """
    # Create a temporary directory for custom message definitions
    custom_msgs_dir = Path("/tmp/custom_msgs")

    try:
        custom_msgs_dir.mkdir(parents=True, exist_ok=True)
        # use directory here
        custom_msg_file = custom_msgs_dir / "MyCustomMsg.msg"
        custom_msg_file.write_text("int32 data\n")

        inj = RosbagInjector(
            config=ROSInjectionConfig(
                file_path=Path("test.bag"),
                sequence_name="test",
                ros_distro=ros_distro,
                custom_msgs=[("custom_msgs", Path(custom_msgs_dir), ros_distro)],
            )
        )

        assert inj._typestore is not None
        assert "custom_msgs/msg/MyCustomMsg" in inj._typestore.types.keys()

    finally:
        shutil.rmtree(custom_msgs_dir, ignore_errors=True)


def test_typestore_reaches_ros_loader():
    # Sufficient just one distro to test forwarding
    ros_distro = Stores.LATEST
    inj = RosbagInjector(
        config=ROSInjectionConfig(
            file_path=Path("test.bag"),
            sequence_name="test",
            ros_distro=ros_distro,
        )
    )
    loader = inj._open_or_get_loader()
    assert inj._typestore.types == get_typestore(ros_distro).types
    assert loader._typestore.types == inj._typestore.types


@pytest.mark.parametrize(
    "ros_distro",
    [distro for distro in Stores],
)
def test_standard_typestore_registration_extractor(ros_distro: Stores):
    """
    Test that the standard ROS message types are registered in the typestore.
    """
    inj = ROSSequenceExtractor(
        config=ROSExtractorConfig(
            rosbag_path=Path("no-path"),
            sequence_name="test",
            ros_distro=ros_distro,
        )
    )

    assert inj.typestore is not None
    assert inj.typestore.types.keys() == get_typestore(ros_distro).types.keys()


@pytest.mark.parametrize(
    "ros_distro",
    [distro for distro in Stores],
)
def test_custom_typestore_registration_extractor(ros_distro: Stores):
    """
    Test that custom ROS message types are registered in the typestore.
    """
    # Create a temporary directory for custom message definitions
    custom_msgs_dir = Path("/tmp/custom_msgs")

    try:
        custom_msgs_dir.mkdir(parents=True, exist_ok=True)
        # use directory here
        custom_msg_file = custom_msgs_dir / "MyCustomMsg.msg"
        custom_msg_file.write_text("int32 data\n")

        inj = ROSSequenceExtractor(
            config=ROSExtractorConfig(
                rosbag_path=Path("no-path"),
                sequence_name="test",
                ros_distro=ros_distro,
                custom_msgs=[("custom_msgs", Path(custom_msgs_dir), ros_distro)],
            )
        )

        assert inj.typestore is not None
        assert "custom_msgs/msg/MyCustomMsg" in inj.typestore.types.keys()

    finally:
        shutil.rmtree(custom_msgs_dir, ignore_errors=True)


class _FakeClient:
    pass


def test_typestore_reaches_mosaico_loader():
    # Sufficient just one distro to test forwarding
    ros_distro = Stores.LATEST
    extr = ROSSequenceExtractor(
        config=ROSExtractorConfig(
            rosbag_path=Path("no-path"),
            sequence_name="test",
            ros_distro=ros_distro,
        )
    )
    loader = extr._open_or_get_mosaicoloader(_FakeClient())
    assert extr.typestore.types == get_typestore(ros_distro).types
    assert loader._typestore.types == extr.typestore.types
