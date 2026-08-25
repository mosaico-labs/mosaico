from pathlib import Path

import pytest
from rosbags.typesys import Stores

from mosaicolabs.bridges.ros.injector import RosbagInjector, ROSInjectionConfig
from mosaicolabs.bridges.ros.registry import ROSTypeRegistry
from mosaicolabs.bridges.ros.sequence_extractor import (
    ROSExtractorConfig,
    ROSSequenceExtractor,
)

_CUSTOM_MSG_TYPE = "custom_msgs/msg/MyCustomMsg"


@pytest.fixture
def custom_msgs_dir(tmp_path):
    d = tmp_path / "custom_msgs"
    d.mkdir()
    (d / "MyCustomMsg.msg").write_text("int32 data\n")
    return d


def test_registry_defaults_to_a_fresh_private_instance(custom_msgs_dir):
    """Two injectors constructed without `registry=` must not share state."""
    inj_a = RosbagInjector(
        ROSInjectionConfig(
            file_path=Path("a.mcap"),
            sequence_name="a",
            ros_distro=Stores.LATEST,
            custom_msgs=[("custom_msgs", custom_msgs_dir, Stores.LATEST)],
        )
    )
    inj_b = RosbagInjector(
        ROSInjectionConfig(
            file_path=Path("b.mcap"), sequence_name="b", ros_distro=Stores.LATEST
        )
    )

    assert inj_a._registry is not inj_b._registry
    assert _CUSTOM_MSG_TYPE in inj_a._typestore.types
    assert _CUSTOM_MSG_TYPE not in inj_b._typestore.types


def test_registry_shared_across_injector_instances(custom_msgs_dir):
    """Passing the same `registry=` instance to two injectors shares definitions."""
    shared = ROSTypeRegistry()
    shared.register_directory(
        package_name="custom_msgs", dir_path=custom_msgs_dir, store=Stores.LATEST
    )

    inj_a = RosbagInjector(
        ROSInjectionConfig(
            file_path=Path("a.mcap"),
            sequence_name="a",
            ros_distro=Stores.LATEST,
            registry=shared,
        )
    )
    inj_b = RosbagInjector(
        ROSInjectionConfig(
            file_path=Path("b.mcap"),
            sequence_name="b",
            ros_distro=Stores.LATEST,
            registry=shared,
        )
    )

    assert inj_a._registry is shared
    assert inj_a._registry is inj_b._registry
    assert _CUSTOM_MSG_TYPE in inj_a._typestore.types
    assert _CUSTOM_MSG_TYPE in inj_b._typestore.types


def test_registry_shared_between_injector_and_extractor(custom_msgs_dir):
    """The same `registry=` instance can be shared across an injector and an extractor."""
    shared = ROSTypeRegistry()
    shared.register_directory(
        package_name="custom_msgs", dir_path=custom_msgs_dir, store=Stores.LATEST
    )

    injector = RosbagInjector(
        ROSInjectionConfig(
            file_path=Path("a.mcap"),
            sequence_name="a",
            ros_distro=Stores.LATEST,
            registry=shared,
        )
    )
    extractor = ROSSequenceExtractor(
        ROSExtractorConfig(
            rosbag_path=Path("out"),
            sequence_name="a",
            ros_distro=Stores.LATEST,
            registry=shared,
        )
    )

    assert _CUSTOM_MSG_TYPE in injector._typestore.types
    assert _CUSTOM_MSG_TYPE in extractor.typestore.types


def test_registry_instances_are_fully_independent():
    """Registering into one ROSTypeRegistry instance must not affect another."""
    a = ROSTypeRegistry()
    b = ROSTypeRegistry()

    a.register(msg_type="pkg/msg/OnlyOnA", source="int32 data\n")

    assert "pkg/msg/OnlyOnA" in a.get_types(None)
    assert "pkg/msg/OnlyOnA" not in b.get_types(None)
