from itertools import zip_longest
from pathlib import Path

import pytest
from rosbags.rosbag2 import StoragePlugin
from rosbags.typesys import Stores

from mosaicolabs.ros_bridge import ROSBridge, ROSLoader
from mosaicolabs.ros_bridge.sequence_extractor import (
    ROSExtractorConfig,
    ROSSequenceExtractor,
)

ROS_DISTRO = [
    Stores.LATEST,
    Stores.ROS1_NOETIC,
    Stores.ROS2_DASHING,
    Stores.ROS2_ELOQUENT,
    Stores.ROS2_FOXY,
    Stores.ROS2_GALACTIC,
    Stores.ROS2_HUMBLE,
    Stores.ROS2_IRON,
    Stores.ROS2_JAZZY,
    Stores.ROS2_KILTED,
]

STORAGE_PLUGINS = [StoragePlugin.MCAP, StoragePlugin.SQLITE3]


def override_ext_configs(
    seq_ext_config: ROSExtractorConfig,
    ros_distro: Stores,
    storage_plugin: StoragePlugin,
) -> ROSSequenceExtractor:
    """Overrides default SequenceExtractor configs with passed ros_distro and storage_plugin"""
    seq_ext_config.ros_distro = ros_distro
    seq_ext_config.storage_plugin = storage_plugin

    return seq_ext_config


def get_rosbagfile_path(configs: ROSExtractorConfig) -> Path:

    rosbag_folder_path = configs.rosbag_path / configs.sequence_name
    file_name = configs.sequence_name

    if configs.ros_distro is Stores.ROS1_NOETIC:
        file_name += ".bag"
    else:
        if configs.storage_plugin == StoragePlugin.MCAP:
            file_name += ".mcap"
        elif configs.storage_plugin == StoragePlugin.SQLITE3:
            file_name += ".db3"

    return rosbag_folder_path / file_name


@pytest.mark.parametrize("ros_distro", ROS_DISTRO)
@pytest.mark.parametrize("storage_plugin", STORAGE_PLUGINS)
def test_run_rosbag_existance(
    inject_synthetic_sequence,  # necessary to trigger sequence ingestion in backed,
    default_extractor_config,
    ros_distro,
    storage_plugin,
):

    overwritten_configs = override_ext_configs(
        default_extractor_config, ros_distro, storage_plugin
    )
    sequence_ext = ROSSequenceExtractor(overwritten_configs)
    sequence_ext.run()

    rosbag_file_path = get_rosbagfile_path(default_extractor_config)

    assert rosbag_file_path.exists()


@pytest.mark.parametrize("ros_distro", ROS_DISTRO)
@pytest.mark.parametrize("storage_plugin", STORAGE_PLUGINS)
def test_run_data_correctness(
    inject_synthetic_sequence,  # necessary to trigger sequence ingestion in backed
    synthetic_sequence_data_stream,  # necessary since it holds the loaded sequence data
    default_extractor_config,
    ros_distro,
    storage_plugin,
):

    overwritten_configs = override_ext_configs(
        default_extractor_config, ros_distro, storage_plugin
    )
    sequence_ext = ROSSequenceExtractor(overwritten_configs)
    sequence_ext.run()

    rosbag_file_path = get_rosbagfile_path(default_extractor_config)

    ros_loader = ROSLoader(file_path=rosbag_file_path, typestore_name=ros_distro)

    for (ros_msg, _), data_steam_item in zip_longest(
        ros_loader, synthetic_sequence_data_stream.items
    ):
        adapter = ROSBridge.get_default_adapter(ros_msg.msg_type)
        reconstructed_ms_msg = adapter.translate(ros_msg)

        assert reconstructed_ms_msg.timestamp_ns == data_steam_item.msg.timestamp_ns
        assert reconstructed_ms_msg.data == data_steam_item.msg.data


def test_overwrite_false_raise_on_existing_path(
    default_extractor_config,
    inject_synthetic_sequence,  # necessary to trigger sequence ingestion in backed
):

    # Creating rosbag from loaded sequence
    sequence_ext1 = ROSSequenceExtractor(default_extractor_config)
    sequence_ext1.run()

    # Creating the rosbag again with overwrite False should Raise a FileExistsError
    default_extractor_config.overwrite = False
    sequence_ext2 = ROSSequenceExtractor(default_extractor_config)

    with pytest.raises(FileExistsError):
        sequence_ext2.run()


def test_overwrite_true_replaces_existing_bag(
    default_extractor_config,
    inject_synthetic_sequence,  # necessary to trigger sequence ingestion in backed
):

    # Creating rosbag from loaded sequence
    sequence_ext1 = ROSSequenceExtractor(default_extractor_config)
    sequence_ext1.run()

    # Creating the rosbag again with overwrite False should Raise a FileExistsError
    sequence_ext2 = ROSSequenceExtractor(default_extractor_config)
    sequence_ext2.run()

    assert get_rosbagfile_path(default_extractor_config).exists()


def test_not_existing_sequence_name(default_extractor_config):
    default_extractor_config.sequence_name = "not-existing-sequence-name"

    sequence_ext = ROSSequenceExtractor(default_extractor_config)

    with pytest.raises(ValueError):
        sequence_ext.run()
