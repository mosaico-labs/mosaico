from itertools import zip_longest
from pathlib import Path

import pytest
from rosbags.rosbag2 import StoragePlugin
from rosbags.typesys import Stores, get_typestore

from mosaicolabs import Pose, SessionLevelErrorPolicy
from mosaicolabs.bridges.ros import MosaicoLoader, ROSBridge, ROSLoader
from mosaicolabs.bridges.ros.sequence_extractor import (
    ROSExtractorConfig,
    ROSSequenceExtractor,
)
from testing.integration.config import UPLOADED_SEQUENCE_NAME

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
) -> ROSExtractorConfig:
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
    mosaico_client,
):

    overwritten_configs = override_ext_configs(
        default_extractor_config, ros_distro, storage_plugin
    )
    sequence_ext = ROSSequenceExtractor(overwritten_configs)
    sequence_ext.run()

    rosbag_file_path = get_rosbagfile_path(default_extractor_config)

    ros_loader = ROSLoader(file_path=rosbag_file_path, typestore_or_distro=ros_distro)

    shandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    assert shandler is not None
    # Assert all the topics are adapted and accepted
    assert len(ros_loader.topics) == len(shandler.topics)
    assert all(top in ros_loader.topics for top in shandler.topics)
    sstreamer = shandler.get_data_streamer()
    # All the messages are reconstructed
    assert ros_loader.msg_count() == sstreamer.msg_count

    for (ros_msg, _), data_steam_item in zip_longest(
        ros_loader, synthetic_sequence_data_stream.items
    ):
        adapter = ROSBridge.get_default_adapter(ros_msg.msg_type)
        assert adapter is not None
        reconstructed_ms_msg = adapter.translate(ros_msg)

        assert reconstructed_ms_msg.timestamp_ns == data_steam_item.msg.timestamp_ns
        assert reconstructed_ms_msg.data == data_steam_item.msg.data

    mosaico_client.close()


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


def test_valid_msgtype(mosaico_client):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-valid-msgtype"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": "geometry_msgs/msg/PoseStamped"}}

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            s_writer.topic_create(ros_topic_name, topic_with_ros_metadata, Pose)

        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        # Reading topic
        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )
        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter and adapter.ontology_data_type() is Pose
        assert (
            rosmsg_type is not None and rosmsg_type == "geometry_msgs/msg/PoseStamped"
        )

        mosaico_client.sequence_delete(ros_sequence_name)
