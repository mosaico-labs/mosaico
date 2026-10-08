from copy import deepcopy
from pathlib import Path

import pytest

from mosaicolabs.bridges.mcap.sequence_extractor import (
    MCAPExtractorConfig,
    MCAPSequenceExtractor,
)
from testing.integration.config import (
    UPLOADED_MCAP_MIXED_SEQUENCE_NAME,
    UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME,
)


def override_mcap_configs(
    extractor_configs: MCAPExtractorConfig, sequence_name
) -> MCAPExtractorConfig:
    new_congis = deepcopy(extractor_configs)
    new_congis.sequence_name = sequence_name

    return new_congis


def get_mcap_path(configs: MCAPExtractorConfig) -> Path:

    mcap_folder_path = configs.saving_path / configs.sequence_name
    file_name = f"{configs.sequence_name}.mcap"

    return mcap_folder_path / file_name


def test_run_mcap_protobuf_existance(
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence,
    default_mcap_extractor_config,
):

    protobuf_mcap_extractor_config = override_mcap_configs(
        default_mcap_extractor_config, UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME
    )
    sequence_ext = MCAPSequenceExtractor(protobuf_mcap_extractor_config)
    sequence_ext.run()

    mcap_file_path = get_mcap_path(protobuf_mcap_extractor_config)

    assert mcap_file_path.exists()


def test_overwrite_false_raise_on_existing_path(
    default_mcap_extractor_config,
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
):
    protobuf_mcap_extractor_config = override_mcap_configs(
        default_mcap_extractor_config, UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME
    )

    # Creating rosbag from loaded sequence
    sequence_ext1 = MCAPSequenceExtractor(protobuf_mcap_extractor_config)
    sequence_ext1.run()

    # Creating the rosbag again with overwrite False should Raise a FileExistsError
    protobuf_mcap_extractor_config.overwrite = False
    sequence_ext2 = MCAPSequenceExtractor(protobuf_mcap_extractor_config)

    with pytest.raises(FileExistsError):
        sequence_ext2.run()


def test_overwrite_true_replaces_existing_mcap(
    default_mcap_extractor_config,
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
):
    protobuf_mcap_extractor_config = override_mcap_configs(
        default_mcap_extractor_config, UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME
    )

    # Creating rosbag from loaded sequence
    sequence_ext1 = MCAPSequenceExtractor(protobuf_mcap_extractor_config)
    sequence_ext1.run()

    # Creating the rosbag again with overwrite False should Raise a FileExistsError
    sequence_ext2 = MCAPSequenceExtractor(protobuf_mcap_extractor_config)
    sequence_ext2.run()

    assert get_mcap_path(protobuf_mcap_extractor_config).exists()


def test_not_existing_sequence_name(default_mcap_extractor_config):

    protobuf_mcap_extractor_config = override_mcap_configs(
        default_mcap_extractor_config, "not-existing-sequence-name"
    )

    sequence_ext = MCAPSequenceExtractor(protobuf_mcap_extractor_config)

    with pytest.raises(ValueError):
        sequence_ext.run()


# def test_valid_msgtype(mosaico_client):
#     ros_sequence_name = "ros-sequence-valid-msgtype"
#     ros_topic_name = "/car/pose"
#     topic_with_ros_metadata = {"_ros_": {"msgtype": "geometry_msgs/msg/PoseStamped"}}

#     with mosaico_client:
#         # Writing topic
#         with mosaico_client.sequence_create(
#             ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
#         ) as s_writer:
#             s_writer.topic_create(ros_topic_name, topic_with_ros_metadata, Pose)

#         t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

#         # Reading topic
#         mosaico_loader = MosaicoToROSLoader(
#             mosaico_client, get_typestore(ros_distro), ros_sequence_name
#         )
#         resolution = mosaico_loader._resolve_topic(t_handler)
#         adapter, rosmsg_type = resolution.adapter, resolution.native_msg_type

#         assert adapter and adapter.ontology_data_type() is Pose
#         assert (
#             rosmsg_type is not None and rosmsg_type == "geometry_msgs/msg/PoseStamped"
#         )

#         mosaico_client.sequence_delete(ros_sequence_name)


def test_run_mcap_mixed_existance(
    inject_mockup_sequence_mcap_mixed,  # required to inject mcap sequence,
    default_mcap_extractor_config,
):
    """Tests whether there are any problems when extracting a Mosaico Sequence coming
    from a MCAP with mixed Channel encodings"""

    mixed_mcap_extractor_config = override_mcap_configs(
        default_mcap_extractor_config, UPLOADED_MCAP_MIXED_SEQUENCE_NAME
    )

    sequence_ext = MCAPSequenceExtractor(mixed_mcap_extractor_config)
    sequence_ext.run()

    mcap_file_path = get_mcap_path(mixed_mcap_extractor_config)

    assert mcap_file_path.exists()
