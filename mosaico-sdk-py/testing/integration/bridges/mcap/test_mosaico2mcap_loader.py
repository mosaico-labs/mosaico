import pytest

from mosaicolabs.bridges.mcap import (
    MCAPInjectionConfig,
    McapReturnType,
    MosaicoToMCAPLoader,
)
from mosaicolabs.bridges.mcap.adapters import UnmodeledAdapter as McapUnmodeledAdapter
from mosaicolabs.bridges.mcap.helpers import _sanitize_mcap_name
from mosaicolabs.bridges.topic_status import TopicStatus
from testing.unit.bridges.config import (
    ALL_CHANNEL_NAMES,
    ALL_PROTOBUF_MSGTYPES,
    N_STEPS,
    START_TIME_NS,
    START_TIME_S,
    STEP_NS,
)


@pytest.fixture
def mosaico2mcap_protobuf_loader(
    mosaico_client, default_mcap_protobuf_injector_config: MCAPInjectionConfig
) -> MosaicoToMCAPLoader:
    sequence_name = default_mcap_protobuf_injector_config.sequence_name
    return MosaicoToMCAPLoader(mosaico_client, sequence_name)


def test_mosaico_to_ros_loader_properties(
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
    mosaico2mcap_protobuf_loader: MosaicoToMCAPLoader,
):
    """Test that loader properties are funtioning"""

    assert len(mosaico2mcap_protobuf_loader.topics) == len(ALL_CHANNEL_NAMES)

    start_time_ns = START_TIME_S * 1e9 + START_TIME_NS
    end_time_ns = start_time_ns + STEP_NS * N_STEPS - STEP_NS
    assert mosaico2mcap_protobuf_loader.duration == end_time_ns - start_time_ns
    assert mosaico2mcap_protobuf_loader.filtered_topics == []


def test_mosaico_to_ros_loader_resolve_adapter_by_topic_handler(
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
    mosaico2mcap_protobuf_loader: MosaicoToMCAPLoader,
):
    """Test that for each loaded topic it is possible to find an adapter using topic handler"""

    # verify that for each channel an UnmodeledAdapter has been created
    for mcap_channel_name in ALL_CHANNEL_NAMES:
        topic_name = _sanitize_mcap_name(mcap_channel_name)

        th = mosaico2mcap_protobuf_loader._client.topic_handler(
            mosaico2mcap_protobuf_loader._sequence_name, topic_name
        )
        assert th is not None

        topic_resolution = mosaico2mcap_protobuf_loader._resolve_topic(th)

        assert topic_resolution.is_accepted
        assert topic_resolution.status == TopicStatus.ACCEPTED
        assert topic_resolution.native_msg_type is not None
        assert topic_resolution.native_msg_type in ALL_PROTOBUF_MSGTYPES
        assert topic_resolution.adapter is not None
        assert issubclass(topic_resolution.adapter, McapUnmodeledAdapter)


def test_mosaico_to_ros_loader_resolve_adapter(
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
    mosaico2mcap_protobuf_loader: MosaicoToMCAPLoader,
):
    """Test that the loaded is capable of resolving an Unmodeled Adapter for all topics by name"""

    for mcap_channel_name in ALL_CHANNEL_NAMES:
        topic_name = _sanitize_mcap_name(mcap_channel_name)

        adapter = mosaico2mcap_protobuf_loader.resolve_adapter(topic_name)

        assert adapter is not None
        assert issubclass(adapter, McapUnmodeledAdapter)


def test_mosaico_to_ros_loader_original_protobuf_instance(
    inject_mockup_sequence_mcap_protobuf,  # required to inject mcap sequence
    mosaico2mcap_protobuf_loader: MosaicoToMCAPLoader,
    channelname_to_protobuf,
):
    """Test that loader is capable of translating mosaico messages into the original protobuf instance"""

    for t_name, msco_msg in mosaico2mcap_protobuf_loader:
        adapter = mosaico2mcap_protobuf_loader.resolve_adapter(t_name)
        assert adapter is not None

        # Testing to_mcap() -> brings mosaico message to original type
        adapter.to_mcap(msco_msg.data)

        # Testing to_native() -> brings mosaico message to bytes understandable by mcap writer
        mcap_return_type = adapter.to_native(msco_msg)
        assert isinstance(mcap_return_type, McapReturnType)
        assert isinstance(mcap_return_type.data_bytes, bytes)


# TODO: add test loading MCAP with jsonchema encoding
