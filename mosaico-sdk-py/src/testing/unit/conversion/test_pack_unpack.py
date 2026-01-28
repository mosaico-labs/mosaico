import pytest

from mosaicolabs.helpers import pack_topic_resource_name, unpack_topic_full_path
from mosaicolabs.handlers.helpers import (
    _validate_sequence_name,
    _validate_topic_name,
    _SUPPORTED_SEQUENCE_NAME_CHARS,
    _SUPPORTED_TOPIC_NAME_CHARS,
)


def test_pack_topic_resource_name():
    sequence_name = "/test_sequence"
    topic_name = "/my/topic/name"

    assert (
        pack_topic_resource_name(sequence_name, topic_name)
        == "test_sequence/my/topic/name"
    )

    sequence_name = "test_sequence"
    topic_name = "my/topic/name"

    assert (
        pack_topic_resource_name(sequence_name, topic_name)
        == "test_sequence/my/topic/name"
    )


def test_unpack_topic_resource_name():
    topic_resrc_name = "test_sequence/my/topic/name"
    sname_tname = unpack_topic_full_path(topic_resrc_name)
    assert sname_tname is not None
    sname, tname = sname_tname
    assert sname == "test_sequence" and tname == "/my/topic/name"

    topic_resrc_name = "/test_sequence/my/topic/name"
    sname_tname = unpack_topic_full_path(topic_resrc_name)
    assert sname_tname is not None
    sname, tname = sname_tname
    assert sname == "test_sequence" and tname == "/my/topic/name"

    assert unpack_topic_full_path("not-unpacked-str") is None
    assert unpack_topic_full_path("/not-unpacked-str") is None


@pytest.mark.parametrize(
    "supported_char",
    [p for p in _SUPPORTED_TOPIC_NAME_CHARS],
)
def test_validate_topic_name(supported_char: str):
    _validate_topic_name("my/topic/name")
    _validate_topic_name("/my/topic/name")
    _validate_topic_name(f"my/topic{supported_char}name")
    _validate_topic_name(f"/my/topic{supported_char}name")


@pytest.mark.parametrize(
    "supported_char",
    [p for p in _SUPPORTED_SEQUENCE_NAME_CHARS],
)
def test_validate_sequence_name(supported_char: str):
    _validate_sequence_name("my-sequence-name")
    _validate_sequence_name("/my-sequence-name")
    _validate_sequence_name(f"my-sequence{supported_char}name")
    _validate_sequence_name(f"/my-sequence{supported_char}name")
