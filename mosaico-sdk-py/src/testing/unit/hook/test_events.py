import json

import pytest

from mosaicolabs.hook import SessionUploaded, TopicUploaded
from mosaicolabs.hook.events import _parse_event

# The same bodies mosaicod's own tests pin, in `mosaicod-marshal/src/hook.rs`.
TOPIC_UPLOADED = {
    "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
    "type": "topic_uploaded",
    "data": {
        "topic": "drive_042/front_camera",
        "sequence": "drive_042",
        "session": "drive_042:session",
        "ontology_tag": "image",
        "user_metadata": {"driver": "Ada"},
        "created_at": 1_000,
    },
}

SESSION_UPLOADED = {
    "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
    "type": "session_uploaded",
    "data": {
        "session": "drive_042:session",
        "sequence": "drive_042",
        "topics": ["drive_042/front_camera", "drive_042/imu"],
        "created_at": 1_000,
    },
}


def parse(payload) -> object:
    return _parse_event(json.dumps(payload).encode())


def test_topic_uploaded():
    assert parse(TOPIC_UPLOADED) == TopicUploaded(
        id="0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
        sequence_name="drive_042",
        topic_name="/front_camera",
        session_locator="drive_042:session",
        ontology_tag="image",
        user_metadata={"driver": "Ada"},
        created_timestamp=1_000,
    )


def test_nested_topic_name():
    payload = json.loads(json.dumps(TOPIC_UPLOADED))
    payload["data"]["topic"] = "drive_042/sensors/front_camera"

    assert parse(payload).topic_name == "/sensors/front_camera"


def test_topic_uploaded_without_user_metadata():
    payload = json.loads(json.dumps(TOPIC_UPLOADED))
    payload["data"]["user_metadata"] = None

    assert parse(payload).user_metadata is None


def test_session_uploaded():
    assert parse(SESSION_UPLOADED) == SessionUploaded(
        id="0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
        sequence_name="drive_042",
        session_locator="drive_042:session",
        topics=["/front_camera", "/imu"],
        created_timestamp=1_000,
    )


def test_unknown_fields_are_ignored():
    payload = json.loads(json.dumps(TOPIC_UPLOADED))
    payload["retries"] = 2
    payload["data"]["completed_at"] = 2_000

    assert isinstance(parse(payload), TopicUploaded)


def test_unknown_type_is_none():
    assert parse({"id": "x", "type": "sequence_deleted", "data": {}}) is None


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        json.dumps({"type": "topic_uploaded", "data": {}}).encode(),
        json.dumps({**TOPIC_UPLOADED, "data": {"topic": "drive_042/a"}}).encode(),
        json.dumps(
            {**TOPIC_UPLOADED, "data": {**TOPIC_UPLOADED["data"], "created_at": "1"}}
        ).encode(),
        json.dumps(
            {**TOPIC_UPLOADED, "data": {**TOPIC_UPLOADED["data"], "created_at": True}}
        ).encode(),
        json.dumps(
            {**SESSION_UPLOADED, "data": {**SESSION_UPLOADED["data"], "topics": [1]}}
        ).encode(),
    ],
)
def test_malformed_body_is_rejected(body):
    with pytest.raises(ValueError):
        _parse_event(body)
