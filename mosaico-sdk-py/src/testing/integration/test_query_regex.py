import pytest
from pyarrow import ArrowInvalid
from pyarrow.flight import FlightError

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.query import QuerySequence, QueryTopic
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_SEQUENCE_NAME,
)


def test_query_topic_name_regex_start_anchor(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    # ^/front matches only topics whose name starts with /front
    query_resp = mosaico_client.query(
        QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
        QueryTopic().with_name_match("^/front"),
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

    mosaico_client.close()


def test_query_topic_name_regex_end_anchor(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    # imu$ matches topics whose name ends with imu (both front and camera imu)
    query_resp = mosaico_client.query(
        QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
        QueryTopic().with_name_match("imu$"),
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    expected = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_IMU_CAMERA_TOPIC]
    assert len(query_resp[0].topics) == len(expected)
    assert all(t.name in expected for t in query_resp[0].topics)

    mosaico_client.close()


def test_query_topic_name_regex_or(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    # (imu|gps) matches topics containing either imu or gps
    query_resp = mosaico_client.query(
        QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
        QueryTopic().with_name_match("(imu|gps)"),
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    expected = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_IMU_CAMERA_TOPIC, UPLOADED_GPS_TOPIC]
    assert len(query_resp[0].topics) == len(expected)
    assert all(t.name in expected for t in query_resp[0].topics)

    mosaico_client.close()


def test_query_topic_name_regex_catch_all(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    # .* matches all topics in the sequence
    query_resp = mosaico_client.query(
        QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
        QueryTopic().with_name_match(".*"),
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert len(query_resp[0].topics) == 4

    mosaico_client.close()


def test_query_sequence_name_regex_anchor(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    # ^<exact-name>$ matches exactly one sequence
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match(f"^{UPLOADED_SEQUENCE_NAME}$"),
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

    mosaico_client.close()


def test_query_topic_name_match_empty_pattern_rejected(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    with pytest.raises(ArrowInvalid):
        mosaico_client.query(QueryTopic().with_name_match(""))

    mosaico_client.close()


def test_query_sequence_name_match_empty_pattern_rejected(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    with pytest.raises(ArrowInvalid):
        mosaico_client.query(QuerySequence().with_name_match(""))

    mosaico_client.close()


def test_query_topic_name_match_invalid_regex_rejected(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    with pytest.raises(FlightError):
        mosaico_client.query(QueryTopic().with_name_match("((unclosed"))

    mosaico_client.close()


def test_query_sequence_name_match_invalid_regex_rejected(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    with pytest.raises(FlightError):
        mosaico_client.query(QuerySequence().with_name_match("((unclosed"))

    mosaico_client.close()


def test_query_sequence_with_percentage(
    mosaico_client: MosaicoClient, inject_synthetic_sequence
):
    query_resp = mosaico_client.query(QuerySequence().with_name_match("%"))
    assert query_resp is not None and query_resp.is_empty()

    mosaico_client.close()


def test_query_topic_with_percentage(
    mosaico_client: MosaicoClient, inject_synthetic_sequence
):
    query_resp = mosaico_client.query(QueryTopic().with_name_match("%"))
    assert query_resp is not None and query_resp.is_empty()

    mosaico_client.close()
