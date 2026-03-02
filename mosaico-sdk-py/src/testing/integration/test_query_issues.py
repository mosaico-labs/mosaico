import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.platform import Sequence, Topic
from mosaicolabs.models.query.builders import QuerySequence, QueryTopic
from testing.integration.config import (
    UPLOADED_SEQUENCE_NAME,
    UPLOADED_IMU_FRONT_TOPIC,
)


def test_query_bug_218_fail(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # --- Passing Test ---
    # Test Existence of fields alone:
    # Query by sequence metadata ONLY
    query_resp = _client.query(
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["visibility"].eq(("team-01"))
        ),
    )

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

    # --- Passing Test ---
    # Test Existence of fields alone:
    # Query by sequence metadata ONLY
    query_resp = _client.query(
        QueryTopic().with_expression(
            Topic.Q.user_metadata["sensor_id"].eq(("imu_front_01"))
        ),
    )

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

    # --- FIXME: Failing test ---
    # Query by sequence metadata AND topic name
    query_resp = _client.query(
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["visibility"].eq(("team-01"))
        ),
        QueryTopic().with_name(UPLOADED_IMU_FRONT_TOPIC),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

    # --- FIXME: Failing test 2 ---
    # Query by topic metadata AND sequence name
    query_resp = _client.query(
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["visibility"].eq(("team-01"))  # Tested ok above
        ),
        QueryTopic().with_expression(
            Topic.Q.user_metadata["sensor_id"].eq(("imu_front_01"))  # Tested ok above
        ),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

    # free resources
    _client.close()
