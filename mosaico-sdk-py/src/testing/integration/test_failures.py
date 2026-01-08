"""
These tests require the connection to the server (localhost)
"""

import pytest
import logging as log
import pyarrow as pa

from mosaicolabs.handlers import TopicWriter
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import SequenceStatus, SerializationFormat
from testing.integration.config import UPLOADED_SEQUENCE_NAME


def test_invalid_host():
    with pytest.raises(
        ConnectionError,
        match="Connection to Flight server at 'invalid-address:0' failed on startup",
    ):
        MosaicoClient.connect(host="invalid-address", port=0, timeout=0)


def test_read_non_existing_sequence_topic(_client: MosaicoClient):
    log.info("Expected three (3) errors after this line...")
    assert _client.sequence_handler("non-existing-sequence") is None
    assert (
        _client.topic_handler(sequence_name="non-existing", topic_name="/topic") is None
    )

    # free resources
    _client.close()


def test_sequence_writer_not_in_context(_client: MosaicoClient):
    swriter = _client.sequence_create("new-sequence", metadata={})
    assert swriter.sequence_status() == SequenceStatus.Null
    with pytest.raises(
        RuntimeError, match="SequenceWriter must be used within a 'with' block."
    ):
        swriter._check_entered()

    # free resources
    _client.close()


def test_sequence_invalid_name(_client: MosaicoClient):
    sequence_name = "invalid/sequence/name"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(ValueError, match="Invalid characters '/' in sequence name"):
        with _client.sequence_create(sequence_name, {}) as _:
            pass

    sequence_name = "/invalid/sequence/name"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(ValueError, match="Invalid characters '/' in sequence name"):
        with _client.sequence_create(sequence_name, {}) as _:
            pass

    # free resources
    _client.close()


class NotSerializable:
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "field",
                pa.float32(),
                nullable=False,
            ),
        ]
    )
    # Define Serializable inner variables, to make the test passing the getattr error
    __ontology_tag__ = "not_serializable"
    __serialization_format__ = SerializationFormat.Ragged
    __class_type__ = type
    field: float


def test_topic_push_not_serializable(_client: MosaicoClient):
    ontology_type = type
    # Check raise value
    with pytest.raises(ValueError, match="is not serializable"):
        # type must fail here
        TopicWriter._validate_ontology_type(ontology_type)  # type: ignore (disable pylance complaining)

    # We do not want to keep the next created sequence on the server: we will raise an Exception
    # to trigger the Abort mechanism (which will be tested in a separate test). This block is necessary
    # to make the test successfull (do not fail after raised exception)
    with pytest.raises(ChildProcessError):
        with _client.sequence_create("test-seq-not-seerializable", {}) as sw:
            # This must fail: type is not serializable
            tw = sw.topic_create("test-topic-unregistered", {}, ontology_type)  # type: ignore (disable pylance complaining)
            assert tw is None

            # This must fail: type is not serializable, although has all the variables injected by Serializable,
            # but it is not a subclass
            tw = sw.topic_create("test-topic-registered", {}, NotSerializable)  # type: ignore (disable pylance complaining)
            assert tw is None

            # do not want to keep this sequence on the server...
            # Generate a specific Exception which is not raised by above functions
            # (we want to be sure the test runs till here)
            raise ChildProcessError

    # free resources
    _client.close()


def test_non_existing_topic_handler(
    _client: MosaicoClient,
    _inject_sequence_data_stream,
):
    """Test the exception raising of non-existing topic handler from sequence"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None

    with pytest.raises(
        ValueError, match="Topic 'non-existing-topic-name' not available in sequence"
    ):
        seqhandler.get_topic_handler("non-existing-topic-name")

    # free resources
    _client.close()


def test_sequence_streamer_non_existing_topics(
    _client: MosaicoClient,
    _inject_sequence_data_stream,
):
    """Test the exception raising of non-existing topics when spawning data-stream from sequence"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None

    with pytest.raises(ValueError, match="Invalid input topic names"):
        seqhandler.get_data_streamer(
            topics=[
                "non-existing-topic-name",
            ]
        )

    # free resources
    _client.close()
