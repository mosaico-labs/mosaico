import pytest
import logging as log

from mosaicolabs.enum import OnErrorPolicy
from mosaicolabs.models.sensors import IMU
from mosaicolabs.comm import MosaicoClient


def test_sequence_report(_client: MosaicoClient):
    sequence_name = "sequence-to-report"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with _client.sequence_create(
            sequence_name, {}, on_error=OnErrorPolicy.Report
        ) as wseq:
            # There must be no problem in asking for a new TopicWriter
            assert wseq.topic_create(topic_name, {}, IMU) is not None
            # 'topic_name' already exist: must fail and return None
            log.info("Expected one (1) error after this line...")
            assert wseq.topic_create(topic_name, {}, IMU) is None
            # raise and exception to exit the context
            log.info("Expected one (1) error after this line...")
            raise Exception("__exception_in_test__")

    # The sequence is still present and not deleted
    shandler = _client.sequence_handler(sequence_name)
    # The sequence is still on the server
    assert shandler is not None
    # The list of registered topics corresponds to [topic_name]
    assert shandler.topics == [topic_name]
    # The topic exists although contains no data and no schema
    assert _client.topic_handler(sequence_name, topic_name) is not None

    # Free resources
    _client.sequence_delete(sequence_name)
    # This must be True...
    log.info("Expected one (1) error after this line...")
    assert _client.sequence_handler(sequence_name) is None

    # free resources
    _client.close()


def test_sequence_abort(_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with _client.sequence_create(
            sequence_name, {}, on_error=OnErrorPolicy.Delete
        ) as wseq:
            log.info("Expected one (1) error after this line...")
            wseq.topic_create(topic_name, {}, IMU)
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
    # The sequence is not available anymore (all the resources freed)
    log.info("Expected one (1) error after this line...")
    assert _client.sequence_handler(sequence_name) is None

    # free resources
    _client.close()
