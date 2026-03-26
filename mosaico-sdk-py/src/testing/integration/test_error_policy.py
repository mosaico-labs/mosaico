import logging as log

import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.comm.notifications import NotificationType
from mosaicolabs.enum import (
    OnErrorPolicy,
    SessionLevelErrorPolicy,
    TopicLevelErrorPolicy,
)
from mosaicolabs.enum.topic_writer_status import TopicWriterStatus
from mosaicolabs.models.data.geometry import Vector3d
from mosaicolabs.models.message import Message
from mosaicolabs.models.sensors import IMU


def test_sequence_report(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-report"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with mosaico_client.sequence_create(
            sequence_name, {}, on_error=SessionLevelErrorPolicy.Report
        ) as wseq:
            # There must be no problem in asking for a new TopicWriter
            assert wseq.topic_create(topic_name, {}, IMU) is not None
            # 'topic_name' already exist: must fail and return None
            log.info("Expected one (1) error after this line...")
            assert wseq.topic_create(topic_name, {}, IMU) is None
            # raise and exception to exit the context
            log.info("Expected one (1) error after this line...")
            raise Exception("__exception_in_test__")

    snotifies = mosaico_client.list_sequence_notifications(sequence_name=sequence_name)
    assert len(snotifies) == 1
    assert snotifies[0].sequence_name == sequence_name
    assert snotifies[0].type == NotificationType.ERROR
    assert "Inner err: '__exception_in_test__'" in snotifies[0].message

    tnotifies = mosaico_client.list_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    assert len(tnotifies) == 1
    assert tnotifies[0].sequence_name == sequence_name
    assert tnotifies[0].topic_name == topic_name
    assert tnotifies[0].type == NotificationType.ERROR
    assert "Inner err: '__exception_in_test__'" in snotifies[0].message

    mosaico_client.clear_sequence_notifications(sequence_name=sequence_name)
    snotifies = mosaico_client.list_sequence_notifications(sequence_name=sequence_name)
    assert len(snotifies) == 0

    mosaico_client.clear_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    tnotifies = mosaico_client.list_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    assert len(tnotifies) == 0

    # The sequence is still present and not deleted (on_error=SessionLevelErrorPolicy.Report)
    shandler = mosaico_client.sequence_handler(sequence_name)
    # The sequence is still on the server
    assert shandler is not None
    # The list of registered topics corresponds to [topic_name]
    assert shandler.topics == [topic_name]
    # The topic exists although contains no data and no schema
    assert mosaico_client.topic_handler(sequence_name, topic_name) is not None

    # Free resources
    mosaico_client.sequence_delete(sequence_name)
    # This must be True...
    log.info("Expected one (1) error after this line...")
    assert mosaico_client.sequence_handler(sequence_name) is None

    # free resources
    mosaico_client.close()


def test_topic_level_error_policy_finalize(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with mosaico_client.sequence_create(
        sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as wseq:
        log.info("Expected one (1) error after this line...")
        twriter = wseq.topic_create(
            topic_name, {}, IMU, on_error=TopicLevelErrorPolicy.Finalize
        )
        assert twriter is not None
        with twriter:
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
        assert twriter.status == TopicWriterStatus.FinalizedWithError
        assert twriter.last_error == "__exception_in_test__"
        assert twriter.is_active is False

        # cannot push data
        with pytest.raises(Exception, match="called on uninitialized state"):
            twriter.push(
                Message(
                    timestamp_ns=0,
                    data=IMU(
                        acceleration=Vector3d(x=0, y=0, z=0),
                        angular_velocity=Vector3d(x=0, y=0, z=0),
                    ),
                )
            )
        # The last push reset the error
        assert twriter.status == TopicWriterStatus.RaisedException
        assert "called on uninitialized state" in twriter.last_error

    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is not None
    thandler = shandler.get_topic_handler(topic_name)
    assert thandler is not None
    assert thandler.locked is True

    # free resources
    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()


def test_topic_level_error_policy_finalize_multi_topic(
    mosaico_client: MosaicoClient,
):
    sequence_name = "sequence-to-delete"
    topic_name_1 = "/topic1"
    topic_name_2 = "/topic2"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with mosaico_client.sequence_create(
        sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as wseq:
        log.info("Expected one (1) error after this line...")
        twriter_1 = wseq.topic_create(
            topic_name_1, {}, IMU, on_error=TopicLevelErrorPolicy.Finalize
        )
        twriter_2 = wseq.topic_create(
            topic_name_2, {}, IMU, on_error=TopicLevelErrorPolicy.Finalize
        )
        assert twriter_1 is not None
        assert twriter_2 is not None
        with twriter_1:
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
        assert twriter_1.status == TopicWriterStatus.FinalizedWithError
        assert twriter_1.last_error == "__exception_in_test__"
        assert twriter_1.is_active is False

        # cannot push data
        with pytest.raises(Exception, match="called on uninitialized state"):
            twriter_1.push(
                Message(
                    timestamp_ns=0,
                    data=IMU(
                        acceleration=Vector3d(x=0, y=0, z=0),
                        angular_velocity=Vector3d(x=0, y=0, z=0),
                    ),
                )
            )
        # The last push reset the error
        assert twriter_1.status == TopicWriterStatus.RaisedException
        assert "called on uninitialized state" in twriter_1.last_error

        # Demonstrate topic independence
        with twriter_2:
            # This is ok and is successfull
            twriter_2.push(
                Message(
                    timestamp_ns=0,
                    data=IMU(
                        acceleration=Vector3d(x=0, y=0, z=0),
                        angular_velocity=Vector3d(x=0, y=0, z=0),
                    ),
                )
            )

    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is not None
    thandler_1 = shandler.get_topic_handler(topic_name_1)
    assert thandler_1 is not None
    assert thandler_1.locked is True
    thandler_2 = shandler.get_topic_handler(topic_name_2)
    assert thandler_2 is not None
    assert thandler_2.locked is True

    # Demonstrate the push in /topic_2 has been made
    assert thandler_2.chunks_number > 0

    # free resources
    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()


def test_topic_level_error_policy_ignore(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with mosaico_client.sequence_create(
        sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as wseq:
        log.info("Expected one (1) error after this line...")
        twriter = wseq.topic_create(
            topic_name, {}, IMU, on_error=TopicLevelErrorPolicy.Ignore
        )
        assert twriter is not None
        with twriter:
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
        assert twriter.status == TopicWriterStatus.IgnoredLastError
        assert twriter.last_error == "__exception_in_test__"
        assert twriter.is_active is True
        # still can push data
        twriter.push(
            Message(
                timestamp_ns=0,
                data=IMU(
                    acceleration=Vector3d(x=0, y=0, z=0),
                    angular_velocity=Vector3d(x=0, y=0, z=0),
                ),
            )
        )
        # The last push reset the error
        assert twriter.status == TopicWriterStatus.Active
        assert twriter.last_error is None

    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is not None
    thandler = shandler.get_topic_handler(topic_name)
    assert thandler is not None
    assert thandler.locked is True

    # free resources
    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()


def test_topic_level_error_policy_ignore_multi_topic(
    mosaico_client: MosaicoClient,
):
    sequence_name = "sequence-to-delete"
    topic_name_1 = "/topic1"
    topic_name_2 = "/topic2"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with mosaico_client.sequence_create(
        sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as wseq:
        log.info("Expected one (1) error after this line...")
        twriter_1 = wseq.topic_create(
            topic_name_1, {}, IMU, on_error=TopicLevelErrorPolicy.Ignore
        )
        assert twriter_1 is not None
        with twriter_1:
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
        assert twriter_1.status == TopicWriterStatus.IgnoredLastError
        assert twriter_1.last_error == "__exception_in_test__"
        assert twriter_1.is_active is True
        # still can push data
        twriter_1.push(
            Message(
                timestamp_ns=0,
                data=IMU(
                    acceleration=Vector3d(x=0, y=0, z=0),
                    angular_velocity=Vector3d(x=0, y=0, z=0),
                ),
            )
        )
        # The last push reset the error
        assert twriter_1.status == TopicWriterStatus.Active
        assert twriter_1.last_error is None

        # Demonstrate topic independence
        twriter_2 = wseq.topic_create(
            topic_name_2, {}, IMU, on_error=TopicLevelErrorPolicy.Ignore
        )
        assert twriter_2 is not None
        # still can push data
        twriter_2.push(
            Message(
                timestamp_ns=0,
                data=IMU(
                    acceleration=Vector3d(x=0, y=0, z=0),
                    angular_velocity=Vector3d(x=0, y=0, z=0),
                ),
            )
        )
        # The last push reset the error
        assert twriter_2.status == TopicWriterStatus.Active
        assert twriter_2.last_error is None

    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is not None
    thandler_1 = shandler.get_topic_handler(topic_name_1)
    assert thandler_1 is not None
    assert thandler_1.locked is True
    thandler_2 = shandler.get_topic_handler(topic_name_2)
    assert thandler_2 is not None
    assert thandler_2.locked is True
    # Demonstrate the push in /topic_2 has been made
    assert thandler_2.chunks_number > 0

    # free resources
    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()


def test_topic_level_error_policy_raise(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with pytest.raises(
        Exception, match="__exception_in_test__"
    ):  # The exception bubbles up to here
        with mosaico_client.sequence_create(
            sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
        ) as wseq:
            log.info("Expected one (1) error after this line...")
            twriter = wseq.topic_create(
                topic_name, {}, IMU, on_error=TopicLevelErrorPolicy.Raise
            )
            assert twriter is not None
            with twriter:
                # raise and exception to exit the context
                # must bubble up to SequenceWriter.__exit__ which deletes the data
                raise Exception("__exception_in_test__")

            # Everything here is skipped

    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is None

    # free resources
    mosaico_client.close()


def test_sequence_abort(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with mosaico_client.sequence_create(
            sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
        ) as wseq:
            log.info("Expected one (1) error after this line...")
            wseq.topic_create(topic_name, {}, IMU)
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
    # The sequence is not available anymore (all the resources freed)
    log.info("Expected one (1) error after this line...")

    # Free resources
    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is None

    # free resources
    mosaico_client.close()


# TODO: Delete before 0.4.0
def test_sequence_report_deprecated_policy(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-report"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with mosaico_client.sequence_create(
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

    snotifies = mosaico_client.list_sequence_notifications(sequence_name=sequence_name)
    assert len(snotifies) == 1
    assert snotifies[0].sequence_name == sequence_name
    assert snotifies[0].type == NotificationType.ERROR
    assert "Inner err: '__exception_in_test__'" in snotifies[0].message

    tnotifies = mosaico_client.list_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    assert len(tnotifies) == 1
    assert tnotifies[0].sequence_name == sequence_name
    assert tnotifies[0].topic_name == topic_name
    assert tnotifies[0].type == NotificationType.ERROR
    assert "Inner err: '__exception_in_test__'" in snotifies[0].message

    mosaico_client.clear_sequence_notifications(sequence_name=sequence_name)
    snotifies = mosaico_client.list_sequence_notifications(sequence_name=sequence_name)
    assert len(snotifies) == 0

    mosaico_client.clear_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    tnotifies = mosaico_client.list_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    assert len(tnotifies) == 0

    # The sequence is still present and not deleted (on_error=OnErrorPolicy.Report)
    shandler = mosaico_client.sequence_handler(sequence_name)
    # The sequence is still on the server
    assert shandler is not None
    # The list of registered topics corresponds to [topic_name]
    assert shandler.topics == [topic_name]
    # The topic exists although contains no data and no schema
    assert mosaico_client.topic_handler(sequence_name, topic_name) is not None

    # Free resources
    mosaico_client.sequence_delete(sequence_name)
    # This must be True...
    log.info("Expected one (1) error after this line...")
    assert mosaico_client.sequence_handler(sequence_name) is None

    # free resources
    mosaico_client.close()


def test_sequence_abort_deprecated_policy(mosaico_client: MosaicoClient):
    sequence_name = "sequence-to-delete"
    topic_name = "/topic1"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the delete condition
    with pytest.raises(Exception, match="__exception_in_test__"):
        with mosaico_client.sequence_create(
            sequence_name, {}, on_error=OnErrorPolicy.Delete
        ) as wseq:
            log.info("Expected one (1) error after this line...")
            wseq.topic_create(topic_name, {}, IMU)
            # raise and exception to exit the context
            raise Exception("__exception_in_test__")
    # The sequence is not available anymore (all the resources freed)
    log.info("Expected one (1) error after this line...")

    # Free resources
    shandler = mosaico_client.sequence_handler(sequence_name)
    assert shandler is None

    # free resources
    mosaico_client.close()
