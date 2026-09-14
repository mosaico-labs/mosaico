"""
Integration tests for the write-side batch-size safety margin.

`_TopicWriteState` (mosaicolabs.handlers.internal.topic_write_state) decides when to
flush a buffered batch using a threshold (`max_batch_size_bytes`) derived from the
*real* server's reported message-size limit
(`ConnectionContext.default_max_batch_size_bytes`, see mosaicolabs.comm.connection).

These tests exercise that logic end-to-end against a live server, sizing payloads
relative to the server's actual reported limit instead of a hardcoded assumption,
so they stay meaningful regardless of how a given deployment is configured.
"""

import logging
import os

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.comm.notifications import NotificationType
from mosaicolabs.enum.topic_writer_status import TopicWriterStatus
from mosaicolabs.models.core import Message
from mosaicolabs.models.sensors import CompressedImage, ImageFormat


def test_topic_writer_splits_oversized_uploads_into_multiple_batches(
    mosaico_client: MosaicoClient,
):
    """
    Pushing enough data to exceed the server's message-size limit must not fail:
    `_TopicWriteState` must transparently split the upload across multiple Flight
    `do_put` batches, and every message must be retrievable afterwards, unchanged.
    """
    sequence_name = "test-batch-splitting-sequence"
    topic_name = "/oversized_topic"

    max_batch_size_bytes = mosaico_client._connection.default_max_batch_size_bytes()

    # Size records so that pushing a handful of them forces at least one flush
    # boundary crossing (~1.5x max_batch_size_bytes in total), without moving an
    # unnecessarily large volume of data.
    record_size_bytes = max(1024, max_batch_size_bytes // 4)
    num_records = 6
    payloads = [os.urandom(record_size_bytes) for _ in range(num_records)]

    with mosaico_client.sequence_create(sequence_name, {}) as swriter:
        twriter = swriter.topic_create(topic_name, {}, CompressedImage)
        assert twriter is not None
        for i, payload in enumerate(payloads):
            twriter.push(
                Message(
                    timestamp_ns=i,
                    data=CompressedImage(data=payload, format=ImageFormat.JPEG),
                )
            )

    # Round-trip: every message must have survived the split, byte-for-byte.
    seqhandler = mosaico_client.sequence_handler(sequence_name)
    assert seqhandler is not None
    thandler = seqhandler.get_topic_handler(topic_name)
    streamer = thandler.get_data_streamer()
    assert streamer.msg_count == num_records

    received = {msg.timestamp_ns: msg.data.data for msg in streamer}
    assert received == {i: payloads[i] for i in range(num_records)}

    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()


def test_topic_writer_drops_single_record_exceeding_server_limit(
    mosaico_client: MosaicoClient,
    caplog,
):
    """
    A single record whose serialized size alone exceeds the server's message-size
    limit cannot be split any further. `TopicWriter.push()` does not raise for
    this: it drops the record, reports it as a topic-level notification, and
    reflects it locally via `status`/`last_error`/`dropped_record_count`, while
    the writer stays active for subsequent records.
    """
    sequence_name = "test-oversized-single-record-sequence"
    topic_name = "/oversized_single_record"

    max_batch_size_bytes = mosaico_client._connection.default_max_batch_size_bytes()
    oversized_payload = os.urandom(max_batch_size_bytes + 1024 * 1024)

    with caplog.at_level(logging.ERROR, logger="mosaicolabs"):
        with mosaico_client.sequence_create(sequence_name, {}) as swriter:
            twriter = swriter.topic_create(topic_name, {}, CompressedImage)
            assert twriter is not None

            twriter.push(
                Message(
                    timestamp_ns=0,
                    data=CompressedImage(
                        data=os.urandom(1024), format=ImageFormat.JPEG
                    ),
                )
            )
            assert twriter.status == TopicWriterStatus.Active

            # This single record alone exceeds the server's limit: push() must
            # not raise, but the record must never reach the server.
            twriter.push(
                Message(
                    timestamp_ns=1,
                    data=CompressedImage(
                        data=oversized_payload, format=ImageFormat.JPEG
                    ),
                )
            )
            assert twriter.status == TopicWriterStatus.RecordTooLarge
            assert twriter.last_error is not None
            assert twriter.dropped_record_count == 1
            # The writer must remain usable after the drop.
            assert twriter.is_active

            twriter.push(
                Message(
                    timestamp_ns=2,
                    data=CompressedImage(
                        data=os.urandom(1024), format=ImageFormat.JPEG
                    ),
                )
            )
            # A successful push resets the status back to Active.
            assert twriter.status == TopicWriterStatus.Active

    assert any(
        "exceeds gRPC limit" in record.message
        and "Record will be skipped" in record.message
        for record in caplog.records
    ), "Expected an error log about the oversized record being skipped"

    # The drop must also be visible server-side as a topic notification, not
    # just in local logs.
    tnotifies = mosaico_client.list_topic_notifications(
        sequence_name=sequence_name, topic_name=topic_name
    )
    assert any(
        n.type == NotificationType.Error
        and "Record dropped for topic" in n.message
        and "its size alone exceeds" in n.message
        for n in tnotifies
    )

    seqhandler = mosaico_client.sequence_handler(sequence_name)
    assert seqhandler is not None
    thandler = seqhandler.get_topic_handler(topic_name)
    streamer = thandler.get_data_streamer()
    # Only the 2 small records made it; the oversized one was silently dropped.
    assert streamer.msg_count == 2
    assert {msg.timestamp_ns for msg in streamer} == {0, 2}

    mosaico_client.sequence_delete(sequence_name)
    mosaico_client.close()
