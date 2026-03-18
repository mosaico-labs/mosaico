"""
Configuration Module.

This module defines the configuration structures used to control the behavior
of the writing process, including error handling policies and batching limits.
"""

from dataclasses import dataclass

from ..enum import SessionLevelErrorPolicy, TopicLevelErrorPolicy


@dataclass
class WriterConfig:
    """
    Configuration for common settings for Sequence and Topic writers.

    Note: Internal Usage
        This is currently **not a user-facing class**. It is extended
        by the `SessionWriterConfig`.

    This dataclass defines the operational parameters for data ingestion, controlling
    both the error recovery strategy and the performance-critical buffering logic
    used by the [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] and
    [`TopicWriter`][mosaicolabs.handlers.TopicWriter].
    """

    max_batch_size_bytes: int
    """
    The memory threshold in bytes before a data batch is flushed to the server.
    
    When the internal buffer of a [`TopicWriter`][mosaicolabs.handlers.TopicWriter] 
    exceeds this value, it triggers a serialization and transmission event. 
    Larger values increase throughput by reducing network overhead but require more 
    client-side memory.
    """

    max_batch_size_records: int
    """
    The threshold in row (record) count before a data batch is flushed to the server.
    
    A flush is triggered whenever **either** this record limit or the 
    `max_batch_size_bytes` limit is reached, ensuring that data is transmitted 
    regularly even for topics with very small individual records.
    """


@dataclass
class SessionWriterConfig(WriterConfig):
    """
    Configuration settings for Sequence writers.

    Note: Internal Usage
        This is currently **not a user-facing class**. It is automatically
        instantiated by the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] when
        allocating new [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter]
        instances via [`sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create].

    This dataclass defines the operational parameters for data ingestion, controlling
    both the error recovery strategy and the performance-critical buffering logic
    used by the [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter].
    """

    on_error: SessionLevelErrorPolicy
    """
    Determines the terminal behavior when an exception occurs during the ingestion 
    lifecycle.
    
    * If set to [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete], the 
        system purges all data from the failed sequence.
    * If set to [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report], the 
        system retains the partial data in an **unlocked** state for debugging.
    """


@dataclass
class TopicWriterConfig(WriterConfig):
    """
    Configuration settings for Topic writers.

    Note: Internal Usage
        This is currently **not a user-facing class**. It is automatically
        instantiated by the [`_BaseSessionWriter`][mosaicolabs.handlers._BaseSessionWriter] when
        allocating new [`TopicWriter`][mosaicolabs.handlers.TopicWriter]
        instances via [`topic_create()`][mosaicolabs.handlers._BaseSessionWriter.sequence_create].

    This dataclass defines the operational parameters for data ingestion, controlling
    both the error recovery strategy and the performance-critical buffering logic
    used by the [`TopicWriter`][mosaicolabs.handlers.TopicWriter].
    """

    on_error: TopicLevelErrorPolicy
    """
    Determines the terminal behavior when an exception occurs during the ingestion 
    lifecycle.
    
    * If set to [`TopicLevelErrorPolicy.Finalize`][mosaicolabs.enum.TopicLevelErrorPolicy.Finalize], the 
        system notifies server and close the topic (`is_active = False`).
    * If set to [`TopicLevelErrorPolicy.Ignore`][mosaicolabs.enum.TopicLevelErrorPolicy.Ignore], the 
        system notifies server but keep topic open for future `push()` calls.
    * If set to [`TopicLevelErrorPolicy.Raise`][mosaicolabs.enum.TopicLevelErrorPolicy.Raise], the 
        system propagates exception to trigger session-level policy.
    """
