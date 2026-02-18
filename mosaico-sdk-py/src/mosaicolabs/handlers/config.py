"""
Configuration Module.

This module defines the configuration structures used to control the behavior
of the writing process, including error handling policies and batching limits.
"""

from dataclasses import dataclass
from ..enum import OnErrorPolicy


@dataclass
class WriterConfig:
    """
    Configuration settings for Sequence and Topic writers.

    Note: Internal Usage
        This is currently **not a user-facing class**. It is automatically
        instantiated by the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] when
        allocating new [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter]
        instances via [`sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create].

    This dataclass defines the operational parameters for data ingestion, controlling
    both the error recovery strategy and the performance-critical buffering logic
    used by the [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] and
    [`TopicWriter`][mosaicolabs.handlers.TopicWriter].
    """

    on_error: OnErrorPolicy
    """
    Determines the terminal behavior when an exception occurs during the ingestion 
    lifecycle.
    
    * If set to [`OnErrorPolicy.Delete`][mosaicolabs.enum.OnErrorPolicy.Delete], the 
        system purges all data from the failed sequence.
    * If set to [`OnErrorPolicy.Report`][mosaicolabs.enum.OnErrorPolicy.Report], the 
        system retains the partial data in an **unlocked** state for debugging.
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
