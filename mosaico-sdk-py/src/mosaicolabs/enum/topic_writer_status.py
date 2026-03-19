from enum import Enum


class TopicWriterStatus(Enum):
    """
    Represents the operational lifecycle state of a Topic upload for a specific Session during the ingestion process
    (see also [`TopicWriter`][mosaicolabs.handlers.TopicWriter]).

    This enumeration tracks the state of a topic writer from its initial creation through
    data writing until it reaches a terminal state.

    Note:
        The `FinalizedWithError`, `IgnoredLastError` and `RaisedException` values can only be tracked
        if the `TopicWriter` is used in a `with` context.
    """

    Active = "active"
    """
    The initial state of a topic writer before server-side registration.
    
    In this state, the local [`TopicWriter`][mosaicolabs.handlers.TopicWriter] instance 
    has been created and the `TOPIC_CREATE` handshake has completed.
    """

    Finalized = "finalized"
    """
    The topic writer has been successfully closed and its data is now immutable.
    
    This terminal state indicates that the `TopicWriter._finalize()`
    action was acknowledged by the server. Once finalized, 
    the topic writer is **locked** and cannot be used to push records.
    """

    FinalizedWithError = "finalized_with_error"
    """
    The topic writer has been finalized with an error. 
    
    This state is reached when the TopicWriter is used in a context and 
    its error policy is set to `TopicLevelErrorPolicy.Finalize`.
    
    This terminal state indicates that the `TopicWriter._finalize()`
    function was called with an error. Once finalized, 
    the topic writer is **locked** and cannot be used to push records.
    """

    IgnoredLastError = "ignored_last_error"
    """
    The topic writer is still active and can be used to push data on the platform.

    This state is reached when the TopicWriter is used in a context and 
    its error policy is set to `TopicLevelErrorPolicy.Ignore`.

    This temporary state indicates that the last time the `with` context exited,
    it was due to an error.
    """

    RaisedException = "raised_exception"
    """
    The topic writer encountered an error in its `with` block.
    The error handling is delegated to the outer 
    [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] error handling policy
    ([`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy])
    , or any try-except outer block.

    This state is reached when the TopicWriter is used in a context and 
    its error policy is set to `TopicLevelErrorPolicy.Raise`.
    """
