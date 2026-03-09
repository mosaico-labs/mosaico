from enum import Enum


class SessionStatus(Enum):
    """
    Represents the operational lifecycle state of a Session upload for a Sequence during the ingestion process
    (see also [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter]).

    This enumeration tracks the state of a session from its initial creation through
    data writing until it reaches a terminal state (Finalized or Error).


    """

    Null = "null"
    """
    The initial state of a writer before server-side registration.
    
    In this state, the local [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] instance 
    has been created but the `SESSION_CREATE` handshake has not yet been performed 
    or completed.
    """

    Pending = "pending"
    """
    The session is registered on the server and actively accepting data.
    
    This state is entered upon successful execution of the `__enter__` method
    of the [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] class.
    While pending, the session allows for the 
    creation of new topics and the ingestion of data batches.
    """

    Finalized = "finalized"
    """
    The session has been successfully closed and its data is now immutable.
    
    This terminal state indicates that the `SequenceWriter._finalize()`
    action was acknowledged by the server. Once finalized, 
    the session is typically **locked** and cannot be deleted unless explicitly 
    unlocked by an administrator.
    """

    Error = "error"
    """
    The ingestion process failed or was explicitly aborted.
    
    This state is reached if an exception occurs within the `with` block or during 
    the finalization phase. Depending on the 
    [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy], the data may have been 
    purged (`Delete`) or retained in an **unlocked** state for debugging (`Report`).
    """


# Set the same for now. Can be made different in the future
SequenceStatus = SessionStatus
"""
Represents the operational lifecycle state of a Sequence during the ingestion process

Alias for [`SessionStatus`][mosaicolabs.enum.SessionStatus].
"""
