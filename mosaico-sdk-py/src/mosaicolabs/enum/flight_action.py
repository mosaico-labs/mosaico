from enum import StrEnum


class FlightAction(StrEnum):
    """
    Internal enumeration of PyArrow Flight action identifiers.

    This enum serves as the single source of truth for all action names used in
    the handshakes between the SDK and the Mosaico server.

    Important: Internal Use Only
        This class is part of the internal communication protocol. End-users
        should never need to use these identifiers directly, as they are
        abstracted by the public methods in [`MosaicoClient`][mosaicolabs.comm.MosaicoClient],
        [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter], and
        [`TopicWriter`][mosaicolabs.handlers.TopicWriter].
    """

    # --- Sequences related ---
    SEQUENCE_CREATE = "sequence_create"
    """Initiates the registration of a new sequence on the server."""

    SEQUENCE_FINALIZE = "sequence_finalize"
    """Marks a sequence as complete and makes its data immutable."""

    SEQUENCE_NOTIFY_CREATE = "sequence_notify_create"
    """Sends asynchronous notifications or error reports during the sequence creation phase."""

    SEQUENCE_NOTIFY_LIST = "sequence_notify_list"
    """Request the list of notifications for a specific sequence"""

    SEQUENCE_NOTIFY_PURGE = "sequence_notify_purge"
    """Request the deletion of the list of notifications for a specific sequence"""

    SEQUENCE_SYSTEM_INFO = "sequence_system_info"
    """Requests physical diagnostics such as storage size and lock status for a sequence."""

    SEQUENCE_ABORT = "sequence_abort"
    """Signals the server to stop an active ingestion and discard partial data."""

    SEQUENCE_DELETE = "sequence_delete"
    """Requests the permanent removal of a sequence and all associated topics from the server."""

    # --- Topics related ---
    TOPIC_CREATE = "topic_create"
    """Registers a new topic within an existing sequence context."""

    TOPIC_NOTIFY_CREATE = "topic_notify_create"
    """Reports errors or status updates specific to an individual topic stream."""

    TOPIC_NOTIFY_LIST = "topic_notify_list"
    """Request the list of notifications for a specific topic in a sequence"""

    TOPIC_NOTIFY_PURGE = "topic_notify_purge"
    """Request the deletion of the list of notifications for a topic in a sequence"""

    TOPIC_SYSTEM_INFO = "topic_system_info"
    """Requests storage and chunk metadata for an individual topic."""

    TOPIC_DELETE = "topic_delete"
    """Requests the permanent removal of a specific topic from the platform."""

    # # --- Layers related ---
    # LAYER_LIST = "layer_list"
    # """Retrieves the list of available abstraction layers."""

    # LAYER_CREATE = "layer_create"
    # """Defines a new logical layer on the server."""

    # LAYER_UPDATE = "layer_update"
    # """Modifies metadata or configuration for an existing layer."""

    # LAYER_DELETE = "layer_delete"
    # """Deletes a logical layer definition."""

    # --- Queries related ---
    QUERY = "query"
    """Commands a multi-layer search query against the platform."""
