from enum import Enum


class FlightAction(Enum):
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

    SESSION_CREATE = "session_create"
    """Initiates the registration of a new session for an existing sequence on the server."""

    SESSION_FINALIZE = "session_finalize"
    """Marks a session as complete and makes its data immutable."""

    SEQUENCE_NOTIFICATION_CREATE = "sequence_notification_create"
    """Sends notifications or error reports during the sequence creation phase."""

    SEQUENCE_NOTIFICATION_LIST = "sequence_notification_list"
    """Request the list of notifications for a specific sequence"""

    SEQUENCE_NOTIFICATION_PURGE = "sequence_notification_purge"
    """Request the deletion of the list of notifications for a specific sequence"""

    SESSION_DELETE = "session_delete"
    """Requests the permanent removal of a session and all associated topics from the server."""

    SEQUENCE_DELETE = "sequence_delete"
    """Requests the permanent removal of a sequence and all associated topics from the server."""

    # --- Topics related ---
    TOPIC_CREATE = "topic_create"
    """Registers a new topic within an existing sequence context."""

    TOPIC_NOTIFICATION_CREATE = "topic_notification_create"
    """Reports errors or status updates specific to an individual topic stream."""

    TOPIC_NOTIFICATION_LIST = "topic_notification_list"
    """Request the list of notifications for a specific topic in a sequence"""

    TOPIC_NOTIFICATION_PURGE = "topic_notification_purge"
    """Request the deletion of the list of notifications for a topic in a sequence"""

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

    API_KEY_CREATE = "api_key_create"
    """Creates a new API key."""

    API_KEY_REVOKE = "api_key_revoke"
    """Revokes an existing API key."""

    API_KEY_STATUS = "api_key_status"
    """Checks the status of a specific API key."""

    # --- Arch related ---
    VERSION = "version"
    """Requests the backend version"""
