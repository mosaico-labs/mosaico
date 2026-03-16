from enum import Enum


class TopicLevelErrorPolicy(Enum):
    """
    Defines the behavior of the [`TopicWriter`][mosaicolabs.handlers.TopicWriter]
    when an exception occurs during ingestion.

    This policy determines how the platform handles partially uploaded data if the
    ingestion process is interrupted or fails.
    """

    Finalize = "finalize"
    """Notify server and close the topic (`is_active = False`)."""

    Ignore = "ignore"
    """Notify server but keep topic open for future `push()` calls."""

    Raise = "raise"
    """Propagate exception to trigger session-level policy."""
