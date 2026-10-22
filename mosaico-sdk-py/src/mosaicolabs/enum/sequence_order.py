from enum import Enum


class SequenceOrder(Enum):
    """
    Sort order used when listing sequences. Ordering is always ascending.
    """

    CreatedAt = "created_at"
    """By creation time, oldest first."""

    Name = "name"
    """By sequence name, in byte order."""
