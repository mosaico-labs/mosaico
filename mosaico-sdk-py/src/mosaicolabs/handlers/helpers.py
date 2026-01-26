"""
Helper Utilities.

Provides utility functions for path manipulation, exception chaining,
and Flight ticket parsing.
"""

from pathlib import Path
from typing import Optional

# Set the unsupported name chars for sequence and topic names
_UNSUPPORTED_TOPIC_NAME_CHARS = ["!", '"', "'", "*", "£", "$", "%", "&"]
_UNSUPPORTED_SEQUENCE_NAME_CHARS = _UNSUPPORTED_TOPIC_NAME_CHARS + ["/"]


def _make_exception(msg: str, exc_msg: Optional[Exception] = None) -> Exception:
    """
    Creates a new exception that chains an inner exception's message.
    Useful for adding context to low-level Flight errors.

    Args:
        msg (str): The high-level error message.
        exc_msg (Optional[Exception]): The original exception.

    Returns:
        Exception: A new exception combining both messages.
    """
    if exc_msg is None:
        return Exception(msg)
    else:
        return Exception(f"{msg}\nInner err: {exc_msg}")


def _validate_sequence_name(name: str):
    if not name:
        raise ValueError("Empty sequence name")
    nbase = Path(name)
    if nbase.is_absolute():
        nbase = nbase.relative_to("/")
    # Assert sequence name format
    nbase = str(nbase)
    # Sequence name contained only a '/'
    if not nbase:
        raise ValueError("Empty sequence name after '/' removal")
    # Check the first char is alphanumeric
    if not nbase[0].isalnum():
        raise ValueError("Sequence name does not begin with a letter or a number.")
    # Check the name does not contain unsupported chars

    if any(ch in nbase for ch in _UNSUPPORTED_SEQUENCE_NAME_CHARS):
        raise ValueError(
            f"Sequence name contains invalid characters: {_UNSUPPORTED_SEQUENCE_NAME_CHARS}"
        )


def _validate_topic_name(name: str):
    if not name:
        raise ValueError("Empty topic name")
    nbase = Path(name)
    if nbase.is_absolute():
        nbase = nbase.relative_to("/")
    # Assert topic name format
    nbase = str(nbase)
    # Topic name contained only a '/'
    if not nbase:
        raise ValueError("Empty topic name after '/' removal")
    # Check the first char is alphanumeric
    if not nbase[0].isalnum():
        raise ValueError("Topic name does not begin with a letter or a number.")
    # Check the name does not contain unsupported chars
    if any(ch in nbase for ch in _UNSUPPORTED_TOPIC_NAME_CHARS if ch != "/"):
        raise ValueError(
            f"Topic name contains invalid characters: {_UNSUPPORTED_TOPIC_NAME_CHARS}"
        )
