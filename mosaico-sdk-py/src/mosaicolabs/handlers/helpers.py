"""
Helper Utilities.

Provides utility functions for path manipulation, exception chaining,
and Flight ticket parsing.
"""

import string
from pathlib import Path
from typing import Any, Optional

# Set the supported name chars for sequence, topic and metadata key names
_SUPPORTED_SEQUENCE_NAME_CHARS = set(
    string.ascii_letters  # a-zA-Z
    + string.digits  # 0-9
    + "-_"
)
_SUPPORTED_TOPIC_NAME_CHARS = _SUPPORTED_SEQUENCE_NAME_CHARS | {"/"}
_SUPPORTED_METADATA_KEY_CHARS = _SUPPORTED_SEQUENCE_NAME_CHARS | {" "}

# Set the unsupported string for metadata key names
_UNSUPPORTED_METADATA_KEY_STRING = {"--"}


def _make_exception(msg: str, exc_msg: Optional[BaseException] = None) -> Exception:
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


def _validate_metadata_keys(metadata: dict[str, Any], _path: str = ""):
    """Recursively checks that every metadata key contains only supported chars"""

    for key, value in metadata.items():
        # Check that key is a string
        if not isinstance(key, str):
            raise ValueError(
                f"Metadata keys must be of string type, got {key!r} of type {type(key)}"
            )

        if not key:
            raise ValueError("Metadata key cannot be empty")

        full_key = f"{_path}.{key}" if _path else key

        # Dedup so a key like "a!!b" doesn't report the same invalid char twice
        unsupported_chars = [
            ch for ch in key if ch not in _SUPPORTED_METADATA_KEY_CHARS
        ]
        if unsupported_chars:
            raise ValueError(
                f"Metadata key '{full_key}' contains invalid characters: {unsupported_chars}"
            )

        unsupported_strings = [
            unsupp_str
            for unsupp_str in _UNSUPPORTED_METADATA_KEY_STRING
            if unsupp_str in key
        ]
        if unsupported_strings:
            raise ValueError(
                f"Metadata key '{full_key}' contains invalid strings: {unsupported_strings}"
            )

        _validate_nested_metadata_value(value, full_key)


def _validate_nested_metadata_value(value: Any, path: str):
    """Descends into dicts and list/tuple containers to reach metadata keys nested arbitrarily deep."""

    if isinstance(value, dict):
        _validate_metadata_keys(value, path)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_nested_metadata_value(item, path)


def _validate_metadata(metadata: dict[str, Any]):
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a dictionary")

    # Commented for now -> Uncomment in case check needs to be done in SDK
    # _validate_metadata_keys(metadata)


def _validate_sequence_name(name: str):
    if not name:
        raise ValueError("Empty sequence name")

    # managed malformed names pathlib cannot handle
    try:
        nbase = Path(name)
        if nbase.is_absolute():
            nbase = nbase.relative_to("/")
    except Exception as e:
        raise ValueError(f"Malformed sequence name, err: '{e}'")
    # Assert sequence name format
    nbase = str(nbase)
    # Sequence name contained only a '/'
    if not nbase:
        raise ValueError("Empty sequence name after '/' removal")
    # Check the first char is alphanumeric
    if not nbase[0].isalnum():
        raise ValueError("Sequence name does not begin with a letter or a number.")
    # Check the name does not contain unsupported chars

    unsupported_chars = [ch for ch in nbase if ch not in _SUPPORTED_SEQUENCE_NAME_CHARS]
    if unsupported_chars:
        raise ValueError(
            f"Sequence name contains invalid characters: {unsupported_chars}"
        )


def _validate_topic_name(name: str):
    if not name:
        raise ValueError("Empty topic name")

    # managed malformed names pathlib cannot handle
    try:
        nbase = Path(name)
        if nbase.is_absolute():
            nbase = nbase.relative_to("/")
    except Exception as e:
        raise ValueError(f"Malformed topic name, err: '{e}'")
    # Assert topic name format
    nbase = str(nbase)
    # Topic name contained only a '/'
    if not nbase:
        raise ValueError("Empty topic name after '/' removal")
    # Check the first char is alphanumeric
    if not nbase[0].isalnum():
        raise ValueError("Topic name does not begin with a letter or a number.")

    # Check the name does not contain unsupported chars
    unsupported_chars = [ch for ch in nbase if ch not in _SUPPORTED_TOPIC_NAME_CHARS]
    if unsupported_chars:
        raise ValueError(f"Topic name contains invalid characters: {unsupported_chars}")
