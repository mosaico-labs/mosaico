import json
from typing import Any, Dict, Union


class ParsingError(Exception):
    """Raised when TopicResourceMetadata cannot be extracted from an endpoint."""

    pass


def _decode_app_metadata(
    app_mdata: Union[bytes, str],
) -> Dict[str, Any]:
    """
    Decodes and validates the raw App Metadata JSON payload.

    Args:
        app_mdata: JSON payload as a UTF-8 string or byte sequence.

    Returns:
        Dict[str, Any]: Decoded app_metadata JSON.

    Raises:
        ParsingError: If JSON cannot be decoded or it is not a dictionary.
    """
    # Decode input to string
    try:
        raw_str = (
            app_mdata.decode("utf-8") if isinstance(app_mdata, bytes) else app_mdata
        )
    except UnicodeDecodeError as e:
        raise ParsingError(f"App metadata bytes are not UTF-8, err '{e}'")

    # Check empty-string
    if not raw_str:
        raise ParsingError("Empty app_metadata")

    # Safely load into JSON
    try:
        data = json.loads(raw_str)
    except json.JSONDecodeError as e:
        raise ParsingError(f"Invalid JSON in app_metadata, err: '{e}'")

    # Validate format
    if not isinstance(data, dict):
        raise ParsingError(f"Expected JSON object, got {type(data).__name__}")

    return data
