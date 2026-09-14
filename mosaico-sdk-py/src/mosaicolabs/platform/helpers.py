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
        app_mdata (Union[bytes, str]): JSON payload as a UTF-8 string or byte sequence.

    Returns:
        Dict[str, Any]: Decoded app_metadata JSON.

    Raises:
        ParsingError: If JSON cannot be decoded or it is not a dictionary.
    """
    try:
        decoded_app_metadata = json.loads(app_mdata)
    except Exception as e:
        raise ParsingError(f"Error decoding app metadata, err '{e}'")

    if not isinstance(decoded_app_metadata, dict):
        raise ParsingError(
            f"Decoded app metadata is not a dictionary, got '{type(decoded_app_metadata)}'"
        )

    return decoded_app_metadata
