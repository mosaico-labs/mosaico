from ..types import Time


def _get_fingerprint(api_key: str) -> str:
    """
    Extract the fingerprint from the API key

    Returns:
        str: The API Key fingerprint

    Raises:
        ValueError: If the API key is not in the correct format
    """

    parts = api_key.split("_")
    if len(parts) != 3:
        raise ValueError("Invalid format for API Key (wrong number of parts)")

    header, payload, fingerprint = parts

    if header != "msco":
        raise ValueError("Invalid format for API Key (missing 'msco')")

    if not (payload.isalnum() and fingerprint.isalnum()):
        raise ValueError("Invalid format for API Key (not alnum)")

    if len(fingerprint) != 8:
        raise ValueError("Invalid format for API Key fingerprint")

    return fingerprint


class APIKeyStatus:
    """
    Represents the status information of an API key.

    This object is returned by :meth:`MosaicoClient.api_key_status`.

    Parameters
    ----------
    api_key_fingerprint : str
        Unique identifier of the API key.
    created_at_ns : int
        Timestamp (in nanoseconds) indicating when the API key was created.
    expires_at_ns : int | None
        Timestamp (in nanoseconds) indicating when the API key expires.
        May be None if the key does not expire.
    description : str | None
        Optional description associated with the API key.
    """

    def __init__(
        self,
        created_at_ns: int,
        expires_at_ns: int | None,
        description: str | None,
    ) -> None:
        self.created_at_ns = created_at_ns
        self.expires_at_ns = expires_at_ns
        self.description = description

    @property
    def is_expired(self) -> bool:
        """Whether the API key is expired"""
        return (
            self.expires_at_ns is not None
            and self.expires_at_ns < Time.now().to_nanoseconds()
        )

    def __repr__(self) -> str:
        return (
            "APIKeyStatus("
            f"created_at_ns={self.created_at_ns!r}, "
            f"expires_at_ns={self.expires_at_ns!r}, "
            f"description={self.description!r}"
            ")"
        )
