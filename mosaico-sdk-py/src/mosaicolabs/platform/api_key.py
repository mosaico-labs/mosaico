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
        api_key_fingerprint: str,
        created_at_ns: int,
        expires_at_ns: int | None,
        description: str | None,
    ) -> None:
        self.api_key_fingerprint = api_key_fingerprint
        self.created_at_ns = created_at_ns
        self.expires_at_ns = expires_at_ns
        self.description = description

    def __repr__(self) -> str:
        return (
            "APIKeyStatus("
            f"api_key_fingerprint={self.api_key_fingerprint!r}, "
            f"created_at_ns={self.created_at_ns!r}, "
            f"expires_at_ns={self.expires_at_ns!r}, "
            f"description={self.description!r}"
            ")"
        )
