from typing import Dict, List, Tuple

import pyarrow.flight as fl


class MosaicoAuthMiddleware(fl.ClientMiddleware):
    """Middleware adding the API token to every flight request."""

    def __init__(self, api_key):
        """
        Initialize the middleware

        Args:
            api_key (str): The API key to use for authentication
        """
        super().__init__()
        self._api_key: str = api_key

    def sending_headers(self) -> Dict[str, List[str] | List[bytes]]:
        """
        Called before sending headers to the server

        Returns:
            dict: Headers to be sent to the server
        """
        return {"mosaico-api-key-token": self._api_key.encode()}

    def received_headers(self, headers: Dict[str, List[str] | List[bytes]]):
        """
        Called after receiving headers from the server

        Args:
            headers (Dict[str, List[str] | List[bytes]]): Headers received from the server
        """
        pass


class MosaicoAuthMiddlewareFactory(fl.ClientMiddlewareFactory):
    """Factory to create istances of MosaicoAuthMiddleware."""

    def __init__(self, api_key):
        """
        Initialize the factory

        Args:
            api_key (str): The API key to use for authentication
        """
        super().__init__()
        self._api_key: str = api_key

        api_key_parts = self._get_key_parts()
        if api_key_parts is None:
            raise ValueError("Invalid format for API Key")

        _, _, self._fingerprint = api_key_parts

    def start_call(self, info: fl.CallInfo) -> MosaicoAuthMiddleware:
        """
        Called at every flight client operation (GetFlightInfo, DoAction, ecc.)

        Args:
            info (fl.CallInfo): Information about the flight call

        Returns:
            MosaicoAuthMiddleware: The middleware to be used for the flight call
        """
        return MosaicoAuthMiddleware(self._api_key)

    @property
    def api_key_fingerprint(self) -> str:
        """
        The fingerprint of the API key

        Returns:
            str: The fingerprint of the API key
        """
        return self._fingerprint

    def _get_key_parts(self) -> Tuple:
        """
        Split the API key into its components

        Returns:
            tuple: (header, payload, fingerprint)

        Raises:
            ValueError: If the API key is not in the correct format
        """
        parts = self._api_key.split("_")
        if len(parts) != 3:
            raise ValueError("Invalid format for API Key (wrong number of parts)")

        header, payload, fingerprint = parts

        if header != "msco":
            raise ValueError("Invalid format for API Key (missing 'msco')")

        if not (payload.isalnum() and fingerprint.isalnum()):
            raise ValueError("Invalid format for API Key (not alnum)")

        return header, payload, fingerprint
