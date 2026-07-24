from typing import Dict, List

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

    def start_call(self, info: fl.CallInfo) -> MosaicoAuthMiddleware:
        """
        Called at every flight client operation (GetFlightInfo, DoAction, ecc.)

        Args:
            info (fl.CallInfo): Information about the flight call

        Returns:
            MosaicoAuthMiddleware: The middleware to be used for the flight call
        """
        return MosaicoAuthMiddleware(self._api_key)
