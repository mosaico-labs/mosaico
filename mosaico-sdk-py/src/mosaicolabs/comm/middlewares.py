import pyarrow.flight as fl


class MosaicoAuthMiddleware(fl.ClientMiddleware):
    """Middleware adding the API token to every flight request."""

    def __init__(self, api_key):
        super().__init__()
        self._api_key: str = api_key

    def sending_headers(self) -> dict[str, list[str] | list[bytes]]:
        return {"mosaico-api-key-token": self._api_key.encode()}

    def received_headers(self, headers):
        pass

    @property
    def api_key(self):
        return self._api_key


class MosaicoAuthMiddlewareFactory(fl.ClientMiddlewareFactory):
    """Factory to create istances of MosaicoAuthMiddleware."""

    def __init__(self, api_key):
        super().__init__()
        self._api_key = api_key

    def start_call(self, info):
        """Called at every flight client operation (GetFlightInfo, DoAction, ecc.)"""
        return MosaicoAuthMiddleware(self._api_key)
