import pytest

from mosaicolabs.comm.mosaico_client import MosaicoClient


def test_timeout():
    with pytest.raises(
        ConnectionError,
        match="Inner err: 'Server did not become available within 2s'",
    ):
        MosaicoClient.connect(host="invalid-address", port=0, timeout=2)
