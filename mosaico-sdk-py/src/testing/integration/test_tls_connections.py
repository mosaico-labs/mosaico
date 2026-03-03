import pytest

from mosaicolabs.comm import MosaicoClient

from .config import UPLOADED_SEQUENCE_NAME


def test_tls_connection_from_path(host, port, tls_cert_path, with_tls):
    if not with_tls:
        pytest.skip("Tests run without '--tls'")
    client = MosaicoClient.connect(host, port, tls_cert_path=tls_cert_path)
    assert client.sequence_handler(UPLOADED_SEQUENCE_NAME) is not None
    client.close()


def test_tls_connection_from_env(host, port, with_tls):
    if not with_tls:
        pytest.skip("Tests run without '--tls'")
    client = MosaicoClient.from_env(host, port)
    assert client.sequence_handler(UPLOADED_SEQUENCE_NAME) is not None
    client.close()


def test_tls_connection_empty_path(host, port, with_tls):
    if not with_tls:
        pytest.skip("Tests run without '--tls'")
    with pytest.raises(ValueError):
        MosaicoClient.connect(host, port, tls_cert_path="")


def test_tls_connection_wrong_path(host, port, tls_cert_path, with_tls):
    if not with_tls:
        pytest.skip("Tests run without '--tls'")
    with pytest.raises(ValueError):
        MosaicoClient.connect(host, port, tls_cert_path=tls_cert_path + "/wrong_path")


def test_tls_connection_cert_not_found(host, port, tls_cert_path, with_tls):
    if not with_tls:
        pytest.skip("Tests run without '--tls'")
    tls_cert_path = tls_cert_path.replace("m", "x")
    with pytest.raises(FileNotFoundError):
        MosaicoClient.connect(host, port, tls_cert_path=tls_cert_path)
