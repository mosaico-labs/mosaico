from enum import Enum

DEFAULT_MOSAICO_PORT = 6726


class MosaicoEnv(str, Enum):
    """Canonical environment variable names for the Mosaico platform."""

    DAEMON_URL = "MOSAICO_DAEMON_URL"
    API_KEY = "MOSAICO_API_KEY"
    TLS = "MOSAICO_TLS"
    CERT_PATH = "MOSAICO_CERT_PATH"
    PROFILE = "MOSAICO_PROFILE"
    CONFIG_PATH = "MOSAICO_CONFIG_PATH"
