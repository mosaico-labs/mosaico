from __future__ import annotations

import os
from typing import Any, Dict, Optional

from mosaicolabs_cli.utils.config import console, get_config_path, load_config
from mosaicolabs_cli.utils.env import MosaicoEnv


class MosaicoProfile:
    """
    Represents a resolved connection profile for the Mosaico platform.

    Resolution precedence:
      1. Environment variables (MOSAICO_DAEMON_URL, MOSAICO_API_KEY, MOSAICO_TLS, MOSAICO_CERT_PATH)
      2. Named profile from config (via --profile flag or MOSAICO_PROFILE env)
      3. Default profile from config file (~/.mosaico/config.toml)
    """

    def __init__(
        self,
        host: str = "",
        port: int = 6276,
        api_key: str = "",
        tls: bool = False,
        cert_path: str = "",
    ):
        self.host, self.port = self._normalize_host_port(host, port)
        self.api_key = api_key.strip() if api_key else ""
        self.tls = tls
        self.cert_path = cert_path.strip() if cert_path else ""

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def resolve(
        cls, profile_name: Optional[str] = None, allow_empty: bool = False
    ) -> "MosaicoProfile":
        """
        Resolve a MosaicoProfile applying the full precedence chain.
        """
        env_host = os.getenv(MosaicoEnv.DAEMON_URL)
        env_api_key = os.getenv(MosaicoEnv.API_KEY)
        env_tls = os.getenv(MosaicoEnv.TLS)
        env_cert_path = os.getenv(MosaicoEnv.CERT_PATH)

        config_path = get_config_path()
        config_data = load_config(config_path)

        profile_dict: Dict[str, Any] = {}

        if profile_name:
            content = config_data.get(profile_name)
            if not isinstance(content, dict):
                console.print(
                    f"[bold red]Error:[/bold red] Profile '{profile_name}' not found in configuration."
                )
                raise SystemExit(1)
            profile_dict = content
        else:
            for _name, content in config_data.items():
                if isinstance(content, dict) and content.get("default", False):
                    profile_dict = content
                    break

        if not profile_dict and not env_host and not allow_empty:
            console.print(
                "[bold red]Error:[/bold red] No default profile found. "
                "Set a default profile or specify one using --profile."
            )
            raise SystemExit(1)

        # Resolve TLS: env > config
        if env_tls is not None:
            tls = env_tls.lower() in ("1", "true", "yes")
        else:
            tls = bool(profile_dict.get("tls", False))

        return cls(
            host=env_host or profile_dict.get("host", ""),
            port=6276 if env_host else int(profile_dict.get("port", 6276)),
            api_key=env_api_key or profile_dict.get("api_key", ""),
            tls=tls,
            cert_path=env_cert_path or profile_dict.get("cert_path", ""),
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MosaicoProfile":
        """Create from a plain dictionary."""
        return cls(
            host=data.get("host", ""),
            port=int(data.get("port", 6276)),
            api_key=data.get("api_key", ""),
            tls=bool(data.get("tls", False)),
            cert_path=data.get("cert_path", ""),
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "api_key": self.api_key,
            "tls": self.tls,
            "cert_path": self.cert_path,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def enable_tls(self) -> bool:
        """Whether TLS should be enabled for this connection."""
        return self.tls

    @property
    def tls_cert_path(self) -> Optional[str]:
        """Returns cert_path if set, None otherwise."""
        return self.cert_path if self.cert_path else None

    @staticmethod
    def _normalize_host_port(host: str, port: int) -> tuple[str, int]:
        """Strip protocol prefix and extract embedded port if present."""
        h = host.strip().rstrip("/")
        h = h.replace("http://", "").replace("https://", "")

        if port == 6276 and ":" in h:
            host_part, port_part = h.rsplit(":", 1)
            if port_part.isdigit():
                return host_part, int(port_part)

        return h, port

    def to_env(self) -> Dict[str, str]:
        """Export resolved profile fields as environment variables."""
        env: Dict[str, str] = {}
        if self.host:
            url = f"{self.host}:{self.port}" if self.port else self.host
            env[MosaicoEnv.DAEMON_URL] = url
        if self.api_key:
            env[MosaicoEnv.API_KEY] = self.api_key
        if self.tls:
            env[MosaicoEnv.TLS] = "true"
        if self.cert_path:
            env[MosaicoEnv.CERT_PATH] = self.cert_path
        return env

    def __repr__(self) -> str:
        key_display = "***" if self.api_key else "empty"
        tls_display = f"tls={'on' if self.tls else 'off'}"
        return f"MosaicoProfile(host='{self.host}', port={self.port}, {tls_display}, api_key='{key_display}', cert_path='{self.cert_path}')"
