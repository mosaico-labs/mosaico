from __future__ import annotations

import os
from typing import Any, Dict, Optional

from mosaicolabs_cli.utils.config import console, get_config_path, load_config
from mosaicolabs_cli.utils.env import DEFAULT_MOSAICO_PORT, MosaicoEnv


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
        port: Optional[int] = None,
        api_key: str = "",
        tls: bool = False,
        cert_path: str = "",
        name: str = "",
        is_default: bool = False,
    ):
        self.host, self.port = self._normalize_host_port(host, port)
        self.api_key = api_key.strip() if api_key else ""
        self.tls = tls
        self.cert_path = cert_path.strip() if cert_path else ""
        self.name = name
        self.is_default = is_default

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def resolve(cls, profile_name: Optional[str] = None) -> "MosaicoProfile":
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
        resolved_name = ""

        if profile_name:
            content = config_data.get(profile_name)
            if content is None or not isinstance(content, dict):
                console.print(
                    f"[bold red]Error:[/bold red] Profile '{profile_name}' not found in configuration."
                )
                raise SystemExit(1)
            profile_dict = content
            resolved_name = profile_name
        else:
            for _name, content in config_data.items():
                if isinstance(content, dict) and content.get("default", False):
                    profile_dict = content
                    resolved_name = _name
                    break

        if env_tls is not None:
            tls = env_tls.lower() in ("1", "true", "yes")
        else:
            tls = bool(profile_dict.get("tls", False))

        if env_host:
            resolved_host, resolved_port = cls._normalize_host_port(env_host)
        else:
            resolved_host = profile_dict.get("host", "")
            resolved_port = int(profile_dict.get("port", DEFAULT_MOSAICO_PORT))

        return cls(
            host=resolved_host,
            port=resolved_port,
            api_key=env_api_key or profile_dict.get("api_key", ""),
            tls=tls,
            cert_path=env_cert_path or profile_dict.get("cert_path", ""),
            name=resolved_name,
            is_default=bool(profile_dict.get("default", False)),
        )

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], name: str = "", is_default: bool = False
    ) -> "MosaicoProfile":
        """Create from a plain dictionary."""
        return cls(
            host=data.get("host", ""),
            port=int(data.get("port", DEFAULT_MOSAICO_PORT)),
            api_key=data.get("api_key", ""),
            tls=bool(data.get("tls", False)),
            cert_path=data.get("cert_path", ""),
            name=name,
            is_default=is_default,
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "api_key": self.api_key,
            "tls": self.tls,
            "cert_path": self.cert_path,
            "default": self.is_default,
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
    def _normalize_host_port(host: str, port: Optional[int] = None) -> tuple[str, int]:
        """
        Strip protocol prefix and extract embedded port if present.

        A port may come from the `port` argument or be embedded in `host`
        (e.g. "localhost:1234"), but not both. If neither is given, falls
        back to DEFAULT_MOSAICO_PORT.

        Raises:
            ValueError: If port is specified both as argument and embedded in host,
                or if embedded port is not a valid integer.
        """
        h = host.strip().rstrip("/")
        h = h.replace("http://", "").replace("https://", "")

        embedded_port: Optional[int] = None
        if ":" in h:
            host_part, port_str = h.rsplit(":", 1)
            if not port_str.isdigit():
                raise ValueError(f"Malformed input host '{h}'")
            embedded_port = int(port_str)
            h = host_part

        if embedded_port is not None and port is not None:
            raise ValueError(
                f"Port given twice: embedded={embedded_port}, argument={port}"
            )

        final_port = embedded_port if embedded_port is not None else port
        return h, final_port if final_port is not None else DEFAULT_MOSAICO_PORT

    def to_csv(self) -> str:
        """Format profile as a CSV row: name,host,port,is_default."""
        return f"{self.name},{self.host},{self.port},{str(self.is_default).lower()}"

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
        name_display = f"name='{self.name}', " if self.name else ""
        return (
            f"MosaicoProfile({name_display}host='{self.host}', port={self.port}, "
            f"{tls_display}, api_key='{key_display}', cert_path='{self.cert_path}')"
        )
