import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .errors import ProfileConfigError, ProfileExistsError, ProfileNotFoundError

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

DEFAULT_PORT = 6726
CONFIG_PATH_ENV = "MOSAICO_CONFIG_PATH"
DEFAULT_CONFIG_PATH = Path.home() / ".mosaico" / "config.toml"


def normalize_host_port(host: str, port: Optional[int] = None) -> Tuple[str, int]:
    """
    Strip `http(s)://` and trailing `/` from `host` and split off an embedded
    port (`"localhost:1234"`). Raises `ValueError` if the port is given twice
    or is not a number. Without a port, `DEFAULT_PORT` is used.
    """
    h = host.strip().rstrip("/").replace("http://", "").replace("https://", "")
    if ":" in h:
        h, port_str = h.rsplit(":", 1)
        if not port_str.isdigit():
            raise ValueError(f"Malformed input host '{h}:{port_str}'")
        if port is not None:
            raise ValueError(f"Port given twice: embedded={port_str}, argument={port}")
        port = int(port_str)
    return h, DEFAULT_PORT if port is None else port


@dataclass(frozen=True)
class Profile:
    """A named connection profile. `host` is normalized on creation."""

    name: str
    host: str = ""
    port: Optional[int] = None
    api_key: str = field(default="", repr=False)
    tls: bool = False
    cert_path: str = ""
    default: bool = False

    def __post_init__(self) -> None:
        host, port = normalize_host_port(self.host, self.port)
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "api_key", self.api_key.strip())
        object.__setattr__(self, "cert_path", self.cert_path.strip())

    @classmethod
    def _from_table(cls, name: str, table: Dict[str, Any]) -> "Profile":
        try:
            return cls(
                name=name,
                host=str(table.get("host", "")),
                port=int(table.get("port", DEFAULT_PORT)),
                api_key=str(table.get("api_key", "")),
                tls=bool(table.get("tls", False)),
                cert_path=str(table.get("cert_path", "")),
                default=bool(table.get("default", False)),
            )
        except (TypeError, ValueError) as e:
            raise ProfileConfigError(f"Invalid profile '{name}': {e}") from e


class ProfileStore:
    """
    The profiles in a Mosaico configuration file, found by default in
    `$MOSAICO_CONFIG_PATH` or `~/.mosaico/config.toml`.

    Concurrent writers are not locked against each other. At most one
    profile is the default: the first profile added becomes the default, and
    removing the default promotes the first remaining profile. A file with more
    than one default raises `ProfileConfigError`.
    """

    def __init__(self, path: "str | os.PathLike[str] | None" = None):
        self.path = Path(path or os.getenv(CONFIG_PATH_ENV) or DEFAULT_CONFIG_PATH)

    def list(self) -> List[Profile]:
        return [
            Profile._from_table(name, table) for name, table in self._load().items()
        ]

    def get(self, name: str) -> Profile:
        return Profile._from_table(name, self._require(self._load(), name))

    def get_default(self) -> Optional[Profile]:
        return next((p for p in self.list() if p.default), None)

    def add(self, profile: Profile, *, overwrite: bool = False) -> Profile:
        """
        Save `profile` and return it with its final `default` flag.

        `overwrite` decides what happens when a profile with the same name
        already exists: False raises `ProfileExistsError`, True replaces it.

        `profile.default` asks to make it the default. It also becomes the
        default when it is the first profile, or when it overwrites the current
        default, so editing the default never leaves the file without one.
        The other profiles then lose the flag.
        """
        data = self._load()
        existing = data.get(profile.name)
        if existing is not None and not overwrite:
            raise ProfileExistsError(f"Profile '{profile.name}' already exists")
        replaces_default = existing is not None and existing.get("default", False)
        is_default = profile.default or replaces_default or not data
        if is_default:
            for table in data.values():
                table["default"] = False
        saved = replace(profile, default=is_default)
        data[profile.name] = {
            "host": saved.host,
            "port": saved.port,
            "api_key": saved.api_key,
            "tls": saved.tls,
            "cert_path": saved.cert_path,
            "default": saved.default,
        }
        self._save(data)
        return saved

    def remove(self, name: str) -> Optional[Profile]:
        """Remove a profile. Return the profile promoted to default, if any."""
        data = self._load()
        was_default = self._require(data, name).get("default", False)
        del data[name]
        promoted = None
        if was_default and data:
            first = next(iter(data))
            data[first]["default"] = True
            promoted = Profile._from_table(first, data[first])
        self._save(data)
        return promoted

    def set_default(self, name: str) -> Profile:
        data = self._load()
        self._require(data, name)
        for profile_name, table in data.items():
            table["default"] = profile_name == name
        self._save(data)
        return Profile._from_table(name, data[name])

    @staticmethod
    def _require(data: Dict[str, Dict[str, Any]], name: str) -> Dict[str, Any]:
        if name not in data:
            raise ProfileNotFoundError(f"Profile '{name}' does not exist")
        return data[name]

    def _load(self) -> Dict[str, Dict[str, Any]]:
        try:
            with open(self.path, "rb") as f:
                data = tomllib.load(f)
        except FileNotFoundError:
            return {}
        except (OSError, tomllib.TOMLDecodeError) as e:
            raise ProfileConfigError(f"Cannot read {self.path}: {e}") from e
        if not all(isinstance(table, dict) for table in data.values()):
            raise ProfileConfigError(f"{self.path}: only profile tables are allowed")
        defaults = [name for name, table in data.items() if table.get("default")]
        if len(defaults) > 1:
            raise ProfileConfigError(
                f"{self.path}: more than one default profile ({', '.join(defaults)})"
            )
        return data

    def _save(self, data: Dict[str, Dict[str, Any]]) -> None:
        tmp = None
        try:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.path.parent)  # mode 0600
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(_dump(data))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except OSError as e:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            raise ProfileConfigError(f"Cannot write {self.path}: {e}") from e


def _dump(data: Dict[str, Dict[str, Any]]) -> str:
    """Write profile tables as TOML. Strings use JSON escaping, valid in TOML."""
    lines = []
    for name, table in data.items():
        lines.append(f"[{_key(name)}]")
        for key, value in table.items():
            if isinstance(value, dict):
                raise ProfileConfigError(f"Nested table '{name}.{key}' not supported")
            lines.append(f"{_key(key)} = {json.dumps(value, ensure_ascii=False)}")
        lines.append("")
    return "\n".join(lines)


def _key(key: str) -> str:
    return key if re.fullmatch(r"[A-Za-z0-9_-]+", key) else json.dumps(key)
