import os
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console

from mosaicolabs_cli.utils.env import MosaicoEnv

# Fallback mechanism for TOML parsing based on Python version
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

console = Console()
error_console = Console(stderr=True)

DEFAULT_CONFIG_FOLDER = ".mosaico"
DEFAULT_CONFIG_FILENAME = "config.toml"
DEFAULT_CONFIG_PATH = Path.home() / DEFAULT_CONFIG_FOLDER / DEFAULT_CONFIG_FILENAME


class OutputFormat(str, Enum):
    TABLE = "table"
    CSV = "csv"


def get_config_path() -> Path:
    """Resolve the configuration file path based on the environment or default."""
    env_path = os.getenv(MosaicoEnv.CONFIG_PATH)
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH


def load_config(path: Optional[Path] = None) -> dict:
    """
    Safely load existing TOML configuration or return an empty dict if missing.
    If no `path` is passed, the default one is used.
    """
    if path is None:
        path = get_config_path()

    if not path.exists():
        return {}
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        error_console.print(
            f"[bold red]Error:[/bold red] Failed to parse config file: {e}"
        )
        raise typer.Exit(code=1)


def serialize_to_toml(data: dict) -> str:
    """Helper to convert a flat nested dict into a clean TOML string injection-free."""
    lines = []
    for section, parameters in data.items():
        lines.append(f"[{section}]")
        for key, value in parameters.items():
            if isinstance(value, bool):
                val_str = "true" if value else "false"
            elif value is None:
                continue
            else:
                escaped_val = str(value).replace('"', '\\"')
                val_str = f'"{escaped_val}"'
            lines.append(f"{key} = {val_str}")
        lines.append("")
    return "\n".join(lines)


def _flatten_metadata(data: Dict[str, Any], prefix: str = "") -> List[str]:
    """Flatten nested metadata dict to dot-notation key=value pairs."""
    parts = []
    for key, value in data.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            parts.extend(_flatten_metadata(value, full_key))
        else:
            parts.append(f"{full_key}={value}")
    return parts
