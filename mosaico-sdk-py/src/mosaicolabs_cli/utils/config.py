from enum import Enum
from typing import Any, Dict, List

from rich.console import Console

console = Console()
error_console = Console(stderr=True)


class OutputFormat(str, Enum):
    TABLE = "table"
    CSV = "csv"


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
