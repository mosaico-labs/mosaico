"""
Options and helpers shared by the `ros` and `mcap` bridge commands.
"""

import json
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from typing_extensions import Annotated

from mosaicolabs_cli.utils.config import error_console
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile

# Exit code of a run interrupted by the user (128 + SIGINT)
EXIT_INTERRUPTED = 130


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


PATTERNS_HELP = (
    "Prefix a pattern with '!' to exclude it. Repeatable; patterns are evaluated in "
    "order. If only exclusions are provided, everything is included except what they "
    "exclude."
)

# --- Shared options ---

TopicsOpt = Annotated[
    Optional[List[str]],
    typer.Option(
        "--topics",
        "-t",
        help=(
            "Topic pattern to filter (supports glob wildcards like '/cam/*' or "
            f"'*camera_info'). {PATTERNS_HELP}"
        ),
    ),
]
DryRunOpt = Annotated[
    bool,
    typer.Option(
        "--dry-run",
        "--dry_run",
        help="Resolve topics/adapters/rejections and print a report, without writing any data.",
    ),
]
LogLevelOpt = Annotated[
    LogLevel,
    typer.Option(
        "--log",
        "-l",
        case_sensitive=False,
        help="Logging verbosity level.",
    ),
]

# --- Injection options ---

InputFileArg = Annotated[
    Path,
    typer.Argument(exists=True, help="Path to the file to inject."),
]
SequenceNameOpt = Annotated[
    str, typer.Option("--name", "-n", help="Name of the target sequence.")
]
UpdateIfExistsOpt = Annotated[
    bool,
    typer.Option(
        "--update-if-exists",
        help=(
            "If the sequence already exists, append this file's topics to it instead of "
            "failing (e.g. multi-part recordings, or reprocessed results)."
        ),
    ),
]
MetadataOpt = Annotated[
    Optional[str],
    typer.Option(
        "--metadata",
        help="Sequence metadata, as a JSON string or the path to a JSON file.",
    ),
]
TopicMetadataOpt = Annotated[
    Optional[str],
    typer.Option(
        "--topic-metadata",
        help=(
            "Mapping of exact topic name to metadata, as a JSON string or the path to a "
            'JSON file (e.g. \'{"/imu": {"unit": "rad/s"}}\').'
        ),
    ),
]

# --- Extraction options ---

SequenceNameArg = Annotated[
    str, typer.Argument(help="Name of the Mosaico sequence to extract.")
]
StartTimestampOpt = Annotated[
    Optional[int],
    typer.Option(
        "--start-timestamp-ns",
        "--start_timestamp_ns",
        help="Inclusive lower-bound timestamp (epoch ns) from which to start extracting.",
    ),
]
EndTimestampOpt = Annotated[
    Optional[int],
    typer.Option(
        "--end-timestamp-ns",
        "--end_timestamp_ns",
        help="Exclusive upper-bound timestamp (epoch ns) at which to stop extracting.",
    ),
]
OverwriteOpt = Annotated[
    bool,
    typer.Option(
        "--overwrite",
        help="Delete and recreate the output path if it already exists.",
    ),
]


# --- Helpers ---


def require_profile(ctx: typer.Context) -> MosaicoProfile:
    """Returns the resolved profile, exiting if it does not point to a server."""
    profile: MosaicoProfile = ctx.obj
    if not profile or not profile.host:
        error_console.print(
            "[bold red]Error:[/bold red] No default profile found. "
            "Set a default profile or specify one using --profile."
        )
        raise SystemExit(1)
    return profile


def connection_kwargs(profile: Optional[MosaicoProfile]) -> Dict[str, Any]:
    """
    Maps the resolved profile onto the connection fields of the bridge configs.

    With no usable profile, the connection fields are left out so the config keeps its
    defaults: only valid for runs that never connect (an injection dry run).
    """
    if not profile or not profile.host:
        return {}
    return {
        "host": profile.host,
        "port": profile.port,
        "mosaico_api_key": profile.api_key or None,
        "tls_cert_path": profile.tls_cert_path,
        "enable_tls": profile.enable_tls,
    }


def parse_json_option(value: Optional[str], option_name: str) -> dict:
    """
    Parses an option holding either a raw JSON object or the path to a JSON file.

    Raises:
        typer.BadParameter: If the value is neither, or does not hold a JSON object.
    """
    if not value:
        return {}

    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        path = Path(value)
        if not path.is_file():
            raise typer.BadParameter(
                "neither a valid JSON string nor the path to a file.",
                param_hint=option_name,
            )
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise typer.BadParameter(
                f"cannot read JSON from '{path}': {e}", param_hint=option_name
            )

    if not isinstance(data, dict):
        raise typer.BadParameter("expected a JSON object.", param_hint=option_name)
    return data


def run_injector(injector) -> None:
    """Runs an injector, mapping its outcome onto the process exit code."""
    from mosaicolabs.bridges.base_injector import InjectionStatus

    try:
        status = injector.run()
    except KeyboardInterrupt:
        sys.exit(EXIT_INTERRUPTED)
    except Exception:
        # Already logged with a full traceback inside run()
        raise typer.Exit(code=1)

    if status is InjectionStatus.CANCELLED:
        sys.exit(EXIT_INTERRUPTED)


def run_extractor(extractor) -> None:
    """Runs an extractor, turning expected failures into a clean CLI error."""
    try:
        extractor.run()
    except (FileExistsError, ValueError) as e:
        error_console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)
