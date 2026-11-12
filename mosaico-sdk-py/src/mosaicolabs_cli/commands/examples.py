"""
`mosaico examples`: runs the official SDK examples against the server of the active profile.
"""

import csv
import logging
import sys
from dataclasses import dataclass
from importlib import import_module
from typing import Callable, Optional

import typer
from rich.table import Table

from mosaicolabs_cli.utils.bridge import (
    EXIT_INTERRUPTED,
    LogLevel,
    LogLevelOpt,
    require_profile,
)
from mosaicolabs_cli.utils.config import OutputFormat, console, error_console
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile


@dataclass(frozen=True)
class Example:
    module: str
    help: str


# The deprecated `mosaicolabs.examples` script keeps its own copy of these names
# (EXAMPLES_MAP), so the CLI never has to import the SDK just to build its help.
EXAMPLES = {
    "mcap_injection": Example(
        "mosaicolabs.examples.bridges.mcap.injection",
        "Download a sample MCAP file and inject it into a new sequence.",
    ),
    "reconstruct_mcaps": Example(
        "mosaicolabs.examples.bridges.mcap.reconstruct",
        "Rebuild MCAP files from the sequences written by mcap_injection.",
    ),
    "ros_injection": Example(
        "mosaicolabs.examples.bridges.ros.injection",
        "Download a sample ROS bag and inject it into a new sequence.",
    ),
    "reconstruct_rosbags": Example(
        "mosaicolabs.examples.bridges.ros.reconstruct",
        "Rebuild ROS bags from the sequences written by ros_injection.",
    ),
    "data_inspection": Example(
        "mosaicolabs.examples.data_inspection",
        "List the sequences in the catalog and inspect their topics.",
    ),
    "query_catalogs": Example(
        "mosaicolabs.examples.query_catalogs",
        "Query the catalog for topics and sequences.",
    ),
    "mujoco_vis": Example(
        "mosaicolabs.examples.mujoco_vis",
        "Retrieve a UR robot sequence, plot its joints and replay it in MuJoCo.",
    ),
}

app = typer.Typer(no_args_is_help=True)


@app.command(name="ls")
def list_examples(
    output: Optional[OutputFormat] = typer.Option(
        None,
        "--output",
        help="Force the output format. Automatically inferred if omitted.",
    ),
) -> None:
    """
    List the available examples.
    """
    if output is None:
        output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

    if output == OutputFormat.CSV:
        # Quoted properly: descriptions may contain commas
        writer = csv.writer(sys.stdout, lineterminator="\n")
        for name, example in EXAMPLES.items():
            writer.writerow([name, example.help])
        return

    table = Table(
        title="Mosaico SDK Examples",
        title_style="bold magenta",
        header_style="bold cyan",
        box=None,
        padding=(0, 2),
    )
    table.add_column("Example", style="bold white")
    table.add_column("Description")
    for name, example in EXAMPLES.items():
        table.add_row(name, example.help)
    console.print(table)


def _configure(profile: MosaicoProfile, log_level: LogLevel) -> None:
    """
    Writes the connection into the examples config.

    The examples read it at import time, so this must run before the example is imported.
    """
    from mosaicolabs.examples import config

    config.MOSAICO_HOST = profile.host
    config.MOSAICO_PORT = profile.port
    config.ENABLE_TLS = profile.enable_tls
    config.API_KEY = profile.api_key or None
    config.LOG_LEVEL = log_level.value


def _load_entry_point(name: str) -> Callable[[], None]:
    module_path = EXAMPLES[name].module
    entry_point = getattr(import_module(module_path), "main", None)
    if entry_point is None:
        error_console.print(
            f"[bold red]Error:[/bold red] No 'main' function found in {module_path}"
        )
        raise typer.Exit(code=1)
    return entry_point


def run_example(ctx: typer.Context, name: str, log_level: LogLevel) -> None:
    profile = require_profile(ctx)
    logging.basicConfig(level=log_level.value)
    _configure(profile, log_level)

    target = f"{profile.host}:{profile.port}" + (" (TLS)" if profile.enable_tls else "")
    console.print(f"[bold cyan]Launching example:[/bold cyan] {name}")
    console.print(f"Target host: {target}\n")
    if profile.tls_cert_path:
        error_console.print(
            "[yellow]The examples don't support a custom CA certificate yet; "
            "the profile's certificate path is ignored.[/yellow]"
        )

    # The import runs inside the try: importing an example can fail too (a missing
    # optional dependency, an error at module level) or be interrupted, since the SDK
    # imports are slow. typer.Exit from a missing `main` is not an Exception, so it passes.
    try:
        entry_point = _load_entry_point(name)
        entry_point()
    except KeyboardInterrupt:
        raise typer.Exit(code=EXIT_INTERRUPTED)
    except Exception as e:
        error_console.print(f"[bold red]Execution failed:[/bold red] {e}")
        if log_level is LogLevel.DEBUG:
            logging.exception("Detailed stack trace:")
        else:
            error_console.print("Run with --log DEBUG for the full traceback.")
        raise typer.Exit(code=1)


def _make_command(name: str) -> Callable[..., None]:
    def command(ctx: typer.Context, log_level: LogLevelOpt = LogLevel.INFO):
        run_example(ctx, name, log_level)

    command.__doc__ = EXAMPLES[name].help
    return command


for _name in EXAMPLES:
    app.command(name=_name)(_make_command(_name))
