import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click
import typer
from rich.table import Table
from typer.core import TyperGroup

from mosaicolabs_cli.utils.config import OutputFormat, console, error_console

app = typer.Typer(no_args_is_help=True)


class MosaicoRouter(TyperGroup):
    """Custom TyperGroup to dynamically discover and execute external plugins."""

    def get_command(self, ctx, cmd_name: str):
        core_cmd = super().get_command(ctx, cmd_name)
        if core_cmd is not None:
            return core_cmd

        ext_binary = f"mosaico-{cmd_name}"
        if shutil.which(ext_binary):

            @click.command(
                name=cmd_name,
                context_settings=dict(
                    ignore_unknown_options=True,
                    allow_extra_args=True,
                    allow_interspersed_args=False,
                ),
                add_help_option=False,
            )
            @click.pass_context
            def dynamic_extension(sub_ctx):
                args = sub_ctx.args
                env = os.environ.copy()
                profile = sub_ctx.find_root().obj
                if profile is not None:
                    env.update(profile.to_env())
                try:
                    result = subprocess.run([ext_binary] + args, env=env)
                    sys.exit(result.returncode)
                except Exception as e:
                    error_console.print(
                        f"Error executing extension '{ext_binary}': {e}"
                    )
                    sys.exit(1)

            return dynamic_extension

        return None


def discover_extensions() -> dict[str, str]:
    """
    Scans the system PATH to discover all executable files prefixed with 'mosaico-'.
    Returns a dictionary mapping the extension name to its absolute binary path.
    """
    extensions = {}

    path_env = os.getenv("PATH", "")

    path_separator = ";" if os.name == "nt" else ":"

    for path_dir in path_env.split(path_separator):
        if not path_dir:
            continue

        dir_path = Path(path_dir)

        if not dir_path.is_dir():
            continue

        try:
            for item in dir_path.iterdir():
                if item.name.startswith("mosaico-") and item.is_file():
                    if os.access(item, os.X_OK):
                        ext_name = item.name.replace("mosaico-", "", 1)

                        if ext_name not in extensions:
                            extensions[ext_name] = str(item.absolute())
        except (PermissionError, FileNotFoundError):
            continue

    return dict(sorted(extensions.items()))


@app.command(name="ls")
def list_extensions(
    output: Optional[OutputFormat] = typer.Option(
        None,
        "--output",
        help="Force the output format. Automatically inferred if omitted.",
    ),
):
    """
    List all installed external extensions discovered in the system $PATH.
    """

    extensions = discover_extensions()

    if not extensions:
        console.print(
            "[yellow]No external extensions discovered in your $PATH.[/yellow]"
        )
        console.print(
            "To add an extension, place an executable named 'mosaico-<command>' in your PATH."
        )
        return

    if output is None:
        output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

    if output == OutputFormat.TABLE:
        table = Table(
            title="Mosaico Installed Extensions",
            title_style="bold magenta",
            header_style="bold cyan",
            box=None,
            padding=(0, 2),
        )
        table.add_column("Extension", style="bold white", width=15)
        table.add_column("Binary Name", style="yellow", width=20)
        table.add_column("Absolute Path", style="green", width=50)

        for name, binary_path in extensions.items():
            binary_name = f"mosaico-{name}"
            table.add_row(name, binary_name, binary_path)

        console.print(table)

    elif output == OutputFormat.CSV:
        for name, binary_path in extensions.items():
            print(f"{name},mosaico-{name},{binary_path}")
