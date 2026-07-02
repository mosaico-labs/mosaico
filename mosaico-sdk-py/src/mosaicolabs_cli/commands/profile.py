import sys
from typing import Optional

import typer
from rich.table import Table

from mosaicolabs_cli.utils.config import (
    OutputFormat,
    console,
    error_console,
    get_config_path,
    load_config,
    serialize_to_toml,
)
from mosaicolabs_cli.utils.env import DEFAULT_MOSAICO_PORT
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile

app = typer.Typer(no_args_is_help=True)


@app.command(name="add")
def add_profile(
    name: str = typer.Argument(..., help="Unique name for the new connection profile."),
    is_default: bool = typer.Option(
        False, "--default", help="Set this profile as the default fallback option."
    ),
    interactive: bool = typer.Option(
        True, "--interactive/--no-interactive", help="Toggle interactive prompt inputs."
    ),
    host: Optional[str] = typer.Option(
        None, "--host", help="Mosaico server host name or IP address."
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        help="Mosaico server port. Leave empty for default when running interactively.",
    ),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Authentication API key."
    ),
    tls: bool = typer.Option(
        False, "--tls/--no-tls", help="Enable TLS for this connection."
    ),
    cert_path: Optional[str] = typer.Option(
        None, "--cert-path", help="Path to custom TLS CA certificate (optional)."
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force overwrite of an existing profile in non-interactive mode.",
    ),
):
    """
    Add or update a connection profile inside the configuration file.
    """

    config_path = get_config_path()
    config_data = load_config(config_path)

    if interactive:
        if not host:
            host = typer.prompt("Mosaico Server Host", type=str)
        if port is None:
            port_input = typer.prompt(
                f"Mosaico Server Port (leave empty for {DEFAULT_MOSAICO_PORT})",
                default=str(DEFAULT_MOSAICO_PORT),
                type=int,
            )
            port = int(port_input)
        if not api_key:
            api_key = typer.prompt("API Key", hide_input=True, default="", type=str)
        if not tls:
            tls = typer.confirm("Enable TLS?", default=False)
        if tls and not cert_path:
            cert_path = typer.prompt(
                "TLS CA Certificate Path (optional, press Enter to skip)",
                default="",
                type=str,
            )
    else:
        if not host:
            error_console.print(
                "[bold red]Error:[/bold red] `--host` is strictly required in non-interactive mode."
            )
            raise typer.Exit(code=1)

    if name in config_data and isinstance(config_data[name], dict):
        if interactive:
            overwrite = typer.confirm(f"Profile '{name}' already exists. Overwrite?")
            if not overwrite:
                console.print("[yellow]Operation aborted.[/yellow]")
                return
        elif not force:
            error_console.print(
                f"[bold red]Error:[/bold red] Profile [yellow]'{name}'[/yellow] already exists. "
                f"Use `--force` to overwrite."
            )
            raise typer.Exit(code=1)

    was_already_default = False
    if name in config_data and isinstance(config_data[name], dict):
        was_already_default = config_data[name].get("default", False)

    should_be_default = is_default or was_already_default or not config_data

    if should_be_default:
        for profile_name, profile_content in config_data.items():
            if profile_name != name and isinstance(profile_content, dict):
                profile_content["default"] = False

    if host:
        try:
            host, port = MosaicoProfile._normalize_host_port(host, port)
        except ValueError as e:
            error_console.print(f"[bold red]Error:[/bold red] {e}")
            raise typer.Exit(code=1)

    config_data[name] = {
        "host": host,
        "port": port,
        "api_key": api_key.strip() if api_key else "",
        "tls": tls,
        "cert_path": cert_path.strip() if cert_path else "",
        "default": should_be_default,
    }

    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)

        toml_string = serialize_to_toml(config_data)
        config_path.write_text(toml_string, encoding="utf-8")

        console.print(
            f"[bold green]Success:[/bold green] Profile [yellow]'{name}'[/yellow] saved to {config_path}"
        )
        if should_be_default:
            console.print(
                f"Profile [yellow]'{name}'[/yellow] has been configured as your active default."
            )

    except Exception as e:
        error_console.print(
            f"[bold red]Error:[/bold red] Could not write configuration file: {e}"
        )
        raise typer.Exit(code=1)


@app.command(name="remove")
def remove_profile(
    name: str = typer.Argument(..., help="The name of the profile you want to remove."),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmation prompt before deleting."
    ),
):
    """
    Remove a connection profile from the configuration file.
    """
    config_path = get_config_path()
    config_data = load_config(config_path)

    if name not in config_data:
        error_console.print(
            f"[bold red]Error:[/bold red] Profile [yellow]'{name}'[/yellow] does not exist. "
            f"Use `mosaico profile ls` to see available profiles."
        )
        raise typer.Exit(code=1)

    if not force:
        confirm = typer.confirm(f"Are you sure you want to delete profile '{name}'?")
        if not confirm:
            console.print("[yellow]Operation aborted.[/yellow]")
            return

    was_default = False
    if isinstance(config_data[name], dict):
        was_default = config_data[name].get("default", False)

    del config_data[name]

    auto_promoted_profile = None
    if was_default and config_data:
        first_remaining_name = next(iter(config_data))
        if isinstance(config_data[first_remaining_name], dict):
            config_data[first_remaining_name]["default"] = True
            auto_promoted_profile = first_remaining_name

    try:
        toml_string = serialize_to_toml(config_data)
        config_path.write_text(toml_string, encoding="utf-8")

        console.print(
            f"[bold green]Success:[/bold green] Profile [yellow]'{name}'[/yellow] removed."
        )

        if auto_promoted_profile:
            console.print(
                f"[info]Notice:[/info] Deleted profile was the default. "
                f"Profile [yellow]'{auto_promoted_profile}'[/yellow] has been automatically set as the new default."
            )

    except Exception as e:
        error_console.print(
            f"[bold red]Error:[/bold red] Could not update configuration file: {e}"
        )
        raise typer.Exit(code=1)


@app.command(name="ls")
def list_profiles(
    output: Optional[OutputFormat] = typer.Option(
        None, "--output", help="Force output format. If omitted, default to table."
    ),
):
    """
    List all configured Mosaico platform profiles from the config file.
    """
    config_path = get_config_path()
    config_data = load_config(config_path)

    if not config_data:
        console.print(
            "[yellow]No profiles configured yet.[/yellow] Use `mosaico profile add` to create one."
        )
        return

    if not output:
        output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

    if output == OutputFormat.TABLE:
        table = Table(
            title="Mosaico Connection Profiles",
            title_style="bold magenta",
            header_style="bold cyan",
            box=None,
            padding=(0, 2),
        )

        table.add_column("Profile Name", style="bold white", width=15)
        table.add_column("Host", style="green", width=35)
        table.add_column("Port", style="green", width=10)
        table.add_column("Default", justify="center", width=10)

        for name, content in config_data.items():
            if isinstance(content, dict):
                p = MosaicoProfile.from_dict(
                    content, name=name, is_default=content.get("default", False)
                )
                default_marker = "[bold green]✓[/bold green]" if p.is_default else ""
                table.add_row(p.name, p.host, str(p.port), default_marker)

        console.print(table)

    elif output == OutputFormat.CSV:
        for name, content in config_data.items():
            if isinstance(content, dict):
                p = MosaicoProfile.from_dict(
                    content, name=name, is_default=content.get("default", False)
                )
                console.print(p.to_csv())

    else:
        error_console.print(
            f"[bold red]Error:[/bold red] Unsupported output format: '{output}'. Use 'table' or 'csv'."
        )
        raise typer.Exit(code=1)


@app.command(name="default")
def set_default_profile(
    name: str = typer.Argument(
        ..., help="The name of the profile you want to set as default."
    ),
):
    """
    Switch the active default profile to the one specified.
    """
    config_path = get_config_path()
    config_data = load_config(config_path)

    if name not in config_data or not isinstance(config_data[name], dict):
        error_console.print(
            f"[bold red]Error:[/bold red] Profile [yellow]'{name}'[/yellow] does not exist. "
            f"Use `mosaico profile ls` to see available profiles."
        )
        raise typer.Exit(code=1)

    if config_data[name].get("default", False):
        console.print(
            f"Profile [yellow]'{name}'[/yellow] is already set as the default."
        )
        return

    for profile_name, profile_content in config_data.items():
        if isinstance(profile_content, dict):
            if profile_name == name:
                profile_content["default"] = True
            else:
                profile_content["default"] = False

    try:
        toml_string = serialize_to_toml(config_data)
        config_path.write_text(toml_string, encoding="utf-8")
        console.print(
            f"[bold green]Success:[/bold green] Switched active profile to [yellow]'{name}'[/yellow]."
        )

    except Exception as e:
        error_console.print(
            f"[bold red]Error:[/bold red] Could not update configuration file: {e}"
        )
        raise typer.Exit(code=1)
