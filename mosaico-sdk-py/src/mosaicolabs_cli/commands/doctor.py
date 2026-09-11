import json
import socket
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.table import Table

from mosaicolabs_cli.utils.config import (
    OutputFormat,
    config_permissions,
    console,
    get_config_path,
)
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile


def _check(name: str, status: str, message: str) -> dict[str, str]:
    return {"name": name, "status": status, "message": message}


def _overall_status(checks: list[dict[str, str]]) -> str:
    statuses = {check["status"] for check in checks}
    if "error" in statuses:
        return "error"
    if "warning" in statuses:
        return "warning"
    return "ok"


def _render_table(checks: list[dict[str, str]], overall: str) -> None:
    table = Table(
        title=f"Mosaico diagnostics: {overall}",
        header_style="bold cyan",
        box=None,
    )
    table.add_column("Check", style="bold white")
    table.add_column("Status")
    table.add_column("Details")

    styles = {"ok": "green", "warning": "yellow", "error": "red"}
    for check in checks:
        status = check["status"]
        table.add_row(
            check["name"],
            f"[{styles[status]}]{status}[/{styles[status]}]",
            check["message"],
        )
    console.print(table)


def doctor(
    ctx: typer.Context,
    network: bool = typer.Option(
        True,
        "--network/--no-network",
        help="Check DNS resolution and TCP connectivity.",
    ),
    timeout: float = typer.Option(
        2.0,
        "--timeout",
        min=0.1,
        help="Network connection timeout in seconds.",
    ),
    output: Optional[OutputFormat] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output diagnostics as table, CSV, JSON, or JSON Lines.",
    ),
) -> None:
    """Diagnose local configuration and connectivity without exposing credentials."""
    profile: MosaicoProfile = ctx.obj
    checks: list[dict[str, str]] = []
    config_path = get_config_path()

    if config_path.exists():
        permissions = config_permissions(config_path)
        if permissions is not None and permissions & 0o077:
            checks.append(
                _check(
                    "config_permissions",
                    "warning",
                    f"{config_path} is mode {permissions:04o}; use 0600.",
                )
            )
        else:
            checks.append(
                _check("config_permissions", "ok", f"{config_path} is restricted.")
            )
    else:
        checks.append(
            _check(
                "configuration",
                "warning",
                f"No configuration file found at {config_path}; environment settings may still be used.",
            )
        )

    if profile and profile.host:
        checks.append(
            _check(
                "profile",
                "ok",
                f"Resolved {profile.host}:{profile.port} (TLS {'on' if profile.enable_tls else 'off'}).",
            )
        )
    else:
        checks.append(
            _check(
                "profile",
                "error",
                "No host is configured. Add a profile or set MOSAICO_DAEMON_URL.",
            )
        )

    if profile and profile.enable_tls and profile.cert_path:
        cert_path = Path(profile.cert_path)
        if cert_path.is_file():
            checks.append(_check("tls_certificate", "ok", str(cert_path)))
        else:
            checks.append(
                _check(
                    "tls_certificate",
                    "error",
                    f"Certificate file does not exist: {cert_path}",
                )
            )

    can_check_network = network and profile and bool(profile.host)
    if can_check_network:
        try:
            socket.getaddrinfo(profile.host, profile.port, type=socket.SOCK_STREAM)
            checks.append(_check("dns", "ok", f"Resolved {profile.host}."))
        except OSError as exc:
            checks.append(_check("dns", "error", str(exc)))

        if checks[-1]["name"] == "dns" and checks[-1]["status"] == "ok":
            try:
                with socket.create_connection(
                    (profile.host, profile.port), timeout=timeout
                ):
                    pass
                checks.append(
                    _check(
                        "tcp",
                        "ok",
                        f"Connected to {profile.host}:{profile.port}.",
                    )
                )
            except OSError as exc:
                checks.append(_check("tcp", "error", str(exc)))

    overall = _overall_status(checks)
    selected_output = output or (
        OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.JSON
    )
    payload = {
        "schema_version": 1,
        "status": overall,
        "profile": {
            "name": profile.name if profile else "",
            "host": profile.host if profile else "",
            "port": profile.port if profile else None,
            "tls": profile.enable_tls if profile else False,
            "api_key_configured": bool(profile and profile.api_key),
        },
        "checks": checks,
    }

    if selected_output == OutputFormat.TABLE:
        _render_table(checks, overall)
    elif selected_output == OutputFormat.CSV:
        for check in checks:
            print(f"{check['name']},{check['status']},{json.dumps(check['message'])}")
    elif selected_output == OutputFormat.JSONL:
        for check in checks:
            print(json.dumps(check, sort_keys=True))
    else:
        print(json.dumps(payload, sort_keys=True))

    if overall == "error":
        raise typer.Exit(code=1)
