import sys
from typing import List, Optional

import typer
from rich.table import Table
from typing_extensions import Annotated

from mosaicolabs import MosaicoClient, QueryResponse, QuerySequence, Time
from mosaicolabs_cli.utils.config import (
    OutputFormat,
    _flatten_metadata,
    console,
    error_console,
)
from mosaicolabs_cli.utils.MosaicoProfile import MosaicoProfile

app = typer.Typer(no_args_is_help=True)


@app.command(name="ls")
def list_sequences(
    ctx: typer.Context,
    locator: Optional[str] = typer.Option(None, "--locator", help="Locator search pattern (supports glob-style * wildcards)."),
    created_after: Optional[int] = typer.Option(None, "--created-after", help="Filter sequences created after this timestamp (epoch ns)."),
    created_before: Optional[int] = typer.Option(None, "--created-before", help="Filter sequences created before this timestamp (epoch ns)."),
    metadata: Optional[List[str]] = typer.Option(None, "--metadata", "-m", help="Filter by user metadata KEY=VALUE. Can be repeated for multiple logical AND conditions."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of sequence results to return."),
    output: Optional[OutputFormat] = typer.Option(None, "--output", "-o", help="Force output format. Automatically inferred if omitted.")
) -> None:
    """
    List sequences.
    """
    profile: MosaicoProfile = ctx.obj

    with MosaicoClient.connect(
        host=profile.host,
        port=profile.port,
        api_key=profile.api_key,
        tls_cert_path=profile.tls_cert_path,
        enable_tls=profile.enable_tls,
    ) as client:
        query = QuerySequence().with_name_match(locator if locator else ".*")

        if created_after or created_before: 
            query = query.with_created_timestamp(
                time_start=Time.from_nanoseconds(created_after) if created_after else None,
                time_end=Time.from_nanoseconds(created_before) if created_before else None
            )

        if metadata:
            for md in metadata:
                if "=" not in md:
                    error_console.print(f"Invalid metadata filter '{md}'. Expected format KEY=VALUE.")
                    raise typer.Exit(code=1)
                key, value = md.split("=", 1)
                query = query.with_user_metadata(key, eq=value)

        with console.status("[bold cyan]Querying sequences..."):
            results = client.query(query)

        if not results:
            console.print("No sequences found matching the criteria.")
            raise typer.Exit()

        if limit:
            results: QueryResponse = results[:limit]

        if not output:
            output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

        if output == OutputFormat.TABLE:
            table = Table(
                title="Mosaico Sequence ls Results",
                title_style="bold magenta",
                header_style="bold cyan",
                box=None,
                padding=(1, 2)
            )

            table.add_column("Locator", style="bold white", width=25)
            table.add_column("Min Timestamp", style="green", width=25)
            table.add_column("Max Timestamp", style="green", width=25)
            table.add_column("User Metadata", style="green", width=35)

            for item in results:
                handler = client.sequence_handler(item.sequence.name)
                metadata_str = ", ".join(_flatten_metadata(handler.user_metadata))
                table.add_row(item.sequence.name, str(handler._timestamp_ns_min), str(handler._timestamp_ns_max), metadata_str)

            console.print(table)

        else:
            for item in results:
                handler = client.sequence_handler(item.sequence.name)
                console.print(f"{item.sequence.name},{handler._timestamp_ns_min},{handler._timestamp_ns_max}")
    


@app.command(name="stat")
def stat_sequence(
    ctx: typer.Context,
    handlers: Annotated[
        Optional[List[str]],
        typer.Argument(help="Sequence handler(s) as 'name,tsmin,tsmax'. Omit to read from stdin."),
    ] = None,
) -> None:
    """
    Show detailed information about one or more sequences.

    Each handler is a comma-separated string: name,timestamp_ns_min,timestamp_ns_max
    """
    profile: MosaicoProfile = ctx.obj

    # Resolve sequence handlers: args > stdin
    if not handlers:
        if sys.stdin.isatty():
            error_console.print("[bold red]Error:[/bold red] No sequence handlers provided. Pass as arguments or pipe via stdin.")
            raise typer.Exit(code=1)
        handlers = [line.strip() for line in sys.stdin if line.strip()]

    if not handlers:
        error_console.print("[bold red]Error:[/bold red] No sequence handlers received.")
        raise typer.Exit(code=1)

    with MosaicoClient.connect(
        host=profile.host,
        port=profile.port,
        api_key=profile.api_key,
        tls_cert_path=profile.tls_cert_path,
        enable_tls=profile.enable_tls,
    ) as client:
        for raw in handlers:
            parts = raw.split(",")
            if len(parts) != 3:
                error_console.print(f"[bold red]Error:[/bold red] Invalid handler format '{raw}'. Expected 'name,tsmin,tsmax'.")
                continue

            # name, ts_min_str, ts_max_str = parts[0].strip(), parts[1].strip(), parts[2].strip()
            name = parts[0].strip(), parts[1].strip(), parts[2].strip()
            name = name[0]

            with console.status(f"[bold cyan]Fetching sequence '{name}'..."):
                handler = client.sequence_handler(name)
            if not handler:
                error_console.print(f"[bold red]Error:[/bold red] Sequence '{name}' not found.")
                continue

            console.print(f"[bold cyan]Sequence:[/bold cyan] {handler.name}")
            console.print(f"  Created:    {handler.created_timestamp}")
            console.print(f"  Size:       {handler.total_size_bytes} bytes")
            console.print(f"  Topics:     {handler.topics}")
            console.print(f"  Time range: {handler.timestamp_ns_min} - {handler.timestamp_ns_max}")
            metadata_str = ", ".join(_flatten_metadata(handler.user_metadata))
            if metadata_str:
                console.print(f"  Metadata:   {metadata_str}")
            console.print()