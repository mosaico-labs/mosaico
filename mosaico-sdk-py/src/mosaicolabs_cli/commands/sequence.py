import sys
from typing import List, Optional

import typer
from rich.table import Table
from typing_extensions import Annotated

from mosaicolabs_cli.utils.config import (
    OutputFormat,
    _flatten_metadata,
    console,
    error_console,
)
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile

app = typer.Typer(no_args_is_help=True)


@app.callback()
def _require_profile(ctx: typer.Context):
    if ctx.resilient_parsing:
        return
    profile: MosaicoProfile = ctx.obj
    if not profile or not profile.host:
        error_console.print(
            "[bold red]Error:[/bold red] No default profile found. "
            "Set a default profile or specify one using --profile."
        )
        raise SystemExit(1)


@app.command(name="ls")
def list_sequences(
    ctx: typer.Context,
    locator: Optional[str] = typer.Option(
        None,
        "--locator",
        help="Locator search pattern (supports glob-style * wildcards).",
    ),
    created_after: Optional[int] = typer.Option(
        None,
        "--created-after",
        help="Filter sequences created after this timestamp (epoch ns).",
    ),
    created_before: Optional[int] = typer.Option(
        None,
        "--created-before",
        help="Filter sequences created before this timestamp (epoch ns).",
    ),
    metadata: Optional[List[str]] = typer.Option(
        None,
        "--metadata",
        "-m",
        help="Filter by user metadata KEY=VALUE. Can be repeated for multiple logical AND conditions.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Maximum number of sequence results to return."
    ),
    output: Optional[OutputFormat] = typer.Option(
        None,
        "--output",
        "-o",
        help="Force output format. Automatically inferred if omitted.",
    ),
) -> None:
    """
    List sequences.
    """
    spinner = console.status("[bold cyan]Querying sequences...")
    if sys.stdout.isatty():
        spinner.start()
    profile: MosaicoProfile = ctx.obj
    from mosaicolabs import MosaicoClient, QuerySequence

    try:
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
                    time_start=created_after if created_after else None,
                    time_end=created_before if created_before else None,
                )

            if metadata:
                for md in metadata:
                    if "=" not in md:
                        error_console.print(
                            f"Invalid metadata filter '{md}'. Expected format KEY=VALUE."
                        )
                        raise typer.Exit(code=1)
                    key, value = md.split("=", 1)
                    query = query.with_user_metadata(key, eq=value)

            results = client.query(query)

            if not results:
                spinner.stop()
                console.print("No sequences found matching the criteria.")
                raise typer.Exit()

            limited_results = results[:limit] if limit else results

            rows = []
            for item in limited_results:
                handler = client.sequence_handler(item.sequence.name)
                if handler is None:
                    continue
                metadata_str = ", ".join(_flatten_metadata(handler.user_metadata))
                rows.append(
                    (
                        item.sequence.name,
                        str(handler.created_timestamp),
                        str(handler._timestamp_ns_min),
                        str(handler._timestamp_ns_max),
                        metadata_str,
                    )
                )
    finally:
        spinner.stop()

    if not output:
        output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

    if output == OutputFormat.TABLE:
        table = Table(
            title="Mosaico Sequence ls Results",
            title_style="bold magenta",
            header_style="bold cyan",
            box=None,
            padding=(1, 2),
        )

        table.add_column("Locator", style="bold white", width=25)
        table.add_column("Created", style="green", width=25)
        table.add_column("Min Timestamp", style="green", width=25)
        table.add_column("Max Timestamp", style="green", width=25)
        table.add_column("User Metadata", style="green", width=35)

        for row in rows:
            table.add_row(*row)

        console.print(table)

    else:
        for name, created, ts_min, ts_max, _ in rows:
            print(f"{name},{ts_min},{ts_max}")


@app.command(name="stat")
def stat_sequence(
    ctx: typer.Context,
    handlers: Annotated[
        Optional[List[str]],
        typer.Argument(
            help="Sequence handler(s) as 'name,tsmin,tsmax'. Omit to read from stdin."
        ),
    ] = None,
) -> None:
    """
    Show detailed information about one or more sequences.

    Each handler is a comma-separated string: name,timestamp_ns_min,timestamp_ns_max
    """
    spinner = console.status("[bold cyan]Fetching sequences...")
    if sys.stdout.isatty():
        spinner.start()
    from mosaicolabs import MosaicoClient

    profile: MosaicoProfile = ctx.obj
    try:
        # Resolve sequence handlers: args > stdin
        if not handlers:
            if sys.stdin.isatty():
                error_console.print(
                    "[bold red]Error:[/bold red] No sequence handlers provided. Pass as arguments or pipe via stdin."
                )
                raise typer.Exit(code=1)
            handlers = [line.strip() for line in sys.stdin if line.strip()]

        if not handlers:
            error_console.print(
                "[bold red]Error:[/bold red] No sequence handlers received."
            )
            raise typer.Exit(code=1)

        with MosaicoClient.connect(
            host=profile.host,
            port=profile.port,
            api_key=profile.api_key,
            tls_cert_path=profile.tls_cert_path,
            enable_tls=profile.enable_tls,
        ) as client:
            output_blocks = []
            for raw in handlers:
                parts = raw.split(",")
                if len(parts) != 3:
                    error_console.print(
                        f"[bold red]Error:[/bold red] Invalid handler format '{raw}'. Expected 'name,tsmin,tsmax'."
                    )
                    continue

                name = parts[0].strip()
                handler = client.sequence_handler(name)
                if not handler:
                    error_console.print(
                        f"[bold red]Error:[/bold red] Sequence '{name}' not found."
                    )
                    continue

                block = []
                block.append(f"[bold cyan]Sequence:[/bold cyan] {handler.name}")
                block.append(f"  Created:    {handler.created_timestamp}")
                block.append(f"  Size:       {handler.total_size_bytes} bytes")
                block.append(f"  Topics:     {handler.topics}")
                block.append(
                    f"  Time range: {handler.timestamp_ns_min} - {handler.timestamp_ns_max}"
                )
                metadata_str = ", ".join(_flatten_metadata(handler.user_metadata))
                if metadata_str:
                    block.append(f"  Metadata:   {metadata_str}")
                output_blocks.append(block)
    finally:
        spinner.stop()

    for block in output_blocks:
        for line in block:
            console.print(line)
        console.print()
