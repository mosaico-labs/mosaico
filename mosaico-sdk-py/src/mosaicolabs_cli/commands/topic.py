import json
import sys
from typing import List, Optional

import typer
from rich.table import Table
from typing_extensions import Annotated

from mosaicolabs import MosaicoClient, QueryResponse, QueryTopic
from mosaicolabs_cli.utils.config import (
    OutputFormat,
    _flatten_metadata,
    console,
    error_console,
)
from mosaicolabs_cli.utils.MosaicoProfile import MosaicoProfile

app = typer.Typer(no_args_is_help=True)

@app.command(name="ls")
def list_topics(
    ctx: typer.Context,
    locator: Optional[str] = typer.Option(None, "--locator", help="Locator search pattern (supports glob-style * wildcards)."),
    metadata: Optional[List[str]] = typer.Option(None, "--metadata", "-m", help="Filter by user metadata KEY=VALUE. Logical AND will be applied if multiple --metadata options are provided."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of topic results to return."),
    output: Optional[OutputFormat] = typer.Option(None, "--output", "-o", help="Force output format. Automatically inferred if omitted.")
):
    """
    List topics.
    """
    profile: MosaicoProfile = ctx.obj

    with MosaicoClient.connect(
        host=profile.host,
        port=profile.port,
        api_key=profile.api_key,
        tls_cert_path=profile.tls_cert_path,
        enable_tls=profile.enable_tls,
    ) as client:
        query = QueryTopic().with_name_match(locator if locator else ".*")

        if metadata:
            for md in metadata:
                if "=" not in md:
                    error_console.print(f"Invalid metadata filter '{md}'. Expected format KEY=VALUE.")
                    raise typer.Exit(code=1)
                key, value = md.split("=", 1)
                query = query.with_user_metadata(key, eq=value)
                
        with console.status("[bold cyan]Querying topics..."):
            results = client.query(query)

        if not results:
            console.print("No topics found matching the criteria.")
            raise typer.Exit()
        
        if limit:
            results: QueryResponse = results[:limit]

        if not output:
            output = OutputFormat.TABLE if sys.stdout.isatty() else OutputFormat.CSV

        if output == OutputFormat.TABLE:
            table = Table(
                title="Mosaico Topic ls Results",
                title_style="bold magenta",
                header_style="bold cyan",
                box=None,
                padding=(0, 2)
            )
            table.add_column("Locator", style="white bold", no_wrap=True)
            table.add_column("Start Time", justify="right", style="yellow")
            table.add_column("End Time", justify="right", style="blue")

            for item in results:
                for topic in item.topics:
                    handler = client.topic_handler(item.sequence.name, topic.name)
                    if handler:
                        table.add_row(
                            f"{item.sequence.name}{topic.name}",
                            str(handler.timestamp_ns_min),
                            str(handler.timestamp_ns_max)
                        )
                    else:
                        error_console.print(f"[bold red]Error:[/bold red] Topic '{topic.name}' in sequence '{item.sequence.name}' not found.")

            console.print(table)

        if output == OutputFormat.CSV:
            for item in results:
                for topic in item.topics:
                    handler = client.topic_handler(item.sequence.name, topic.name)
                    if handler:
                        console.print(f"{item.sequence.name}{topic.name},{handler.timestamp_ns_min},{handler.timestamp_ns_max}")
                    else:
                        error_console.print(f"[bold red]Error:[/bold red] Topic '{topic.name}' in sequence '{item.sequence.name}' not found.")


@app.command(name="stat")
def stat_topic(
    ctx: typer.Context,
    handlers: Annotated[
        Optional[List[str]],
        typer.Argument(help="Topic handler(s) as 'name,tsmin,tsmax'. Omit to read from stdin."),
    ] = None,
) -> None:
    """
    Show detailed information about one or more topics.

    Each handler is a comma-separated string: name,timestamp_ns_min,timestamp_ns_max
    """
    profile: MosaicoProfile = ctx.obj

    # Resolve topic handlers: args > stdin
    if not handlers:
        if sys.stdin.isatty():
            error_console.print("[bold red]Error:[/bold red] No topic handlers provided. Pass as arguments or pipe via stdin.")
            raise typer.Exit(code=1)
        handlers = [line.strip() for line in sys.stdin if line.strip()]

    if not handlers:
        error_console.print("[bold red]Error:[/bold red] No topic handlers received.")
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

            sequence_name = None
            topic_name = None
            # split name into sequence and topic if possible
            if "/" in name:
                sequence_name, topic_name = name.split("/", 1)

            with console.status(f"[bold cyan]Fetching topic '{name}'..."):
                handler = client.topic_handler(sequence_name, topic_name)

            if not handler:
                error_console.print(f"[bold red]Error:[/bold red] Topic '{name}' not found.")
                continue

            console.print(f"[bold cyan]Topic:[/bold cyan] {name}")
            console.print(f"  Created:    {handler.created_timestamp}")
            console.print(f"  Size:       {handler.total_size_bytes} bytes")
            console.print(f"  Ontology:   {handler.ontology_tag}")
            console.print(f"  Time range: {handler.timestamp_ns_min} - {handler.timestamp_ns_max}")
            metadata_str = ", ".join(_flatten_metadata(handler.user_metadata))
            if metadata_str:
                console.print(f"  Metadata:   {metadata_str}")
            console.print()

@app.command(name="mcat")
def mcat_topic(
    ctx: typer.Context,
    handlers: Annotated[
        Optional[List[str]],
        typer.Argument(help="Topic handler(s) as 'name,tsmin,tsmax'. Omit to read from stdin."),
    ] = None,
    fromIndex: Optional[int] = typer.Option(None, "--from-index", help="Starting index of messages to output (0-based)."),
    count: Optional[int] = typer.Option(None, "--count", help="Number of messages to output. If omitted, outputs all messages from the starting index.")
) -> None:
    """
    Output raw topic data to stdout as JSON Lines.

    When multiple handlers share the same topic locator but different time ranges,
    the ranges are merged (min start, max end) into a single stream.
    """
    profile: MosaicoProfile = ctx.obj

    # Resolve topic handlers: args > stdin
    if not handlers:
        if sys.stdin.isatty():
            error_console.print("[bold red]Error:[/bold red] No topic handlers provided. Pass as arguments or pipe via stdin.")
            raise typer.Exit(code=1)
        handlers = [line.strip() for line in sys.stdin if line.strip()]

    if not handlers:
        error_console.print("[bold red]Error:[/bold red] No topic handlers received.")
        raise typer.Exit(code=1)

    # Parse and merge handlers by locator (min ts_start, max ts_end)
    merged: dict[str, list[int]] = {}
    for raw in handlers:
        parts = raw.split(",")
        if len(parts) != 3:
            error_console.print(f"[bold red]Error:[/bold red] Invalid handler format '{raw}'. Expected 'name,tsmin,tsmax'.")
            continue
        name, ts_min_str, ts_max_str = parts[0].strip(), parts[1].strip(), parts[2].strip()
        ts_min, ts_max = int(ts_min_str), int(ts_max_str)
        if name in merged:
            merged[name][0] = min(merged[name][0], ts_min)
            merged[name][1] = max(merged[name][1], ts_max)
        else:
            merged[name] = [ts_min, ts_max]

    with MosaicoClient.connect(
        host=profile.host,
        port=profile.port,
        api_key=profile.api_key,
        tls_cert_path=profile.tls_cert_path,
        enable_tls=profile.enable_tls,
    ) as client:
        for name, (ts_min, ts_max) in merged.items():
            sequence_name = None
            topic_name = None
            if "/" in name:
                sequence_name, topic_name = name.split("/", 1)

            handler = client.topic_handler(sequence_name, topic_name)
            if not handler:
                error_console.print(f"[bold red]Error:[/bold red] Topic '{name}' not found.")
                continue

            streamer = handler.get_data_streamer(ts_min, ts_max)

            msg_index = 0
            start = fromIndex or 0
            emitted = 0

            for message in streamer:
                if msg_index < start:
                    msg_index += 1
                    continue

                payload = json.loads(message.data.model_dump_json())
                payload["_timestamp"] = message.timestamp_ns
                payload["_topic"] = topic_name or name
                payload["_ontology"] = message.ontology_tag()
                print(json.dumps(payload))
                sys.stdout.flush()

                emitted += 1
                msg_index += 1

                if count is not None and emitted >= count:
                    break