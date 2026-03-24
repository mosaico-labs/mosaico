"""
This example demonstrates how to list the sequences in the MosaicoDB catalog.
This example can be run after running the ingestion example (examples/ros_injection/main.py).

Ingest data via:
```bash
cd mosaico-sdk-py/src/examples && poetry run python ros_injection/main.py
```

Then, list sequences via:
```bash
poetry run python data_inspection.py
```
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Mosaico SDK Imports
from mosaicolabs import MosaicoClient

from .config import MOSAICO_HOST, MOSAICO_PORT

# Initialize Rich Console for beautiful terminal output
console = Console()


def main():
    # --- PHASE 3: Verification & Retrieval ---
    # Connect to the client using a context manager to ensure resource cleanup.

    with MosaicoClient.connect(host=MOSAICO_HOST, port=MOSAICO_PORT) as client:
        seq_list = client.list_sequences()
        for sequence_name in seq_list:
            console.print(
                Panel(f"[bold green]Info for sequence '{sequence_name}'[/bold green]")
            )

            # Retrieve a SequenceHandler for the newly ingested data
            shandler = client.sequence_handler(sequence_name)
            if not shandler:
                console.print(
                    "[bold red]Error:[/bold red] Could not retrieve sequence handler."
                )
                return
            # Display sequence diagnostics
            size_mb = shandler.total_size_bytes / (1024 * 1024)

            console.print(f"• [bold]Remote Size:[/bold]   {size_mb:.2f} MB")
            console.print(f"• [bold]Created At:[/bold]    {shandler.created_timestamp}")
            console.print(
                f"• [bold]Timestamps Range Found:[/bold]  {shandler.timestamp_ns_min} - {shandler.timestamp_ns_max}"
            )

            # Create table
            table = Table(title="Topics List")
            # Add columns
            table.add_column("Name", style="magenta")
            table.add_column("Ontology Tag", style="green")
            table.add_column("Remote Size (MB)", style="green")
            table.add_column("Duration (s)", style="green")

            # Add rows

            console.print(f"• [bold]Topics Found:[/bold]  {len(shandler.topics)}")

            for topic in shandler.topics:
                try:
                    thandler = shandler.get_topic_handler(topic)
                except Exception:
                    console.print(
                        f"[bold red]Error:[/bold red] Could not retrieve topic handler {topic}"
                    )
                    return

                duration_sec = 0
                if thandler.timestamp_ns_min and thandler.timestamp_ns_max:
                    duration_sec = (
                        thandler.timestamp_ns_max - thandler.timestamp_ns_min
                    ) / 1e9

                size_mb = thandler.total_size_bytes / (1024 * 1024)
                table.add_row(
                    topic,
                    thandler.ontology_tag,
                    f"{size_mb:.2f}",
                    f"{duration_sec:0.2f}",
                )
            console.print(table)


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    main()
