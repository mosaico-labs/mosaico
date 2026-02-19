"""
Mosaico SDK: End-to-End ROS Ingestion & Retrieval Example.

This script demonstrates a complete workflow:
1. Downloading a remote ROS bag (MCAP) using a progress-monitored utility.
    Data are available at: https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1
2. Configuring and executing a high-performance ROS Bridge injection.
    The data are ingested using the 'Adaptation' philosophy to translate ROS types into the Mosaico Ontology.
3. Connecting to the Mosaico Data Platform to verify and inspect the ingested sequence.
4. Accessing sequence-level metadata and physical diagnostics.
"""

import logging as log
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

# NOTE: The Before starting Phase 2, the custom adapter must be registered. See __init__.py
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

# Mosaico SDK Imports
from mosaicolabs import MosaicoClient, Time
from mosaicolabs.ros_bridge import ROSInjectionConfig, RosbagInjector

# Configuration Constants
MOSAICO_HOST = "localhost"
MOSAICO_PORT = 6726  # Standard Mosaico port
ASSET_DIR = Path("/tmp/mosaico_assets")

# NVIDIA R2B Dataset 2024 - Verified compatible with Mosaico
BAGFILE_URL = (
    "https://api.ngc.nvidia.com/v2/resources/org/nvidia/team/isaac/r2bdataset2024/1/files"
    "?redirect=true&path=r2b_whitetunnel/r2b_whitetunnel_0.mcap"
)

# Initialize Rich Console for beautiful terminal output
console = Console()


def _filename_from_url(url: str) -> Optional[str]:
    """Infers the filename from URL query parameters or the path."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "path" in qs and qs["path"]:
        return Path(qs["path"][0]).name
    name = Path(parsed.path).name
    return name if name else None


def download_asset(url: str, target_dir: Path) -> Path:
    """
    Downloads a remote asset with a high-visibility progress bar.

    This utility ensures the local workspace is prepared before injection begins.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _filename_from_url(url)
    if not filename:
        raise ValueError("Cannot resolve filename from URL.")

    file_path = target_dir / filename
    if file_path.exists():
        console.print(
            f"[yellow]Asset {filename} already exists. Skipping download.[/yellow]"
        )
        return file_path

    console.print(f"[bold blue]Downloading:[/bold blue] {url}")
    req = Request(url, headers={"User-Agent": "mosaico-downloader"})

    with urlopen(req) as response:
        total = int(response.headers.get("Content-Length", 0))
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Fetching {filename}", total=total)
            with open(file_path, "wb") as f:
                while chunk := response.read(1024 * 1024):
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))
    return file_path


def run_pipeline():
    """
    Executes the multi-phase Mosaico workflow.

    The phases are:
    1. Asset Preparation: Downloads the asset from the URL.
    2. ROS Bridge Injection: Injects the asset into Mosaico.
    3. Verification & Retrieval: Verifies the asset in Mosaico.
    """

    # --- PHASE 1: Asset Preparation ---
    try:
        downloaded_time = Time.now()
        out_bag_file = download_asset(BAGFILE_URL, ASSET_DIR)
    except Exception as e:
        log.error(f"Failed to prepare asset: {e}")
        sys.exit(1)

    # --- PHASE 2: ROS Bridge Injection ---
    # Configure the ROS injection. This uses the 'Adaptation' philosophy to translate
    # ROS types into the Mosaico Ontology.
    config = ROSInjectionConfig(
        host=MOSAICO_HOST,
        port=MOSAICO_PORT,
        # on_error=OnErrorPolicy.Report,
        file_path=out_bag_file,
        sequence_name=out_bag_file.stem,  # Sequence name derived from filename
        metadata={
            "source_url": BAGFILE_URL,
            "ingested_via": "mosaico_example_ros_injection",
            "download_time_utc": str(downloaded_time),
        },
        # topics=["/back_stereo_camera/left/image_compressed"],
        log_level="INFO",
    )

    console.print(Panel("[bold green]Phase 2: Starting ROS Ingestion[/bold green]"))
    injector = RosbagInjector(config)
    try:
        injector.run()  # Handles connection, loading, adaptation, and batching
    except Exception as e:
        console.print(f"[bold red]Injection Failed:[/bold red] {e}")
        sys.exit(1)

    # --- PHASE 3: Verification & Retrieval ---
    # Connect to the client using a context manager to ensure resource cleanup.
    console.print(Panel("[bold green]Phase 3: Verifying Data on Server[/bold green]"))

    with MosaicoClient.connect(host=MOSAICO_HOST, port=MOSAICO_PORT) as client:
        # Retrieve a SequenceHandler for the newly ingested data
        shandler = client.sequence_handler(out_bag_file.stem)

        if not shandler:
            console.print(
                "[bold red]Error:[/bold red] Could not retrieve sequence handler."
            )
            return

        # Display sequence diagnostics
        size_mb = shandler.total_size_bytes / (1024 * 1024)

        console.print(f"• [bold]Sequence Name:[/bold] {shandler.name}")
        console.print(f"• [bold]Remote Size:[/bold]   {size_mb:.2f} MB")
        console.print(f"• [bold]Created At:[/bold]    {shandler.created_datetime}")
        console.print(f"• [bold]Topics Found:[/bold]  {len(shandler.topics)}")

        for topic in shandler.topics:
            console.print(f"  - {topic}")

        # Retrieve a TopicHandler for a specific topic
        topic_name = "/front_stereo_imu/imu"
        tchandler = shandler.get_topic_handler(topic_name)

        if not tchandler:
            console.print(
                f"[bold red]Error:[/bold red] Could not retrieve topic handler {topic_name}"
            )
            return

        # Display topic diagnostics
        console.print(f"• [bold]Topic Name:[/bold] {tchandler.name}")
        console.print(f"• [bold]Topic Ontology Tag:[/bold] {tchandler.ontology_tag}")
        console.print(
            f"• [bold]Topic Timestamps Range:[/bold] {tchandler.timestamp_ns_min} - {tchandler.timestamp_ns_max}"
        )


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    log.basicConfig(level=log.INFO)
    run_pipeline()
