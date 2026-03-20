"""
Mosaico SDK: End-to-End ROS Ingestion & Retrieval Example.

This script demonstrates a complete workflow:
1. Downloading a remote ROS bag (MCAP) using a progress-monitored utility.
    Data are available at: https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1
2. Configuring and executing a high-performance ROS Bridge injection.
    The data are ingested using the 'Adaptation' philosophy to translate ROS types into the Mosaico Ontology.
3. Connecting to the Mosaico Data Platform to verify and inspect the ingested sequence.
4. Accessing sequence-level metadata and physical diagnostics.

Run the example via:
```bash
cd mosaico-sdk-py/src/examples && poetry run python ros_injection/main.py
```
"""

import logging as log
import sys
from pathlib import Path

# NOTE: The Before starting Phase 2, the custom adapter must be registered. See __init__.py
from rich.console import Console
from rich.panel import Panel

# Mosaico SDK Imports
from mosaicolabs import MosaicoClient, Time
from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig

# Example Imports
from ..config import ASSET_DIR, MOSAICO_HOST, MOSAICO_PORT
from .helpers import download_asset

# NVIDIA R2B Dataset 2024 - Verified compatible with Mosaico
BASE_BAGFILE_URL = "https://api.ngc.nvidia.com/v2/resources/org/nvidia/team/isaac/r2bdataset2024/1/files"

BAG_FILES_PATH = [
    "?redirect=true&path=r2b_galileo/r2b_galileo_0.mcap",
    "?redirect=true&path=r2b_galileo2/r2b_galileo2_0.mcap",
    "?redirect=true&path=r2b_robotarm/r2b_robotarm_0.mcap",
    "?redirect=true&path=r2b_whitetunnel/r2b_whitetunnel_0.mcap",
]

# Initialize Rich Console for beautiful terminal output
console = Console()


def main():
    """
    Executes the multi-phase Mosaico workflow.

    The phases are:
    1. Asset Preparation: Downloads the assets from the URL.
    2. ROS Bridge Injection: Injects the assets into Mosaico.
    3. Verification & Retrieval: Verifies the assets in Mosaico.
    """

    ingested_sequences = []
    # --- PHASE 1: Asset Preparation ---
    for bag_path in BAG_FILES_PATH:
        bag_file_url = BASE_BAGFILE_URL + bag_path
        try:
            downloaded_time = Time.now()
            out_bag_file = download_asset(bag_file_url, Path(ASSET_DIR), console)
        except Exception as e:
            log.error(f"Failed to prepare asset: {e}")
            sys.exit(1)

        ingested_sequences.append(out_bag_file.stem)

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
                "source_url": bag_file_url,
                "ingested_via": "mosaico_example_ros_injection",
                "download_time_ns": downloaded_time.to_nanoseconds(),
                "original_size_bytes": out_bag_file.stat().st_size,
            },
            # topics=["/back_stereo_camera/left/image_compressed"],
            log_level="INFO",
        )

        console.print(
            Panel(
                f"[bold green]Phase 2: Starting ROS Ingestion of sequence: {config.sequence_name}[/bold green]"
            )
        )
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
        seq_list = client.list_sequences()
        console.print(
            Panel(
                f"[bold cyan]Mosaico Server contains {len(seq_list)} sequences[/bold cyan]"
            )
        )
        for sequence_name in seq_list:
            if sequence_name in ingested_sequences:
                console.print(
                    f"• [bold]Sequence '{sequence_name}' correctly found on Server[/bold]"
                )


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    log.basicConfig(level=log.INFO)
    main()
