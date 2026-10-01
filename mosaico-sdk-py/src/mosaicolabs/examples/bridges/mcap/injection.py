"""
Mosaico SDK: End-to-End MCAP Ingestion & Retrieval Example.

This script demonstrates a complete workflow:
1. Downloading a remote MCAP using a progress-monitored utility.
    Data are available at: https://huggingface.co/datasets/Labelbox/robotics-datasets
2. Configuring and executing a high-performance MCAP Bridge injection.
    The data are ingested using the 'Adaptation' philosophy to translate MCAP types into the Mosaico Ontology.
3. Connecting to the Mosaico Data Platform to verify and inspect the ingested sequence.
4. Accessing sequence-level metadata and physical diagnostics.

Run the example via:
```bash
cd mosaico-sdk-py/src/examples/bridges && poetry run python mcap/injection.py
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
from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector

# Example Imports
from ...config import (
    API_KEY,
    ASSET_DIR,
    ENABLE_TLS,
    LOG_LEVEL,
    MOSAICO_HOST,
    MOSAICO_PORT,
)
from ..helpers import download_asset

# NVIDIA R2B Dataset 2024 - Verified compatible with Mosaico
BASE_MCAPS_URL = (
    "https://huggingface.co/datasets/Labelbox/robotics-datasets/resolve/main/data/"
)

MCAP_FILES_PATH = [
    "trio/connecting_cables_to_adapters/recording_0001.mcap",
    "bimanual/xarm6/place_clothing_into_basket/recording_0001.mcap",
    "bimanual/xarm6/remove_hats_and_dirty_laundry/recording_0001.mcap",
    "bimanual/xarm6/watering_house_plant/recording_0001.mcap",
]

# Initialize Rich Console for beautiful terminal output
console = Console()


def main():
    """
    Executes the multi-phase Mosaico workflow.

    The phases are:
    1. Asset Preparation: Downloads the assets from the URL.
    2. MCAP Bridge Injection: Injects the assets into Mosaico.
    3. Verification & Retrieval: Verifies the assets in Mosaico.
    """

    ingested_sequences = []
    # --- PHASE 1: Asset Preparation ---
    for mcap_path in MCAP_FILES_PATH:
        mcap_file_url = BASE_MCAPS_URL + mcap_path
        out_file_name = mcap_path.replace("/", "_")

        try:
            downloaded_time = Time.now()
            out_mcap_file = download_asset(
                mcap_file_url, Path(ASSET_DIR), console, out_file_name
            )
        except Exception as e:
            log.error(f"Failed to prepare asset: {e}")
            sys.exit(1)

        sequence_name = out_file_name.removesuffix(".mcap")
        ingested_sequences.append(sequence_name)

        # --- PHASE 2: MCAP Bridge Injection ---
        # Configure the MCAP injection. This uses the 'Adaptation' philosophy to translate
        # MCAP types into the Mosaico Ontology.
        config = MCAPInjectionConfig(
            host=MOSAICO_HOST,
            port=MOSAICO_PORT,
            enable_tls=ENABLE_TLS,
            mosaico_api_key=API_KEY,
            file_path=out_mcap_file,
            sequence_name=sequence_name,  # Sequence name derived from filename
            metadata={
                "source_url": mcap_file_url,
                "ingested_via": "mosaico_example_mcap_injection",
                "download_time_ns": downloaded_time.to_nanoseconds(),
                "original_size_bytes": out_mcap_file.stat().st_size,
            },
            # channels=["/back_stereo_camera/left/image_compressed"],
            log_level=LOG_LEVEL,
        )

        console.print(
            Panel(
                f"[bold green]Phase 2: Starting MCAP Ingestion of sequence: {config.sequence_name} - Size (MB): {out_mcap_file.stat().st_size / (1024 * 1024):.2f} [/bold green]"
            )
        )
        injector = MCAPInjector(config)

        try:
            injector.run()  # Handles connection, loading, adaptation, and batching
        except Exception as e:
            console.print(f"[bold red]Injection Failed:[/bold red] {e}")
            sys.exit(1)

    # --- PHASE 3: Verification & Retrieval ---
    # Connect to the client using a context manager to ensure resource cleanup.
    console.print(Panel("[bold green]Phase 3: Verifying Data on Server[/bold green]"))

    with MosaicoClient.connect(
        host=MOSAICO_HOST,
        port=MOSAICO_PORT,
        enable_tls=ENABLE_TLS,
        api_key=API_KEY,
    ) as client:
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
