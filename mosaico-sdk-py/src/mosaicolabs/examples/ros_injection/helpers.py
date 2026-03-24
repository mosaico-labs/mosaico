from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

# NOTE: The Before starting Phase 2, the custom adapter must be registered. See __init__.py
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


def _filename_from_url(url: str) -> Optional[str]:
    """Infers the filename from URL query parameters or the path."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "path" in qs and qs["path"]:
        return Path(qs["path"][0]).name
    name = Path(parsed.path).name
    return name if name else None


def download_asset(url: str, target_dir: Path, console: Console) -> Path:
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
