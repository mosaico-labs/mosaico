"""
Shared configuration and helpers for the rosbags-tests scripts.

Every script in this folder works on each rosbag found in `MOSAICO_ASSETS_DIR`
and talks to a mosaicod instance started against the `docker/devel` stack.
The data is kept across runs, until it is wiped (see `WIPE_HINT`).

Two store backends are supported:
    - fs: local filesystem, data lands in `/tmp/mosaico-rosbags-tests`
    - s3: the versitygw object store defined in `docker/devel/compose.yml`

Each backend has its own database, so the catalog never references data that
lives in the other backend.

mosaicod is built with the `video` feature, so the store optimizer turns image
sequences into videos: FFmpeg must be installed (e.g. `brew install ffmpeg`).
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from mosaicolabs import MosaicoClient
from rich.console import Console

# When stdout is not a terminal (e.g. a report saved to a file), rich would wrap at
# 80 columns and truncate the tables.
console = Console(width=None if sys.stdout.isatty() else 120)

# --- Paths ---

ROSBAGS_TESTS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = ROSBAGS_TESTS_DIR.parent.parent
MOSAICOD_DIR = PROJECT_DIR / "mosaicod"
DOCKER_DIR = PROJECT_DIR / "docker" / "devel"

ASSETS_DIR = Path(os.environ.get("MOSAICO_ASSETS_DIR", Path.home() / "mosaico_assets"))
ROSBAG_SUFFIXES = {".mcap", ".db3", ".bag"}

# --- mosaicod ---

MOSAICOD_HOST = "localhost"
MOSAICOD_PORT = int(os.environ.get("MOSAICOD_PORT", "6726"))
MOSAICOD_STARTUP_TIMEOUT_S = 60
# The rosbags are large, a debug build would make ingestion slow.
MOSAICOD_PROFILE = "release"
# The store optimizer turns image sequences into videos only with this feature. Every
# script builds with it, otherwise each build would overwrite the binary of the other.
MOSAICOD_FEATURES = "mosaicod/video"

# --- Database (see docker/devel/compose.yml) ---

DB_URL = "postgresql://localhost:5432"
DB_SERVICE = "database"

# --- Object store (see docker/devel/compose.yml) ---

S3_SERVICE = "store"
# Where the bucket lives inside the versitygw container.
S3_BUCKET_DIR = "/data/store/mosaico"
DB_USER = "postgres"
DB_PASSWORD = "password"

# How to wipe the data of the tests. The object store is a folder bound into its container,
# so `down -v` does not remove it: it is emptied from the container, since with a VM (e.g.
# colima) the folder is in the VM, not on the host.
WIPE_HINT = (
    "in docker/devel, `docker compose exec store sh -c 'rm -rf /data/store/mosaico/*'` "
    "and `docker compose down -v`, then `rm -rf /tmp/mosaico-rosbags-tests`"
)


class Store(str, Enum):
    FS = "fs"
    S3 = "s3"

    @property
    def database(self) -> str:
        return f"mosaico_rosbags_{self.value}"

    @property
    def location(self) -> str:
        env = self.mosaicod_env()
        endpoint = env["MOSAICOD_STORE_ENDPOINT"].removeprefix("file://")
        return f"{endpoint}/{env['MOSAICOD_STORE_BUCKET']}"

    def mosaicod_env(self) -> dict:
        if self is Store.FS:
            return {
                "MOSAICOD_STORE_ENDPOINT": "file:///tmp",
                "MOSAICOD_STORE_BUCKET": "mosaico-rosbags-tests",
            }
        return {
            "MOSAICOD_STORE_ENDPOINT": "http://localhost:7090",
            "MOSAICOD_STORE_BUCKET": "mosaico",
            "MOSAICOD_STORE_ACCESS_KEY": "admin",
            "MOSAICOD_STORE_SECRET_KEY": "password",
        }


# --- Output ---


def info(msg: str):
    console.print(f"[bold blue]==>[/] {msg}")


def success(msg: str):
    console.print(f"[bold green]==>[/] {msg}")


def warn(msg: str):
    console.print(f"[bold yellow]==>[/] {msg}")


def error(msg: str):
    console.print(f"[bold red]error:[/] {msg}")


def die(msg: str):
    error(msg)
    sys.exit(1)


def run(cmd: list[str], cwd: Path, capture: bool = False, **kwargs) -> str:
    """Runs a command and exits with an error message if it fails."""
    result = subprocess.run(
        cmd, check=False, cwd=cwd, text=True, capture_output=capture, **kwargs
    )
    if result.returncode != 0:
        if capture:
            console.print(result.stderr)
        die(f"Command failed: {' '.join(cmd)}")
    return result.stdout if capture else ""


# --- Rosbags ---


def find_rosbags(bag: Path | None = None) -> list[Path]:
    """Returns the given rosbag, or every rosbag in `ASSETS_DIR` when none is given."""
    if bag is not None:
        if not bag.is_file():
            die(f"Rosbag not found: {bag}")
        return [bag.resolve()]

    bags = sorted(
        p for p in ASSETS_DIR.rglob("*") if p.suffix in ROSBAG_SUFFIXES and p.is_file()
    )
    if not bags:
        die(f"No rosbag found in {ASSETS_DIR}.")
    return bags


def sequence_name(bag: Path) -> str:
    """The sequence of a rosbag is named after its file, without the extension."""
    return bag.stem


class SequenceState(str, Enum):
    MISSING = "missing"
    COMPLETE = "complete"
    # A session was never finalized, e.g. because an ingestion was interrupted.
    PARTIAL = "partial"


def sequence_state(client: MosaicoClient, name: str) -> SequenceState:
    if not client.sequence_exists(name):
        return SequenceState.MISSING

    handler = client.sequence_handler(name)
    if handler is None or not handler.sessions:
        return SequenceState.PARTIAL
    if all(s.completed_timestamp is not None for s in handler.sessions):
        return SequenceState.COMPLETE
    return SequenceState.PARTIAL


# --- Environment ---


def _docker_compose() -> list[str]:
    """Supports both the `docker compose` plugin and the standalone `docker-compose`."""
    probe = subprocess.run(
        ["docker", "compose", "version"], check=False, capture_output=True
    )
    if probe.returncode == 0:
        return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    die("Docker Compose is not installed.")


def _start_docker():
    info("Starting [bold]docker/devel[/] stack")
    run([*_docker_compose(), "up", "-d", "--wait"], cwd=DOCKER_DIR)


def _ensure_database(store: Store):
    """Creates the backend database. mosaicod applies the migrations on startup."""
    exec_db = [*_docker_compose(), "exec", "-T", DB_SERVICE]
    query = f"SELECT 1 FROM pg_database WHERE datname = '{store.database}'"
    exists = run(
        [*exec_db, "psql", "-U", DB_USER, "-tAc", query], cwd=DOCKER_DIR, capture=True
    )
    if exists.strip() != "1":
        info(f"Creating database [bold]{store.database}[/]")
        run([*exec_db, "createdb", "-U", DB_USER, store.database], cwd=DOCKER_DIR)


def _build_mosaicod() -> Path:
    info(
        f"Building mosaicod (profile [bold]{MOSAICOD_PROFILE}[/], "
        f"features [bold]{MOSAICOD_FEATURES}[/])"
    )
    run(
        [
            "cargo",
            "build",
            "--profile",
            MOSAICOD_PROFILE,
            "--bin",
            "mosaicod",
            "--features",
            MOSAICOD_FEATURES,
        ],
        cwd=MOSAICOD_DIR,
        env={**os.environ, "SQLX_OFFLINE": "true"},
    )
    return MOSAICOD_DIR / "target" / MOSAICOD_PROFILE / "mosaicod"


def _port_in_use() -> bool:
    with socket.socket() as s:
        return s.connect_ex((MOSAICOD_HOST, MOSAICOD_PORT)) == 0


def _mosaicod_env(store: Store) -> dict:
    """The environment of every mosaicod process (server, store optimizer) of a store."""
    return {
        **os.environ,
        **store.mosaicod_env(),
        "RUST_LOG": os.environ.get("RUST_LOG", "mosaico=info"),
        "MOSAICOD_DB_URL": f"{DB_URL}/{store.database}",
        "MOSAICOD_DB_USER": DB_USER,
        "MOSAICOD_DB_PASSWORD": DB_PASSWORD,
    }


@contextmanager
def _run_mosaicod(binary: Path, store: Store) -> Iterator[None]:
    if _port_in_use():
        die(
            f"Port {MOSAICOD_PORT} is already in use. Stop the running server first, "
            "so the checks do not run against a different store."
        )

    env = _mosaicod_env(store)
    log_path = Path(f"/tmp/mosaicod-rosbags-tests-{store.value}.log")
    info(f"Starting mosaicod on port {MOSAICOD_PORT} (log [dim]{log_path}[/])")

    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [str(binary), "server", "--port", str(MOSAICOD_PORT)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    try:
        deadline = time.monotonic() + MOSAICOD_STARTUP_TIMEOUT_S
        while not _port_in_use():
            if proc.poll() is not None:
                console.print(log_path.read_text()[-2000:])
                die("mosaicod exited during startup.")
            if time.monotonic() > deadline:
                die(f"mosaicod did not start within {MOSAICOD_STARTUP_TIMEOUT_S}s.")
            time.sleep(0.5)
        yield
    finally:
        proc.terminate()
        proc.wait()
        console.print("[dim]mosaicod stopped.[/]")


@dataclass
class Environment:
    store: Store
    client: MosaicoClient
    binary: Path


@contextmanager
def environment(store: Store) -> Iterator[Environment]:
    """
    Starts docker, prepares the database, then builds and runs mosaicod.

    Yields a client connected to it. mosaicod is stopped on exit.
    """
    _start_docker()
    _ensure_database(store)
    binary = _build_mosaicod()

    with _run_mosaicod(binary, store):
        console.print(f" * store     [dim]{store.value} ({store.location})[/]")
        console.print(f" * database  [dim]{DB_URL}/{store.database}[/]")
        with MosaicoClient.connect(host=MOSAICOD_HOST, port=MOSAICOD_PORT) as client:
            yield Environment(store=store, client=client, binary=binary)


def run_store_optimizer(env: Environment) -> Path:
    """Runs a single pass of the store optimizer and returns the path of its log."""
    log_path = Path(f"/tmp/mosaicod-rosbags-tests-{env.store.value}-optimizer.log")
    info(f"Running the store optimizer (log [dim]{log_path}[/])")
    with open(log_path, "w") as log:
        # A time interval of 0 performs a single run, then exits.
        result = subprocess.run(
            [str(env.binary), "store-optimizer", "--time-interval", "0"],
            check=False,
            env=_mosaicod_env(env.store),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    if result.returncode != 0:
        console.print(log_path.read_text()[-2000:])
        die("The store optimizer failed.")
    return log_path


# --- Inspection ---


def query_database(store: Store, sql: str) -> list[list[str]]:
    """Runs a query on the database of the store and returns its rows."""
    out = run(
        [
            *_docker_compose(),
            "exec",
            "-T",
            DB_SERVICE,
            "psql",
            "-U",
            DB_USER,
            "-d",
            store.database,
            "-tA",
            "-F",
            "\t",
            "-c",
            sql,
        ],
        cwd=DOCKER_DIR,
        capture=True,
    )
    return [line.split("\t") for line in out.splitlines() if line]


def folder_sizes(store: Store) -> dict[str, int]:
    """Returns the bytes of the files of each top-level folder of the store."""
    sizes: dict[str, int] = {}
    if store is Store.FS:
        root = Path(store.location)
        for path in root.rglob("*"):
            if path.is_file():
                folder = path.relative_to(root).parts[0]
                sizes[folder] = sizes.get(folder, 0) + path.stat().st_size
        return sizes

    # The S3 bucket is a folder inside the versitygw container.
    out = run(
        [
            *_docker_compose(),
            "exec",
            "-T",
            S3_SERVICE,
            "find",
            S3_BUCKET_DIR,
            "-type",
            "f",
            "-exec",
            "stat",
            "-c",
            "%s %n",
            "{}",
            "+",
        ],
        cwd=DOCKER_DIR,
        capture=True,
    )
    for line in out.splitlines():
        size, name = line.split(" ", 1)
        folder = Path(name).relative_to(S3_BUCKET_DIR).parts[0]
        sizes[folder] = sizes.get(folder, 0) + int(size)
    return sizes


def parse_arguments(description: str) -> Path | None:
    """Parses the arguments shared by every rosbags-tests script and returns the rosbag."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "bag",
        nargs="?",
        type=Path,
        metavar="ROSBAG",
        help=f"Rosbag to process. Defaults to every rosbag in {ASSETS_DIR}.",
    )
    return parser.parse_args().bag
