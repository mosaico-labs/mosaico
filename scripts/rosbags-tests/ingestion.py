"""
Ingests each rosbag into mosaicod, skipping the ones already stored.

Each rosbag becomes a sequence named after its file, without the extension.
A sequence left partial by an interrupted ingestion is reported as a failure.
"""

# Run through `filesystem/ingest.py` or `s3/ingest.py`, which select the store.

from pathlib import Path

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig

from common import (
    MOSAICOD_HOST,
    MOSAICOD_PORT,
    Environment,
    SequenceState,
    Store,
    console,
    environment,
    error,
    find_rosbags,
    info,
    parse_arguments,
    sequence_name,
    sequence_state,
    success,
    warn,
)


def ingest(bag: Path, name: str):
    config = ROSInjectionConfig(
        file_path=bag,
        sequence_name=name,
        host=MOSAICOD_HOST,
        port=MOSAICOD_PORT,
    )
    # The injector returns instead of raising when the user presses Ctrl-C.
    if RosbagInjector(config).run() is InjectionStatus.CANCELLED:
        raise KeyboardInterrupt


def ingest_rosbags(env: Environment, bags: list[Path]) -> bool:
    """Ingests the rosbags, skipping the ones already stored. Returns whether none failed."""
    ingested, skipped, failed = [], [], []

    for bag in bags:
        name = sequence_name(bag)
        console.print()
        info(f"[bold]{name}[/] [dim]({bag})[/]")

        state = sequence_state(env.client, name)
        if state is SequenceState.COMPLETE:
            warn(f"Sequence '{name}' is already stored, skipping.")
            skipped.append(name)
            continue
        if state is SequenceState.PARTIAL:
            error(f"Sequence '{name}' is partial, an ingestion was interrupted.")
            failed.append(name)
            continue

        try:
            ingest(bag, name)
        except Exception:  # noqa: BLE001
            # The injector already logged the traceback.
            error(f"Ingestion of '{name}' failed.")
            failed.append(name)
            continue
        success(f"Sequence '{name}' ingested.")
        ingested.append(name)

    console.print()
    info(f"Ingestion summary (store [bold]{env.store.value}[/])")
    console.print(f" * ingested  {len(ingested)} [dim]{' '.join(ingested)}[/]")
    console.print(f" * skipped   {len(skipped)} [dim]{' '.join(skipped)}[/]")
    console.print(f" * failed    {len(failed)} [dim]{' '.join(failed)}[/]")

    return not failed


def main(store: Store) -> int:
    bags = find_rosbags(parse_arguments(f"{__doc__}\nStore: {store.location}"))
    with environment(store) as env:
        ok = ingest_rosbags(env, bags)
    return 0 if ok else 1
