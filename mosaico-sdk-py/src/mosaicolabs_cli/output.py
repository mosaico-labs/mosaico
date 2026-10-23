"""Explicit, command-local output dispatch and reusable stream writers."""

import csv
import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Generic, TextIO, TypeVar

from mosaicolabs_cli.output_format import OutputFormat

# Version of the CLI JSON document contract, independent of SDK/server releases.
# Bump for incompatible field/type/meaning changes, not changes to record values.
OUTPUT_SCHEMA_VERSION = 1

Record = dict[str, Any]
Data = TypeVar("Data")


class OutputRenderer(Generic[Data]):
    """Dispatch to explicitly supplied writers, without global registration.

    Each instance owns its format mapping. Writers receive the original data
    and a caller-owned text stream; they decide whether to buffer or stream.
    The dispatcher neither consumes data nor closes the stream. Ordinary
    functions, partials, and callable objects can all serve as writers.
    """

    def __init__(
        self, handlers: Mapping[OutputFormat, Callable[[Data, TextIO], None]]
    ) -> None:
        self._handlers = dict(handlers)

    def render(
        self, data: Data, output: OutputFormat, *, stream: TextIO | None = None
    ) -> None:
        try:
            handler = self._handlers[output]
        except KeyError:
            supported = ", ".join(fmt.value for fmt in self._handlers)
            raise ValueError(
                f"Unsupported output format: '{output}'. Supported formats: {supported}"
            ) from None
        handler(data, sys.stdout if stream is None else stream)


def resolve_output(
    output: OutputFormat | None,
    *,
    piped: OutputFormat = OutputFormat.CSV,
) -> OutputFormat:
    """Resolve defaults before querying or handling empty results."""
    if output is not None:
        return output
    return OutputFormat.TABLE if sys.stdout.isatty() else piped


def render_json(data: Any, stream: TextIO) -> None:
    """Write a complete JSON document without terminal formatting."""
    stream.write(json.dumps(data, sort_keys=True) + "\n")


def render_json_collection(
    records: Iterable[Record], stream: TextIO, *, collection: str
) -> None:
    """Materialize a finite collection into the versioned JSON envelope."""
    render_json(
        {"schema_version": OUTPUT_SCHEMA_VERSION, collection: list(records)}, stream
    )


def render_jsonl(records: Iterable[Record], stream: TextIO) -> None:
    """Emit and flush each complete record before requesting the next one.

    May be called repeatedly on the same stream to append batches. If a producer
    fails, earlier records remain valid JSON Lines; the error propagates.
    """
    for record in records:
        stream.write(json.dumps(record, sort_keys=True) + "\n")
        stream.flush()


def render_csv(
    records: Iterable[Record], stream: TextIO, *, fields: Sequence[str]
) -> None:
    """Write only the command's explicit columns, without a header or decoration."""
    writer = csv.writer(stream, lineterminator="\n")
    for record in records:
        writer.writerow(
            str(record[field]).lower()
            if isinstance(record[field], bool)
            else record[field]
            for field in fields
        )
