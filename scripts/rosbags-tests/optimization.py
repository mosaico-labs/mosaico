"""
Ingests the rosbags, then runs the store optimizer and reports what it did.

The store optimizer usually runs once a day: here a single pass starts one
minute after the sequences are stored. It rewrites each topic into a new
folder, turning image sequences into lossless videos, then switches the topic
to it in the database. The old folder stays in the store until the cleanup
routine removes it, so both are measured.

The report has a table for each rosbag, with the ontology and serialization
format of each topic and what the optimizer did to it: only image sequences
become videos, while the other topics, including images such as
`CompressedImage`, which the optimizer does not parse, only have their chunks
compacted. A second table shows the size of each topic before and after and
the time the optimizer spent on it. A final table shows, for each rosbag, the size of its topics before
and after and the time the optimizer spent on them.
"""

# Run through `filesystem/optimize.py` or `s3/optimize.py`, which select the store.

import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from rich import box
from rich.table import Table

from common import (
    Environment,
    Store,
    console,
    environment,
    error,
    find_rosbags,
    folder_sizes,
    info,
    parse_arguments,
    query_database,
    run_store_optimizer,
    sequence_name,
    success,
    warn,
)
from ingestion import ingest_rosbags

# The delay between the end of the ingestion and the store optimizer.
OPTIMIZER_DELAY_S = 60

# The serialization formats the store optimizer turns into videos.
VIDEO_FORMATS = {"image_sequence", "image_sequence_raw"}
# The serialization format of the other images, such as `CompressedImage`: the
# store optimizer does not parse them, and only compacts their chunks.
IMAGE_FORMATS = {"image"}


@dataclass
class Topic:
    path: str
    format: str
    ontology: str
    # When the topic was optimized, in nanoseconds since the Unix epoch.
    optimized_ns: int | None


def topics(store: Store) -> dict[str, Topic]:
    """The topics of the store, by locator (`<sequence>/<topic>`)."""
    rows = query_database(
        store,
        "SELECT locator_name, path_in_store, serialization_format, ontology_tag, "
        "optimization_end_unix_tstamp FROM topic_t",
    )
    return {
        locator: Topic(path, fmt, ontology, int(end) if end else None)
        for locator, path, fmt, ontology, end in rows
    }


def notifications(store: Store, since_ns: int) -> dict[str, list[str]]:
    """The notifications written on each topic since `since_ns`, by locator."""
    rows = query_database(
        store,
        "SELECT t.locator_name, n.msg FROM topic_notification_t n "
        "JOIN topic_t t ON t.topic_id = n.topic_id "
        f"WHERE n.creation_unix_tstamp >= {since_ns} ORDER BY n.creation_unix_tstamp",
    )
    result: dict[str, list[str]] = {}
    for locator, msg in rows:
        result.setdefault(locator, []).append(msg)
    return result


class Outcome(str, Enum):
    """What the store optimizer did to a topic."""

    VIDEO = "FFV1 video"
    COMPACTED = "compacted"
    IMAGES_COMPACTED = "compacted, not parsed"
    ISSUE = "left as sent"
    ALREADY_OPTIMIZED = "already optimized"
    NOT_OPTIMIZED = "not optimized"


@dataclass
class TopicReport:
    name: str
    ontology: str
    format: str
    outcome: Outcome
    issues: list[str] = field(default_factory=list)
    # Measured only for the topics optimized by this run.
    size_before: int = 0
    size_after: int = 0
    seconds: float | None = None


@dataclass
class Report:
    sequence: str
    topics: list[TopicReport] = field(default_factory=list)
    size_before: int = 0
    size_after: int = 0
    seconds: float = 0.0

    def count(self, outcome: Outcome) -> int:
        return sum(t.outcome is outcome for t in self.topics)


def outcome(old: Topic, new: Topic, issues: list[str]) -> Outcome:
    if old.optimized_ns:
        return Outcome.ALREADY_OPTIMIZED
    if not new.optimized_ns:
        return Outcome.NOT_OPTIMIZED
    if old.format in VIDEO_FORMATS:
        return Outcome.ISSUE if issues or new.path == old.path else Outcome.VIDEO
    if old.format in IMAGE_FORMATS:
        return Outcome.IMAGES_COMPACTED
    return Outcome.COMPACTED


def build_reports(
    names: list[str],
    before: dict[str, Topic],
    after: dict[str, Topic],
    issues: dict[str, list[str]],
    sizes: dict[str, int],
    start_ns: int,
) -> list[Report]:
    reports = {name: Report(name) for name in names}

    # The optimizer works on a topic at a time: the time spent on a topic is the
    # time between its end and the end of the previous one.
    optimized_now = sorted(
        (t.optimized_ns, locator)
        for locator, t in after.items()
        if t.optimized_ns and not before[locator].optimized_ns
    )
    seconds = {}
    previous_ns = start_ns
    for end_ns, locator in optimized_now:
        seconds[locator] = (end_ns - previous_ns) / 1e9
        previous_ns = end_ns

    for locator, old in sorted(before.items()):
        sequence, name = locator.split("/", 1)
        report = reports.get(sequence)
        if report is None:
            continue
        new = after[locator]
        topic_issues = issues.get(locator, [])
        topic = TopicReport(
            "/" + name,
            old.ontology,
            old.format,
            outcome(old, new, topic_issues),
            topic_issues,
        )
        if locator in seconds:
            topic.seconds = seconds[locator]
            topic.size_before = sizes.get(old.path, 0)
            topic.size_after = sizes.get(new.path, 0)
            report.seconds += topic.seconds
            report.size_before += topic.size_before
            report.size_after += topic.size_after
        report.topics.append(topic)
    return list(reports.values())


OUTCOME_STYLES = {
    Outcome.VIDEO: "green",
    Outcome.IMAGES_COMPACTED: "yellow",
    Outcome.ISSUE: "yellow",
    Outcome.ALREADY_OPTIMIZED: "yellow",
    Outcome.NOT_OPTIMIZED: "red",
}


def print_topics(report: Report):
    """Prints what the store optimizer did to each topic of a rosbag."""
    counts = ", ".join(f"{n} {o.value}" for o in Outcome if (n := report.count(o)))
    table = Table(
        title=f"{report.sequence} ({len(report.topics)} topics)",
        caption=counts,
        box=box.SIMPLE_HEAD,
    )
    for column in ("Topic", "Ontology", "Format", "Optimization"):
        table.add_column(column, no_wrap=True)
    for t in report.topics:
        table.add_row(
            t.name,
            t.ontology,
            t.format,
            t.outcome.value,
            style=OUTCOME_STYLES.get(t.outcome),
        )
    console.print()
    console.print(table)


def print_topic_sizes(report: Report):
    """Prints the size of each topic of a rosbag before and after, and its time."""
    table = Table(
        title=f"{report.sequence}: size and time of each topic",
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Topic", no_wrap=True)
    for column in (
        "Before (KiB)",
        "After (KiB)",
        "Saved (KiB)",
        "Saved (%)",
        "Time (s)",
    ):
        table.add_column(column, justify="right", no_wrap=True)

    def row(name: str, before: int, after: int, seconds: float | None) -> list[str]:
        if seconds is None:
            return [name, "-", "-", "-", "-", "-"]
        saved = before - after
        return [
            name,
            kib(before),
            kib(after),
            kib(saved),
            f"{saved / before:.1%}" if before else "-",
            f"{seconds:.2f}",
        ]

    for t in report.topics:
        table.add_row(*row(t.name, t.size_before, t.size_after, t.seconds))
    table.add_section()
    table.add_row(
        *row("total", report.size_before, report.size_after, report.seconds),
        style="bold",
    )
    console.print()
    console.print(table)


def print_summary(reports: list[Report], store: Store, wall_s: float):
    table = Table(
        title=f"Store optimizer summary (store {store.value})",
        caption="Only image sequences become videos: the store optimizer does not "
        "parse other images, such as CompressedImage, and only compacts their chunks.",
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Rosbag", no_wrap=True)
    for column in (
        "Before (MiB)",
        "After (MiB)",
        "Saved (MiB)",
        "Saved (%)",
        "Time (s)",
    ):
        table.add_column(column, justify="right", no_wrap=True)

    def row(r: Report) -> list[str]:
        saved = r.size_before - r.size_after
        return [
            r.sequence,
            mib(r.size_before),
            mib(r.size_after),
            mib(saved),
            f"{saved / r.size_before:.1%}" if r.size_before else "-",
            f"{r.seconds:.1f}",
        ]

    for r in reports:
        table.add_row(*row(r))
    total = Report(
        "total",
        size_before=sum(r.size_before for r in reports),
        size_after=sum(r.size_after for r in reports),
        seconds=sum(r.seconds for r in reports),
    )
    table.add_section()
    table.add_row(*row(total), style="bold")

    console.print()
    console.print(table)
    console.print(f"Store optimizer run: {wall_s:.1f} s")


def mib(size: int) -> str:
    return f"{size / 2**20:,.1f}"


def kib(size: int) -> str:
    return f"{size / 2**10:,.1f}"


def optimize(env: Environment, names: list[str]) -> bool:
    """Runs the store optimizer and prints its report. Returns whether it all went well."""
    before = topics(env.store)
    start_ns = time.time_ns()
    start = time.monotonic()
    log_path = run_store_optimizer(env)
    wall_s = time.monotonic() - start

    after = topics(env.store)
    issues = notifications(env.store, start_ns)
    reports = build_reports(
        names, before, after, issues, folder_sizes(env.store), start_ns
    )
    for report in reports:
        print_topics(report)
        print_topic_sizes(report)
    print_summary(reports, env.store, wall_s)

    ok = True
    for r in reports:
        for t in r.topics:
            for message in t.issues:
                warn(f"{r.sequence}{t.name}: {message}")
        if n := r.count(Outcome.ALREADY_OPTIMIZED):
            warn(f"{r.sequence}: {n} topics were already optimized.")
        if n := r.count(Outcome.NOT_OPTIMIZED):
            error(f"{r.sequence}: {n} topics were not optimized.")
            ok = False
    if not ok:
        error(f"See the store optimizer log: {log_path}")
    return ok


def ingest_and_optimize(env: Environment, bags: list[Path]) -> bool:
    """
    Ingests the rosbags, then runs the store optimizer one minute later and prints
    its report. Returns whether it all went well.
    """
    ingested = ingest_rosbags(env, bags)

    console.print()
    info(f"Waiting {OPTIMIZER_DELAY_S} s before running the store optimizer")
    time.sleep(OPTIMIZER_DELAY_S)

    optimized = optimize(env, [sequence_name(bag) for bag in bags])
    return ingested and optimized


def main(store: Store) -> int:
    bags = find_rosbags(parse_arguments(f"{__doc__}\nStore: {store.location}"))
    with environment(store) as env:
        ok = ingest_and_optimize(env, bags)

    if ok:
        success("Store optimization completed.")
        return 0
    return 1
