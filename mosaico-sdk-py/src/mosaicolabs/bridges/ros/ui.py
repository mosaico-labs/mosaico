from typing import Dict, List, Optional, Protocol, Tuple

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .loader import TopicStatus


# --- Generic Protocol for ProgressManager LoaderUIAPI ---
class LoaderUIAPI(Protocol):
    """
    Structural protocol for data loaders consumed by :class:`ProgressManager`.

    Both :class:`ROSLoader` and :class:`MosaicoLoader` satisfy this protocol,
    allowing :class:`ProgressManager` to set up progress bars without depending
    on a concrete loader class.
    """

    @property
    def topics(self) -> List[str]:
        """This should return the Mosaico compatible topics of the loaded data as strings"""
        ...

    @property
    def resolved_topics(self) -> List[str]:
        """This should return **all** the topics of the loaded data as strings"""
        ...

    @property
    def rejected_topics(self) -> List[Tuple[str, TopicStatus]]:
        """This should return a list of tuples containing all the rejected topic names, and the rejection reason (topic_name, topic_status)"""
        ...

    def msg_count(self, topic: Optional[str] = None) -> int:
        """This should return the total number of messages in the passed
        topic if not None. Otherwise returns all messages in all topics"""
        ...


# --- UI / Progress Helper ---


class ProgressManager:
    """
    Visual management system for loader tracking.

    This class decouples the UI presentation logic from the data processing pipeline.
    It utilizes the `rich` library to provide real-time feedback through progress bars,
    tracking individual topic throughput and aggregate global progress.


    Methods:
        setup(): Initializes the progress tracking tasks by querying message counts from the loader.
        update_status(topic, status, style): Modifies the label of a specific topic bar.
        advance_global(): Increments the master progress bar without affecting individual topic bars.
        advance_all(topic): Increments both the specific topic task and the global master task.
    """

    def __init__(self, loader: LoaderUIAPI):
        """
        Initialize the progress manager.

        Args:
            loader (LoaderUIAPI): The initialized data loader. Used to query total
                                message counts for setting up progress bars.
        """
        self.loader = loader
        self.progress = Progress(
            TextColumn("[bold cyan]{task.fields[name]}"),
            BarColumn(),
            MofNCompleteColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TimeRemainingColumn(),
            "•",
            TimeElapsedColumn(),
            expand=True,
        )
        self.tasks: Dict[str, TaskID] = {}
        self.global_task: Optional[TaskID] = None

    def setup(self):
        """
        Calculates totals and creates the visual progress tasks.
        Must be called before the main processing loop starts.
        """
        # Create individual progress bars for each topic but count only the accepted ones
        for topic_name in self.loader.resolved_topics:
            if topic_name in self.loader.topics:
                count = self.loader.msg_count(topic_name)
            else:
                count = None

            self.tasks[topic_name] = self.progress.add_task(
                "", total=count, name=topic_name
            )

        # Rejected topics (with rejected reason) are highlighted
        for topic_name, topic_status in self.loader.rejected_topics:
            self.update_status(
                topic_name, topic_status.value, topic_status.display_color()
            )

        # Create a master progress bar for the aggregate total of the accepted topics
        total_msgs = sum(self.loader.msg_count(t) for t in self.loader.topics)
        self.global_task = self.progress.add_task(
            "Total", total=total_msgs, name="Total Upload"
        )

    def update_status(self, topic: str, status: str, style: str = "white"):
        """
        Updates the text description of a specific topic's progress bar.
        Useful for indicating errors or skipped topics (e.g. "[red]Unresolved Adapter").

        Args:
            topic (str): The topic name.
            status (str): The status message to display.
            style (str): The rich style string (e.g., 'red', 'bold yellow').
        """
        if topic in self.tasks:
            self.progress.update(
                self.tasks[topic],
                name=f"[{style}]{topic}: {status}",
            )

    def advance_global(self):
        """Advances only the global progress bar (used when skipping messages)."""
        if self.global_task is not None:
            self.progress.advance(self.global_task)

    def advance_all(self, topic: str):
        """Advances both the specific topic's bar and the global bar."""
        if topic in self.tasks:
            self.progress.advance(self.tasks[topic])
        if self.global_task is not None:
            self.progress.advance(self.global_task)
