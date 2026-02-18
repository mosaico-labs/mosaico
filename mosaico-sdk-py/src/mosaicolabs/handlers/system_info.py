from dataclasses import dataclass
import datetime
from typing import Optional


@dataclass
class SystemInfo:
    """
    Metadata and structural information for a Mosaico
    [`Sequence`][mosaicolabs.models.platform.Sequence] or
    [`Topic`][mosaicolabs.models.platform.Topic] resource.

    This Data Transfer Object summarizes the physical and logical state of a
    sequence or topic on the server, typically retrieved via a system-info action.

    Attributes:
        total_size_bytes (int): The aggregate size of all data chunks in bytes.
        created_datetime (datetime.datetime): The UTC timestamp of when the
            resource was first initialized.
        is_locked (bool): Indicates if the resource is currently read-only.
            Usually true if an upload is finalized or a retention policy is active.
        chunks_number (Optional[int]): The total count of data partitions (chunks)
            stored on the server. Defaults to None if not applicable.
    """

    total_size_bytes: int
    created_datetime: datetime.datetime
    is_locked: bool
    chunks_number: Optional[int] = None
