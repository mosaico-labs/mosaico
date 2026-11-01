from dataclasses import dataclass
from typing import Optional

import pyarrow as pa


@dataclass
class ULogMessage:
    """
    The unit of work yielded by `ULogLoader`: all the valid rows of one ULog topic.

    Plays the role of `MCAPMessage` (bridges/mcap/mcap_message.py), with one difference.
    ULog is columnar (pyulog loads each topic as numpy columns), so the loader yields
    one whole topic at a time, as a `RecordBatch` with the `timestamp_ns` envelope column
    plus the raw ULog payload schema (`ULogSchemaConverter.to_pyarrow`), instead of one
    message per row.

    Attributes:
        topic_name (str): The Mosaico topic name, `/{message_name}/{multi_id}`.
        message_name (str): The ULog format name (e.g. `sensor_accel`).
        multi_id (int): The ULog instance of the format (0, 1, ...).
        data (Optional[pa.RecordBatch]): The topic rows, sorted by timestamp. `None` if
            building them failed.
    """

    topic_name: str
    message_name: str
    multi_id: int
    data: Optional[pa.RecordBatch]
