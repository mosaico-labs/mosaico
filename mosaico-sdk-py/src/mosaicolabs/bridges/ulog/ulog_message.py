from dataclasses import dataclass
from typing import Iterator

import pyarrow as pa


@dataclass
class ULogMessage:
    """
    The unit of work yielded by `ULogLoader`: all the valid rows of one ULog topic.

    ULog is columnar (pyulog loads each topic as numpy columns), so the loader yields one
    whole topic at a time instead of one message per row. Its rows come as `RecordBatch`
    chunks, each with the `timestamp_ns` envelope column plus the raw ULog payload schema
    (`ULogSchemaConverter.to_pyarrow`).

    Attributes:
        topic_name (str): The Mosaico topic name, `/{message_name}/{multi_id}`.
        message_name (str): The ULog format name (e.g. `sensor_accel`).
        multi_id (int): The ULog instance of the format (0, 1, ...).
        data (Iterator[pa.RecordBatch]): The topic rows sorted by timestamp, in chunks
            within the batch size limit of the loader. A chunk is built only when it is
            requested. All of them must be consumed before asking the loader for the next
            topic, which releases the columns they are built from.
    """

    topic_name: str
    message_name: str
    multi_id: int
    data: Iterator[pa.RecordBatch]
