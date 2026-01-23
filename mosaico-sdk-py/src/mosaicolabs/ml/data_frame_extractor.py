import pyarrow as pa
import pandas as pd
from typing import List, Dict, Optional, Generator, Union, Tuple, Sequence

from mosaicolabs.handlers import SequenceHandler
from mosaicolabs.logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class DataFrameExtractor:
    """
    High-performance bridge between Mosaico Sequences and Tabular DataFrames.

    This class extracts data from multiple topics in a sequence and converts them
    into a single, flattened, sparse DataFrame. It uses a windowed approach to
    ensure memory efficiency even with multi-gigabyte sequences.

    Attributes:
        _sequence_handler (SequenceHandler): The handler for the source sequence.

    .. Warning::
        Setting `window_sec` to a very large value or float("inf") will disable windowing.
        The extractor will attempt to load the entire requested time range into memory.
        This is only recommended for small sequences or systems with high RAM capacity.
    """

    def __init__(self, sequence_handler: "SequenceHandler"):
        """
        Initializes the DataFrameExtractor.

        Args:
            sequence_handler (SequenceHandler): An active handle to a Mosaico sequence.
        """
        self._sequence_handler = sequence_handler

    def _resolve_selection(self, selection: Sequence) -> Dict[str, Optional[List[str]]]:
        """
        Parses the user selection into a standardized topic-to-fields map.

        Args:
            selection (List): List of strings (topic names) or tuples (topic, fields).

        Returns:
            Dict[str, Optional[List[str]]]: A map where keys are topic names and values
                are lists of requested sub-fields (None means 'all fields').
        """
        resolved = {}
        for item in selection:
            if isinstance(item, str):
                # Full topic selection: 'imu_front'
                resolved[item] = None
            elif isinstance(item, tuple):
                if len(item) != 2:
                    raise ValueError(f"Expected a tuple of 2 elements. Got `{item}`")
                # Targeted field selection: ('gps_main', ['position.z', 'status'])
                topic, fields = item
                if isinstance(fields, str):
                    fields = [fields]
                resolved[topic] = fields
        return resolved

    def to_pandas_chunks(
        self,
        selection: Optional[
            Sequence[Union[str, Tuple[str, Union[str, List[str]]]]]
        ] = None,
        window_sec: float = 5.0,
        timestamp_ns_start: Optional[int] = None,
        timestamp_ns_end: Optional[int] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Generator that yields time-windowed pandas DataFrames from the sequence.

        This method leverages server-side filtering and local batch processing
        to maintain a low memory footprint. It handles batches that cross window
        boundaries by carrying over the remainder to the next chunk.

        .. NOTE:: This function must be iterated (e.g. called in a for loop)

        Args:
            selection (List, optional): Topics and fields to extract. Defaults to all topics.
            window_sec (float): Duration of each DataFrame chunk in seconds.
            start_ns (int, optional): Global start time for extraction.
            end_ns (int, optional): Global end time for extraction.

        Yields:
            pd.DataFrame: A sparse, flattened DataFrame containing data from all
                selected topics and their fields within the current time window.
        """
        # Check if time information in sequence are coherent
        if (
            self._sequence_handler.timestamp_ns_min is None
            or self._sequence_handler.timestamp_ns_max is None
        ):
            raise ValueError(
                f"Unable to extract dataframe from the input sequence {self._sequence_handler.name}. "
                "The sequence might contain no data or could not derive 'min' and 'max' timestamps from topics"
            )
        # Provide clamped data to data streamer, for better time management
        timestamp_ns_start = (
            timestamp_ns_start
            if timestamp_ns_start is None
            else max(
                timestamp_ns_start, self._sequence_handler.timestamp_ns_min
            )  # do not go beyond (before) the minimum sequence timestamp
        )
        timestamp_ns_end = (
            timestamp_ns_end
            if timestamp_ns_end is None
            else min(
                timestamp_ns_end, self._sequence_handler.timestamp_ns_max
            )  # do not go beyond (after) the maximum sequence timestamp
        )
        seq_streamer = self._sequence_handler.get_data_streamer(
            topics=[t if isinstance(t, str) else t[0] for t in selection]
            if selection
            else [],
            start_timestamp_ns=timestamp_ns_start,
            end_timestamp_ns=timestamp_ns_end,
        )
        seq_readers = seq_streamer._as_batch_provider()

        # Setup temporal boundaries
        global_start_ns = (
            timestamp_ns_start  # Already clamped
            if timestamp_ns_start is not None
            else self._sequence_handler.timestamp_ns_min
        )
        global_end_ns = (
            timestamp_ns_end  # Already clamped
            if timestamp_ns_end is not None
            else self._sequence_handler.timestamp_ns_max
        )
        window_ns = int(window_sec * 1e9)
        is_full_load = window_ns >= (global_end_ns - global_start_ns)

        if is_full_load:
            seq_duration_sec = (global_end_ns - global_start_ns) / 1e9
            logger.warning(
                f"Window size ({window_sec}s) is larger than sequence duration ({seq_duration_sec}s). "
                "The entire sequence will be loaded into RAM. Ensure sufficient memory is available."
            )

        # Map selection for internal processing
        resolved_selection = (
            self._resolve_selection(selection)
            if selection
            else {t: None for t in seq_readers.keys()}  # all the available topics
        )

        # Carry-over buffer to handle batches spanning across two windows
        carry_over: Dict[str, pd.DataFrame] = {
            t: pd.DataFrame() for t in resolved_selection.keys()
        }

        try:
            current_window_start = global_start_ns
            while current_window_start < global_end_ns:
                current_window_end = current_window_start + window_ns
                window_parts = []

                # Safely convert data streams to DataFrame.
                # NOTE: if any topic contains no data, it is not included in the list of topic readers
                for t_name, reader in seq_readers.items():
                    # Retrieve leftover data from the previous iteration and re-initialize the Carry-over cache
                    df_topic = carry_over[t_name]
                    carry_over[t_name] = pd.DataFrame()

                    # Fetch raw batches from the state until the window is covered
                    while (
                        df_topic.empty
                        or df_topic["timestamp_ns"].max() < current_window_end
                    ):
                        batch = reader._fetch_next_batch()
                        if not batch:
                            break

                        # Process Arrow batch to Flattened Pandas DF
                        new_df = self._flatten_and_filter(
                            batch,
                            t_name,
                            resolved_selection.get(t_name),  # safe get
                        )
                        df_topic = pd.concat([df_topic, new_df], ignore_index=True)
                        del new_df  # Free tmp memory immediately

                    if not df_topic.empty:
                        if is_full_load:
                            window_parts.append(df_topic)
                        else:
                            # Split data: [Current Window] | [Future Data (Carry-over)]
                            mask = (
                                df_topic["timestamp_ns"] >= current_window_start
                            ) & (df_topic["timestamp_ns"] < current_window_end)

                            window_parts.append(df_topic[mask])
                            carry_over[t_name] = df_topic[~mask]

                # Consolidate and sort the sparse windowed DataFrame
                if window_parts:
                    # Concatenate all topics; missing values at specific timestamps become NaNs
                    yield pd.concat(
                        window_parts, axis=0, ignore_index=True
                    ).sort_values("timestamp_ns")

                # In full_load, we have finished after the first yield
                if is_full_load:
                    break

                current_window_start = current_window_end
        finally:
            seq_streamer.close()

    def _flatten_and_filter(
        self, batch: pa.RecordBatch, topic_name: str, fields: Optional[List[str]]
    ) -> pd.DataFrame:
        """
        Converts an Arrow RecordBatch into a flattened and namespace-prefixed DataFrame.

        Logic:
        1. Convert batch to pa.Table.
        2. Recursively flatten nested structs (Ontology paths).
        3. Convert to Pandas with self-destruction for memory efficiency.
        4. Apply column filtering based on user selection.
        5. Prefix columns with {topic_name}. to ensure namespace isolation.
        """
        table = pa.Table.from_batches([batch])

        # Recursive flattening of nested structs (e.g., 'pose.position.x')
        while any(pa.types.is_struct(f.type) for f in table.schema):
            table = table.flatten()

        # Zero-copy conversion where possible
        df = table.to_pandas(self_destruct=True)

        # Apply field selection filtering
        if fields is not None:
            df = df[self._match_columns(df.columns, fields)]
        # Apply topic-based namespace to all columns except the timestamp
        df.columns = [
            f"{topic_name}.{c}" if c != "timestamp_ns" else c for c in df.columns
        ]
        return df

    def _match_columns(self, columns: pd.Index, fields: List[str]) -> List[str]:
        prefixes = tuple(f + "." for f in fields)
        fields_set = set(fields)

        for f in fields:
            if not (f in columns or any(c.startswith(f + ".") for c in columns)):
                raise ValueError(f"The field '{f}' does not exist in the columns.")

        cols = [
            c
            for c in columns
            if c == "timestamp_ns" or c in fields_set or c.startswith(prefixes)
        ]

        return cols
