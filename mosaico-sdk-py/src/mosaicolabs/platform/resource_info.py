from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from pyarrow.flight import FlightEndpoint

from mosaicolabs.logging_config import get_logger

from .helpers import ParsingError, _decode_app_metadata

# Set the hierarchical logger
logger = get_logger(__name__)


@dataclass(frozen=True)
class TopicResourceInfo:
    """
    Metadata and structural information for a Mosaico
    [`Topic`][mosaicolabs.models.platform.Topic] resource.

    This Data Transfer Object summarizes the physical and logical state of a
    topic on the server, retrieved via the get_fligh_info enpoint.

    Attributes:
        total_size_bytes (int): The aggregate size of all data chunks in bytes.
        chunks_number (int): The total count of data partitions (chunks)
            stored on the server.
        timestamp_ns_min (Optional[int]): The minimum timestamp of the data in the topic.
        timestamp_ns_max (Optional[int]): The maximum timestamp of the data in the topic.
    """

    total_size_bytes: int
    chunks_number: int
    timestamp_ns_min: Optional[int]
    timestamp_ns_max: Optional[int]

    @classmethod
    def _from_flight_endpoint(cls, endpoint: FlightEndpoint) -> "TopicResourceInfo":
        """
        Factory method to create metadata from an Arrow Flight endpoint.

        Args:
            endpoint: The FlightEndpoint object containing location URIs.

        Returns:
            TopicResourceMetadata: An immutable instance containing parsed data.

        Raises:
            TopicParsingError: If the endpoint has no locations, multiple
                locations, or if the URI format is invalid.
        """
        try:
            app_mdata = _decode_app_metadata(endpoint.app_metadata)
        except ParsingError as pe:
            raise ParsingError(
                f"Failed to decode endpoint.app_metadata in TopicResourceInfo. Inner err: '{pe}'"
            )

        return TopicResourceInfo._from_info_metadata(app_mdata.get("info", {}))

    @classmethod
    def _from_info_metadata(
        cls,
        info_data: Dict[str, Any],
    ) -> "TopicResourceInfo":
        """
        Internal static method to parse topic-related remote info.

        Args:
            info_data (Dict[str, Any]): The info_data from the FlightInfo.

        Returns:
            TopicResourceInfo: The TopicResourceInfo object.
        """
        if not isinstance(info_data, dict):
            raise ValueError(
                f"Unrecognized format for TopicResourceInfo in manifest: type {type(info_data).__name__}, expected a JSON."
            )

        chunks_number = info_data.get("chunks_number")
        total_size_bytes = info_data.get("total_bytes")

        if chunks_number is None or total_size_bytes is None:
            raise ValueError("TopicResourceInfo in manifest misses required fields.")

        tmin, tmax = cls._parse_timestamp_range(info_data.get("timestamp", {}))

        return TopicResourceInfo(
            chunks_number=chunks_number,
            total_size_bytes=total_size_bytes,
            timestamp_ns_min=tmin,
            timestamp_ns_max=tmax,
        )

    @staticmethod
    def _parse_timestamp_range(
        tstamp_mdata: dict,
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Parses the minimum and maximum timestamps of the resource.

        Args:
            tstamp_mdata (dict): The timestamp metadata.

        Returns:
            Tuple[Optional[int], Optional[int]]: The minimum and maximum timestamps.
        """
        # (can be missing in manifest - i.e. degenerate Topics with no data stream)
        tmin = None
        tmax = None
        # Can be null (i.e. "timestamp" present but empty)
        if isinstance(tstamp_mdata, dict):
            tmin = tstamp_mdata.get("start_ns")
            tmax = tstamp_mdata.get("end_ns")
            # Ensure both keys exist
            if (tstamp_mdata.get("min") is None) != (tmax is None):
                logger.error(
                    f"Wrong format of 'timestamp' field: 'min' or 'max' are None, but not both, {tstamp_mdata}"
                )

        return tmin, tmax
