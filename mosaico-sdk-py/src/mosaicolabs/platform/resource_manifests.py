import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from mosaicolabs.logging_config import get_logger

from ..helpers.helpers import unpack_topic_full_path
from .resource_info import (
    SequenceResourceInfo,
    TopicResourceInfo,
)

# Use TYPE_CHECKING to avoid circular imports or heavy dependencies at runtime
if TYPE_CHECKING:
    from pyarrow.flight import FlightEndpoint

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicParsingError(Exception):
    """Raised when TopicResourceMetadata cannot be extracted from an endpoint."""

    pass


class SessionParsingError(Exception):
    """Raised when SessionResourceMetadata cannot be extracted from an endpoint."""

    pass


class SequenceParsingError(Exception):
    """Raised when SequenceResourceMetadata cannot be extracted from an endpoint."""

    pass


@dataclass(frozen=True)
class TopicResourceManifest:
    """
    Metadata container for a specific data topic resource.

    This class acts as a Value Object, standardizing topic and sequence
    identifiers extracted from Arrow Flight transport layers. Being 'frozen'
    ensures the metadata remains immutable and hashable throughout its lifecycle.

    Attributes:
        topic_name (str): The standardized name of the resource topic.
        sequence_name (str): The name of the sequence the topic belongs to.
        resource_info (TopicResourceInfo): The server side system info of the topic resource.
        timestamp_ns_min (Optional[int]): The minimum timestamp of the topic in nanoseconds.
        timestamp_ns_max (Optional[int]): The maximum timestamp of the topic in nanoseconds.
    """

    name: str
    sequence_name: str
    resource_info: TopicResourceInfo
    timestamp_ns_min: Optional[int]
    timestamp_ns_max: Optional[int]

    @classmethod
    def _from_flight_endpoint(
        cls, endpoint: "FlightEndpoint"
    ) -> "TopicResourceManifest":
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
        # Flight endpoints can technically have multiple locations for redundancy,
        # but our specific domain logic expects exactly one primary location.
        if not endpoint.locations:
            raise TopicParsingError(
                "Endpoint contains no locations; cannot resolve topic."
            )

        if len(endpoint.locations) > 1:
            raise TopicParsingError(
                f"Multi-location endpoints not supported. Found: {len(endpoint.locations)}"
            )

        try:
            # Extract URI (stored as bytes in pyarrow.flight)
            uri_bytes = endpoint.locations[0].uri

            # Delegate parsing logic to the internal static helper
            seq_name, topic_name = cls._parse_uri(uri_bytes)
            # Parse and return the app_metadata fields
            app_mdata = cls._decode_app_metadata(endpoint.app_metadata)
            if app_mdata is None:
                logger.error(TopicParsingError("Failed to parse app_metadata"))
                return cls(
                    name=topic_name,
                    sequence_name=seq_name,
                    resource_info=TopicResourceInfo._make_void(),
                    timestamp_ns_min=None,
                    timestamp_ns_max=None,
                )
            tmin_ns, tmax_ns = cls._parse_timestamp_range(
                app_mdata.get("timestamp", {})
            )
            resource_info = TopicResourceInfo._from_info_metadata(
                app_mdata.get("info", {})
            )

            return cls(
                name=topic_name,
                sequence_name=seq_name,
                timestamp_ns_min=tmin_ns,
                timestamp_ns_max=tmax_ns,
                resource_info=resource_info,
            )

        except Exception as e:
            # Wrap internal errors (like UnicodeDecode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise TopicParsingError(
                f"Failed to parse metadata from endpoint: {e}"
            ) from e

    @staticmethod
    def _parse_uri(uri_bytes: bytes) -> Tuple[str, str]:
        """
        Decodes and validates the raw URI string.

        Internal helper that handles the 'mosaico:' protocol stripping
        and string splitting logic.
        """
        # Decode bytes to string and protocol validation (mosaico resource)
        decoded_uri = uri_bytes.decode("utf-8")
        if not decoded_uri.startswith("mosaico:"):
            raise TopicParsingError(
                f"URI missing required 'mosaico:' prefix: {decoded_uri}"
            )

        # Path Extraction
        path = decoded_uri.removeprefix("mosaico:")

        # Domain-specific unpacking (expects a tuple of strings)
        result = unpack_topic_full_path(path)

        if not result or len(result) != 2:
            raise TopicParsingError(
                f"Path '{path}' is not a valid sequence/topic pair."
            )

        return result

    @staticmethod
    def _decode_app_metadata(
        app_mdata: Union[bytes, str],
    ) -> Optional[Dict[str, Any]]:
        """
        Decodes and validates the raw App Metadata JSON payload.

        Args:
            app_mdata: JSON payload as a UTF-8 string or byte sequence.

        Returns:
            Tuple: (timestamp_ns_min, timestamp_ns_max, PlatformResourceInfo(...))

        Raises:
            TopicParsingError: If JSON is malformed or missing required schema keys.
        """
        # Decode input to string
        try:
            raw_str = (
                app_mdata.decode("utf-8") if isinstance(app_mdata, bytes) else app_mdata
            )
        except UnicodeDecodeError as e:
            logger.error(
                TopicParsingError(f"App metadata bytes are not UTF-8, err '{e}'")
            )
            return None

        # Check empty-string
        if not raw_str:
            logger.error(TopicParsingError("Empty app_metadata"))
            return None

        # Safely load into JSON
        try:
            data = json.loads(raw_str)
        except json.JSONDecodeError as e:
            logger.error(TopicParsingError(f"Invalid JSON in app_metadata, err: '{e}'"))
            return None

        # Validate format
        if not isinstance(data, dict):
            logger.error(
                TopicParsingError(f"Expected JSON object, got {type(data).__name__}")
            )
            return None

        # --- Check for mandatory terms ---
        info_data = data.get("info")

        if info_data is None:
            logger.error(
                TopicParsingError(
                    "Cannot find mandatory element 'info' in topic app_metadata"
                )
            )
            return None

        return data

    @staticmethod
    def _parse_timestamp_range(
        tstamp_mdata: dict,
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Returns the minimum and maximum timestamps of the resource.

        Returns:
            Tuple[Optional[int], Optional[int]]: The minimum and maximum timestamps.
        """
        # (can be missing in manifest - i.e. degenerate Topics with no data stream)
        tmin = None
        tmax = None
        # Can be null (i.e. "timestamp" present but empty)
        if isinstance(tstamp_mdata, dict):
            tmin = tstamp_mdata.get("min")
            tmax = tstamp_mdata.get("max")
            # Ensure both keys exist
            if tstamp_mdata.get("min") is None != tmax is None:
                logger.error(
                    f"Wrong format of 'timestamp' field: 'min' or 'max' are None, but not both, {tstamp_mdata}"
                )

        return tmin, tmax


@dataclass(frozen=True)
class SequenceResourceManifest:
    """
    Metadata container for a specific data topic resource.

    This class acts as a Value Object, standardizing topic and sequence
    identifiers extracted from Arrow Flight transport layers. Being 'frozen'
    ensures the metadata remains immutable and hashable throughout its lifecycle.

    Attributes:
        resource_locator (str): The standardized name of the resource sequence.
        resource_info (SequenceResourceInfo): The resource info of the sequence.
    """

    resource_locator: str
    resource_info: SequenceResourceInfo

    @classmethod
    def _from_app_metadata(
        cls,
        app_mdata: Union[bytes, str],
    ) -> "SequenceResourceManifest":
        """
        Factory method to create a SequenceResourceManifest from FlightInfo.app_metadata.

        Args:
            app_mdata: The app_metadata object containing the sequence resource info.

        Returns:
            SequenceResourceManifest: An immutable instance containing parsed data.

        Raises:
            TopicParsingError: If the endpoint has no locations, multiple
                locations, or if the URI format is invalid.
        """

        try:
            # Parse and return the app_metadata fields
            mdata = cls._decode_app_metadata(app_mdata)
            # If not possible to parse the app_metadata, raise
            if mdata is None:
                raise SequenceParsingError("Failed to parse app_metadata")

            resource_locator = mdata.get("resource_locator")
            if resource_locator is None:
                raise SequenceParsingError(
                    "Missing required fields in 'app_metadata' field. Returning 'invalid'."
                )

            return cls(
                resource_locator=resource_locator,
                resource_info=SequenceResourceInfo._from_app_metadata(mdata),
            )

        except Exception as e:
            # Wrap internal errors (like UnicodeDecode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise SequenceParsingError(
                f"Failed to parse metadata from endpoint: {e}"
            ) from e

    @staticmethod
    def _decode_app_metadata(
        app_mdata: Union[bytes, str],
    ) -> Optional[Dict[str, Any]]:
        """
        Decodes and validates the raw App Metadata JSON payload.

        Args:
            app_mdata: JSON payload as a UTF-8 string or byte sequence.

        Returns:
            Tuple: (timestamp_ns_min, timestamp_ns_max, PlatformResourceInfo(...))

        Raises:
            SequenceParsingError: If JSON is malformed or missing required schema keys.
        """
        # Decode input to string
        try:
            raw_str = (
                app_mdata.decode("utf-8") if isinstance(app_mdata, bytes) else app_mdata
            )
        except UnicodeDecodeError as e:
            logger.error(
                SequenceParsingError(f"App metadata bytes are not UTF-8, err '{e}'")
            )
            return None

        # Check empty-string
        if not raw_str:
            logger.error(SequenceParsingError("Empty app_metadata"))
            return None

        # Safely load into JSON
        try:
            data = json.loads(raw_str)
        except json.JSONDecodeError as e:
            logger.error(
                SequenceParsingError(f"Invalid JSON in app_metadata, err: '{e}'")
            )
            return None

        # Validate format
        if not isinstance(data, dict):
            logger.error(
                SequenceParsingError(f"Expected JSON object, got {type(data).__name__}")
            )
            return None

        return data
