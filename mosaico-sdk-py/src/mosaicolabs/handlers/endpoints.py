from dataclasses import dataclass
import json
from typing import Tuple, TYPE_CHECKING, Union

from ..helpers.helpers import unpack_topic_full_path

# Use TYPE_CHECKING to avoid circular imports or heavy dependencies at runtime
if TYPE_CHECKING:
    from pyarrow.flight import FlightEndpoint


class TopicParsingError(Exception):
    """Raised when TopicResourceMetadata cannot be extracted from an endpoint."""

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
    """

    @dataclass(frozen=True)
    class _TopicAppMetadata:
        """Internal container for application-specific metadata."""

        tmin_ns: int
        tmax_ns: int
        tstart_ns: int
        tend_ns: int

    topic_name: str
    sequence_name: str
    timestamp_ns_min: int
    timestamp_ns_max: int
    timestamp_ns_start: int
    timestamp_ns_end: int

    @classmethod
    def from_flight_endpoint(
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
            try:
                # FIXME: This won't go until updates in backend (Tests will fail)
                topic_app_mdata = cls._parse_app_metadata(endpoint.app_metadata)
            except Exception:
                topic_app_mdata = cls._TopicAppMetadata(0, 0, 0, 0)

            return cls(
                topic_name=topic_name,
                sequence_name=seq_name,
                timestamp_ns_min=topic_app_mdata.tmin_ns,
                timestamp_ns_max=topic_app_mdata.tmax_ns,
                timestamp_ns_start=topic_app_mdata.tstart_ns,
                timestamp_ns_end=topic_app_mdata.tend_ns,
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
            raise ValueError(f"URI missing required 'mosaico:' prefix: {decoded_uri}")

        # Path Extraction
        path = decoded_uri.removeprefix("mosaico:")

        # Domain-specific unpacking (expects a tuple of strings)
        result = unpack_topic_full_path(path)

        if not result or len(result) != 2:
            raise ValueError(f"Path '{path}' is not a valid sequence/topic pair.")

        return result

    @staticmethod
    def _parse_app_metadata(
        app_mdata: Union[bytes, str],
    ) -> "TopicResourceManifest._TopicAppMetadata":
        """
        Decodes and validates the raw App Metadata JSON payload.

        Args:
            app_mdata: JSON payload as a UTF-8 string or byte sequence.

        Returns:
            _TopicAppMetadata: Validated internal metadata object.

        Raises:
            TopicParsingError: If JSON is malformed or missing required schema keys.
        """
        # Normalize input to string and safely load as JSON
        try:
            raw_str = (
                app_mdata.decode("utf-8") if isinstance(app_mdata, bytes) else app_mdata
            )
        except UnicodeDecodeError as e:
            raise TopicParsingError(f"App metadata bytes are not UTF-8: {e}")
        try:
            data = json.loads(raw_str)
        except json.JSONDecodeError as e:
            raise TopicParsingError(f"Invalid JSON in app_metadata: {e}")

        if not isinstance(data, dict):
            raise TopicParsingError(f"Expected JSON object, got {type(data).__name__}")

        # Schema Validation
        # Using .get() with a default empty dict allows for cleaner nested access
        tstamp_data = data.get("timestamp", {})
        if not isinstance(tstamp_data, dict):
            raise TopicParsingError("'timestamp' field must be an object.")

        tmin = tstamp_data.get("min")
        tmax = tstamp_data.get("max")
        tstart = tstamp_data.get("start")
        tend = tstamp_data.get("end")

        # Ensure both keys exist and are integers (adjust type check if they are floats)
        if not isinstance(tmin, int) or not isinstance(tmax, int):
            raise TopicParsingError(
                f"App metadata missing required integer 'min'/'max' in: {tstamp_data}"
            )
        # Ensure both keys exist and are integers (adjust type check if they are floats)
        if not isinstance(tstart, int) or not isinstance(tend, int):
            raise TopicParsingError(
                f"App metadata missing required integer 'start'/'end' in: {tstamp_data}"
            )

        return TopicResourceManifest._TopicAppMetadata(
            tmin_ns=tmin, tmax_ns=tmax, tstart_ns=tstart, tend_ns=tend
        )
