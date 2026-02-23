"""
Message Envelope Module.

This module defines the `Message` class, which acts as the transport envelope
for all ontology data. It wraps the specific ontology payload (`data`) with
middleware-level metadata (like recording timestamp_ns).

"""

# --- Python Standard Library Imports ---
from typing import Any, Dict, Optional, Type, TypeVar
from mosaicolabs.models.header import Header, Time
from pydantic import PrivateAttr
import pyarrow as pa
import pandas as pd


from ..logging_config import get_logger
from ..helpers.helpers import encode_to_dict
from .serializable import Serializable
from .internal.helpers import _fix_empty_dicts
from .base_model import BaseModel

# Set the hierarchical logger
logger = get_logger(__name__)


def _make_schema(*args: pa.StructType) -> pa.Schema:
    """Helper to merge multiple PyArrow structs into a single Schema."""
    return pa.schema([field for struct in args for field in struct])


TSerializable = TypeVar("TSerializable", bound="Serializable")


class Message(BaseModel):
    """
    The universal transport envelope for Mosaico data.

    The `Message` class wraps a polymorphic [`Serializable`][mosaicolabs.models.Serializable]
    payload with middleware metadata, such as recording timestamps and headers.

    Attributes:
        timestamp_ns: Message/Sensor acquisition timestamp in nanoseconds (resambles the data ontology high precision time header).
        data: The actual ontology data payload (e.g., an IMU or GPS instance).
        recording_timestamp_ns: Recording timestamp in nanoseconds. This is the timestamp in which the message was recorded in the receiving store file (like rosbags, parquet files, etc.), different from sensor acquisition time.

    """

    # Define the Message schema (Envelope fields only)
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "timestamp_ns",
                pa.int64(),
                nullable=False,
                metadata={
                    "description": "Message/Sensor acquisition timestamp in nanoseconds (resambles the data ontology high precision time header)."
                },
            ),
            pa.field(
                "recording_timestamp_ns",
                pa.int64(),
                nullable=True,
                metadata={
                    "description": "Recording timestamp in nanoseconds (different from sensor acquisition time)."
                },
            ),
        ]
    )

    data: Serializable
    """The actual ontology data payload (e.g., an IMU or GPS instance)."""

    timestamp_ns: Optional[int] = None
    """
    Message/Sensor acquisition timestamp in nanoseconds (resambles the data ontology high precision time header).

    Can be omitted if the data ontology already contains the timestamp (e.g. `data.header.stamp`) or the `recording_timestamp_ns` is set.
    If all timestamps data are None, the message will be rejected and a `ValueError` is raised.
    """

    recording_timestamp_ns: Optional[int] = None
    """
    Recording timestamp in nanoseconds (different from sensor acquisition time).
    
    This is the timestamp in which the message was recorded in the receiving store file
    (like rosbags, parquet files, etc.)
    """

    # Internal cache for efficient field separation during encoding
    _self_model_keys: set[str] = PrivateAttr(default_factory=set)
    _data_model_keys: set[str] = PrivateAttr(default_factory=set)

    def model_post_init(self, context: Any) -> None:
        """
        Validates the message structure after initialization.

        Ensures that there are no field name collisions between the envelope
        (e.g., `timestamp_ns`) and the data payload.
        """
        super().model_post_init(context)
        data_header: Optional[Header] = getattr(self.data, "header", None)
        timestamp = (
            self.timestamp_ns  # try setting the timestamp from the `timestamp_ns` field
            if self.timestamp_ns is not None
            else data_header.stamp.to_nanoseconds()  # try setting the timestamp from the data header
            if data_header is not None
            else self.recording_timestamp_ns  # try setting the timestamp from the `recording_timestamp_ns` field
        )
        if timestamp is None:
            raise ValueError(
                "Timestamp data is needed. It must be set in data ontology header, OR in `Message.timestamp_ns` OR in `Message.recording_timestamp_ns`."
            )
        # Set the timestamp
        self.timestamp_ns = timestamp

        # Assign the correct timestamp to the data header (if it does not provide one)
        if data_header is None:
            self.data.header = Header(stamp=Time.from_nanoseconds(timestamp))

        self._self_model_keys = {
            field for field in self.__class__.model_fields if field != "data"
        }
        self._data_model_keys = {field for field in self.data.__class__.model_fields}

        colliding_fields = self._self_model_keys & self._data_model_keys
        if colliding_fields:
            raise ValueError(
                f"Fields name collision detected between class '{type(self.data).__name__}' "
                f"and Message envelope. Colliding fields: {colliding_fields}."
            )

    def ontology_type(self) -> Type[Serializable]:
        """Retrieves the class type of the ontology object stored in the `data` field."""
        return self.data.__class_type__

    def ontology_tag(self) -> str:
        """Returns the unique ontology tag name associated with the object in the data field."""
        return getattr(
            self.data, "__ontology_tag__"
        )  # avoid the IDE complaining (__ontology_tag__ defined as Optional but surely not None at this point)

    def _encode(self) -> Dict[str, Any]:
        """
        Flattens the message and its payload into a dictionary for serialization.

        This merges envelope fields and data fields into a single flat structure
        compatible with PyArrow serialization.

        Returns:
            A dictionary containing all flattened message and payload data.
        """
        # Encode envelope fields
        columns_dict = {
            field: encode_to_dict(getattr(self, field))
            for field in self._self_model_keys
        }

        # Encode and merge payload fields
        columns_dict.update(
            {
                field: encode_to_dict(getattr(self.data, field))
                for field in self._data_model_keys
            }
        )

        return columns_dict

    @classmethod
    def _create(cls, tag: str, **kwargs) -> "Message":
        """
        Factory method to create a Message and its specific ontology payload.

        This method separates the provided keyword arguments into envelope-level
        fields and payload-level fields based on the registered ontology tag.

        Args:
            tag: The registered ontology identifier (e.g., "imu").
            **kwargs: A dictionary containing all required fields for both the
                message and the data object.

        Returns:
            A fully populated `Message` instance.

        Raises:
            ValueError: If the tag is not registered.
            Exception: If required message fields are missing from `kwargs`.
        """
        # Validate Tag
        DataClass = Serializable._get_class_type(tag)
        if DataClass is None:
            raise ValueError(
                f"No ontology registered with tag '{tag}'. "
                f"Available tags: {Serializable._list_registered()}"
            )

        # Cleanup Input (Fix Parquet artifacts)
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else dict({})
        if not fixed_kwargs:
            raise Exception(f"Unable to obtain valid fields from kwargs: {kwargs}")

        # Argument Separation
        data_fields = list(DataClass.model_fields.keys())

        # Extract Envelope args
        message_kwargs = {
            key: val for key, val in fixed_kwargs.items() if key not in data_fields
        }
        if not message_kwargs:
            raise Exception("Input kwargs missing required Message fields.")

        # Extract Payload args
        data_kwargs = {
            key: val for key, val in fixed_kwargs.items() if key in data_fields
        }

        # Instantiation
        data_obj = DataClass(**data_kwargs)
        return cls(data=data_obj, **message_kwargs)

    @classmethod
    def _get_schema(cls, data_cls: Type["Serializable"]) -> pa.Schema:
        """
        Generates a combined PyArrow Schema for the message and a specific ontology.

        Args:
            data_cls: The specific `Serializable` subclass type.

        Returns:
            A combined PyArrow Schema including both envelope and payload fields.

        Raises:
            ValueError: If field name collisions are detected in the schema.
        """
        # Collision check
        colliding_keys = set(cls.__msco_pyarrow_struct__.names) & set(
            data_cls.__msco_pyarrow_struct__.names
        )
        if colliding_keys:
            raise ValueError(
                f"Class '{data_cls.__name__}' schema collides with Message schema: {list(colliding_keys)}"
            )

        return _make_schema(
            cls.__msco_pyarrow_struct__,
            data_cls.__msco_pyarrow_struct__,
        )

    # --- Public API ---

    def get_data(self, target_type: Type[TSerializable]) -> TSerializable:
        """
        Safe, type-hinted accessor for the data payload.

        Args:
            target_type: The expected `Serializable` subclass type.

        Returns:
            The data object cast to the requested type.

        Raises:
            TypeError: If the actual data type does not match the requested `target_type`.

        Example:
            ```python
            # Get the IMU data from the message
            image_data = message.get_data(Image)
            print(f"Message time: {message.timestamp_ns}: Sensor time: {image_data.header.stamp.to_nanoseconds()}")
            print(f"Message time: {message.timestamp_ns}: Image size: {image_data.height}x{image_data.width}")
            # Show the image
            image_data.to_pillow().show()

            # Get the Floating64 data from the message
            floating64_data = message.get_data(Floating64)
            print(f"Message time: {message.timestamp_ns}: Data time: {floating64_data.header.stamp.to_nanoseconds()}")
            print(f"Message time: {message.timestamp_ns}: Data value: {floating64_data.data}")
            ```
        """
        if not isinstance(self.data, target_type):
            raise TypeError(
                f"Message data is type '{type(self.data).__name__}', "
                f"but '{target_type.__name__}' was requested."
            )
        return self.data

    @staticmethod
    def from_dataframe_row(
        row: pd.Series, topic_name: str, timestamp_column_name: str = "timestamp_ns"
    ) -> Optional["Message"]:
        """
        Reconstructs a `Message` object from a flattened DataFrame row.

        In the Mosaico Data Platform, DataFrames represent topics using a nested naming
        convention: `{topic}.{tag}.{field}`. This method performs
        **Smart Reconstruction** by:

        1. **Topic Validation**: Verifying if any columns associated with the `topic_name`
           exist in the row.
        2. **Tag Inference**: Inspecting the column headers to automatically determine
           the original ontology tag (e.g., `"imu"`).
        3. **Data Extraction**: Stripping prefixes and re-nesting the flat columns
           into their original dictionary structures.
        4. **Type Casting**: Re-instantiating the specific [`Serializable`][mosaicolabs.models.Serializable]
           subclass and wrapping it in a `Message` envelope.

        Args:
            row: A single row from a Pandas DataFrame, representing a point in time
                across one or more topics.
            topic_name: The name of the specific topic to extract from the row.
            timestamp_column_name: The name of the column containing the timestamp.

        Returns:
            A reconstructed `Message` instance containing the typed ontology data,
                or `None` if the topic is not present or the data is incomplete.

        Example:
            ```python
            # Obtain a dataframe with DataFrameExtractor
            from mosaicolabs import MosaicoClient, IMU, Image
            from mosaicolabs.ml import DataFrameExtractor, SyncTransformer

            with MosaicoClient.connect("localhost", 6726) as client:
                sequence_handler = client.get_sequence_handler("example_sequence")
                for df in DataFrameExtractor(sequence_handler).to_pandas_chunks(
                    topics = ["/front/imu", "/front/camera/image_raw"]
                ):
                    # Do something with the dataframe.
                    # For example, you can sync the data using the `SyncTransformer`:
                    sync_transformer = SyncTransformer(
                        target_fps = 30, # resample at 30 Hz and fill the Nans with a Hold policy
                    )
                    synced_df = sync_transformer.transform(df)

                    # Reconstruct the image message from a dataframe row
                    image_msg = Message.from_dataframe_row(synced_df, "/front/camera/image_raw")
                    image_data = image_msg.get_data(Image)
                    # Show the image
                    image_data.to_pillow().show()
                    # ...
            ```
        """

        # Topic Presence Check
        # Check if any columns belonging to this topic exist in the row
        topic_prefix = f"{topic_name}."
        if not any(str(col).startswith(topic_prefix) for col in row.index):
            return None  # Topic not present in this DataFrame

        # Tag Inference Logic
        tag = None
        for col in row.index:
            col_str = str(col)
            if col_str.startswith(topic_prefix) and col_str != timestamp_column_name:
                parts = col_str.split(".")
                # Semantic Naming check: {topic}.{tag}.{field}
                if len(parts) >= 3:
                    tag = parts[1]
                    break

        # Tag Check
        # If tag remains None after inference attempt there was something wrong when creating the dataframe
        if tag is None:
            # This should never happen
            raise ValueError(
                f"Ontology tag for topic '{topic_name}' could not be inferred."
            )

        # Define extraction prefix based on Tag presence
        # Fallback to Clean Mode if inference failed but topic columns exist
        prefix = f"{topic_name}.{tag}."

        # Extract relevant data with Pylance fix
        relevant_data = {
            str(col)[len(prefix) :]: val
            for col, val in row.items()
            if str(col).startswith(prefix)
        }

        # If the prefix matched nothing (e.g., mismatch between inferred tag and actual data)
        if not relevant_data:
            return None

        # Reconstruct the Nested Dictionary
        # Ensure timestamp_ns is present; usually a global column in Mosaico DFs
        timestamp = row.get(timestamp_column_name)
        if timestamp is None or pd.isna(timestamp):
            return None

        nested_data: Dict[str, Any] = {timestamp_column_name: int(timestamp)}

        for key, value in relevant_data.items():
            # Convert Pandas/NumPy NaNs to Python None for model compatibility
            processed_value = None if pd.isna(value) else value

            parts = key.split(".")
            d = nested_data
            for part in parts[:-1]:
                d = d.setdefault(part, {})
            d[parts[-1]] = processed_value

        # Final Message Creation
        try:
            # Reconstructs the strongly-typed Ontology object from flattened rows
            return Message._create(tag=tag, **nested_data)
        except Exception as e:
            logger.error(f"Failed to reconstruct Message for topic {topic_name}: {e}")
            return None
