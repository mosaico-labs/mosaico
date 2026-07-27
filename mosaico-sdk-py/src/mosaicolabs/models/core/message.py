"""
Message Envelope Module.

This module defines the `Message` class, which acts as the transport envelope
for all ontology data. It wraps the specific ontology payload (`data`) with
middleware-level metadata (like recording timestamp_ns).

"""

# --- Python Standard Library Imports ---
import warnings
from collections import defaultdict
from typing import Any, Dict, Optional, Type, TypeVar, Union

import pandas as pd
import pyarrow as pa
from pydantic import PrivateAttr

from ...logging_config import get_logger
from .base_model import BaseModel
from .internal.helpers import encode_to_dict
from .serializable import Serializable

# Set the hierarchical logger
logger = get_logger(__name__)


def _make_schema(*args: pa.StructType) -> pa.Schema:
    """Helper to merge multiple PyArrow structs into a single Schema."""
    return pa.schema([field for struct in args for field in struct])


TSerializable = TypeVar("TSerializable", bound="Serializable")


class Message(BaseModel):
    """
    The universal transport envelope for Mosaico data.

    The `Message` class wraps a polymorphic [`Serializable`][mosaicolabs.models.core.Serializable]
    payload with its ingestion timestamps (record time).

    Attributes:
        timestamp_ns: Ingestion timestamp in nanoseconds (record time).
            This represents the time at which the message was received and persisted by
            the recording system (e.g., rosbag, parquet writer, logging pipeline, or database).
        data: The actual ontology data payload (e.g., an IMU or GPS instance).

    ### Querying with the **`.Q` Proxy** {: #queryability }
    When constructing a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog],
    the `Message` attributes are fully queryable.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.timestamp_ns` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Note: Universal Compatibility
        The `<Model>` placeholder represents any Mosaico ontology class (e.g., `IMU`, `GPS`, `Floating64`)
        or any custom user-defined class that is a subclass of [`Serializable`][mosaicolabs.models.core.Serializable].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, Floating64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.timestamp_ns.lt(1770282868))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

        ```
    """

    # Define the Message schema (Envelope fields only)
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "timestamp_ns",
                pa.int64(),
                nullable=False,
                metadata={
                    "description": "Ingestion timestamp in nanoseconds (record time)."
                },
            ),
        ]
    )

    data: Serializable
    """The actual ontology data payload (e.g., an IMU or GPS instance)."""

    timestamp_ns: int
    """
    Ingestion timestamp in nanoseconds (record time).

    This represents the time at which this message was received and
    persisted by the recording system (e.g., rosbag, parquet writer,
    logging pipeline, or database).

    This timestamp reflects infrastructure timing and may include:

    - transport delay
    - middleware delay
    - serialization/deserialization delay
    - scheduling delay

    It does NOT represent when the sensor measurement occurred.

    Typical usage:

    - latency measurement
    - debugging transport or pipeline delays
    - ordering messages by arrival time

    ### Querying with the **`.Q` Proxy**
    The timestamp_ns field is queryable using the `.Q` proxy.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<Model>.Q.timestamp_ns` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    The `<Model>` placeholder represents any Mosaico ontology class (e.g., `IMU`, `GPS`, `Floating64`)
    or any custom user-defined class that is a subclass of [`Serializable`][mosaicolabs.models.core.Serializable]
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific recording second
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.timestamp_ns.lt(1770282868))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    # Internal cache for efficient field separation during encoding
    _self_model_keys: set[str] = PrivateAttr(default_factory=set)

    def model_post_init(self, context: Any) -> None:
        """
        Validates the message structure after initialization.

        Ensures that there are no field name collisions between the envelope
        (e.g., `timestamp_ns`) and the data payload.
        """
        super().model_post_init(context)
        self._self_model_keys = Message._message_model_fields()

        colliding_fields = set(self.__msco_pyarrow_struct__.names) & set(
            self.data.__msco_pyarrow_struct__.names
        )
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
        columns_dict.update(self.data._encode())

        return columns_dict

    @classmethod
    def _decode(
        cls,
        tag_or_type: Union[str, Type[Serializable]],
        **msg_data_kwargs,
    ) -> "Message":
        """
        Factory method to create a Message and its specific ontology payload.

        This method separates the provided keyword arguments into envelope-level
        fields and payload-level fields based on the registered ontology tag.

        Args:
            tag: The registered ontology identifier (e.g., "imu").
            **msg_data_kwargs: A dictionary containing all required fields for both the
                message and the data object.

        Returns:
            A fully populated `Message` instance.

        Raises:
            ValueError: If the tag is not registered.
            Exception: If required message fields are missing from `msg_data_kwargs`.
        """
        # Validate Tag
        DataClass = (
            Serializable._get_class_type(tag_or_type)  # It's the ontology tag
            if isinstance(tag_or_type, str)
            else tag_or_type  # It's a type
        )
        # Check if this ontology tag is wrapped by an ontology model class.
        if DataClass is None:
            raise ValueError(
                f"No ontology registered with tag '{tag_or_type}'. "
                f"Available tags: {Serializable._list_registered()}. "
            )

        if not msg_data_kwargs:
            raise Exception(
                f"Unable to obtain valid fields from kwargs: {msg_data_kwargs}"
            )

        msg_model_fields = cls._message_model_fields()
        # Extract Envelope args
        message_kwargs = {
            key: val for key, val in msg_data_kwargs.items() if key in msg_model_fields
        }
        if not message_kwargs:
            raise Exception("Input kwargs missing required Message fields.")

        # Extract Payload args
        data_kwargs = {
            key: val
            for key, val in msg_data_kwargs.items()
            if key not in msg_model_fields
        }

        # Instantiation
        data_obj = DataClass._decode(**data_kwargs)
        return cls(data=data_obj, **message_kwargs)

    @classmethod
    def _message_model_fields(cls):
        return {name for name in cls.model_fields.keys() if name != "data"}

    @classmethod
    def _extract_data_schema(cls, schema: pa.Schema) -> pa.StructType:
        return pa.struct(
            field for field in schema if field.name not in cls._message_model_fields()
        )

    @classmethod
    def _get_schema(
        cls,
        cls_or_schema: Union[
            Type["Serializable"],
            pa.StructType,
        ],
    ) -> pa.Schema:
        """
        Generates a combined PyArrow Schema for the message and a specific ontology.

        Args:
            cls_or_schema: The specific `Serializable` subclass type or the pyarrow schema of the data.

        Returns:
            A combined PyArrow Schema including both envelope and payload fields.

        Raises:
            ValueError: If field name collisions are detected in the schema.
        """
        # Collision check
        data_schema = (
            cls_or_schema
            if isinstance(cls_or_schema, pa.StructType)
            else cls_or_schema.__msco_pyarrow_struct__
        )
        colliding_keys = set(cls.__msco_pyarrow_struct__.names) & set(data_schema.names)
        if colliding_keys:
            raise ValueError(
                f"Class schema collides with Message schema: {list(colliding_keys)}"
            )

        return _make_schema(
            cls.__msco_pyarrow_struct__,
            data_schema,
        )

    # --- Public API ---

    def get_data(self, target_type: Type[TSerializable]) -> Optional[TSerializable]:
        """
        Safe, type-hinted accessor for the data payload.

        Args:
            target_type: The expected `Serializable` subclass type.

        Returns:
            The data object cast to the requested type or None if cannot be casted.

        Example:
            ```python
            # Get the IMU data from the message
            image_data = message.get_data(Image)
            print(f"Timestamp: {message.timestamp_ns}")
            print(f"Image size: {image_data.height}x{image_data.width}")
            # Show the image
            image_data.to_pillow().show()

            # Get the Floating64 data from the message
            floating64_data = message.get_data(Floating64)
            print(f"Timestamp: {message.timestamp_ns}")
            print(f"Data value: {floating64_data.data}")
            ```
        """
        if not isinstance(self.data, target_type):
            return None
        return self.data

    @staticmethod
    def _process_value(value):
        """
        Process a value to handle None values.

        Args:
            value: The value to process.

        Returns:
            The processed value.
        """
        import numpy as np

        # Handle list / tuple / ndarray
        if isinstance(value, (list, tuple, np.ndarray)):
            return [None if pd.isna(v) else v for v in value]
        # Handle pandas scalar / normal scalar
        try:
            return None if pd.isna(value) else value
        except Exception:
            # Fallback: Return the value as is
            return value

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
        4. **Type Casting**: Re-instantiating the specific [`Serializable`][mosaicolabs.models.core.Serializable]
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
                sequence_handler = client.sequence_handler("example_sequence")
                for df in DataFrameExtractor(sequence_handler).to_pandas_chunks(
                    topics = ["/front/imu", "/front/camera/image_raw"]
                ):
                    # Do something with the dataframe.
                    # e.g. reconstruct the image message from a dataframe row
                    image_msg = Message.from_dataframe_row(
                        row=df, topic_name="/front/camera/image_raw"
                    )
                    image_data = image_msg.get_data(Image)
                    # Show the image
                    image_data.to_pillow().show()
                    # ...
            ```
        """

        warnings.warn(
            "This classmethod is still not supported and can be deprecated.",
            category=PendingDeprecationWarning,
            stacklevel=2,
        )

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
            processed_value = Message._process_value(value)

            parts = key.split(".")
            d = nested_data
            for part in parts[:-1]:
                d = d.setdefault(part, {})
            d[parts[-1]] = processed_value

        # Final Message Creation
        try:
            # Reconstructs the strongly-typed Ontology object from flattened rows
            return Message._decode(tag_or_type=tag, **nested_data)
        except Exception as e:
            logger.error(f"Failed to reconstruct Message for topic {topic_name}: {e}")
            return None

    def _to_pa_record_batch(self) -> pa.RecordBatch:
        """
        Serializes the message instance into a PyArrow RecordBatch.

        This method encodes the message fields into a dictionary-based structure
        and wraps them in a single-row RecordBatch using the schema defined
        for the specific ontology type.

        Returns:
            pa.RecordBatch: A single-row PyArrow RecordBatch containing the encoded message data."""

        result = defaultdict(list)
        for k, v in self._encode().items():
            result[k].append(v)

        return pa.RecordBatch.from_pydict(
            dict(result),
            schema=self._get_schema(self.ontology_type()),
        )

    @classmethod
    def _from_pa_record_batch(cls, rb: pa.RecordBatch, tag: str) -> "Message":
        """
        Reconstructs a Message instance from a PyArrow RecordBatch.

        This factory method extracts data from the first row of a RecordBatch
        and instantiates the appropriate message type.

        Args:
            rb (pa.RecordBatch): The PyArrow RecordBatch containing the message data.
                Must contain exactly one row.
            tag (str): The ontology tag of the message data into the RecordBatch.

        Returns:
            Message: A concrete instance of a Message or its subclasses.

        Raises:
            ValueError: If the RecordBatch does not contain exactly one row.
            ValueError: If the provided `tag` is empty or None.
        """
        if len(rb) != 1:
            raise ValueError(
                f"_from_pa_record_batch expects a single-row RecordBatch, got {len(rb)} rows."
            )

        if not tag:
            raise ValueError("Tag must be a valid value.")

        flat = {col: rb.column(col)[0].as_py() for col in rb.column_names}
        return cls._decode(tag_or_type=tag, **flat)
