from abc import ABC, abstractmethod
from typing import ClassVar, Generic, Tuple, Type, TypeVar

import pyarrow as pa

from mcap.records import Schema

NativeType = TypeVar("NativeType")


class McapSchemaConverter(ABC, Generic[NativeType]):
    """Base class all MCAP schema converters (jsonschema, protobuf, ...) inherit from
    to convert their schema content into PyArrow types.

    Each subclass must declare a non-empty ``SUPPORTED_SCHEMA_ENCODINGS`` (enforced by
    ``__init_subclass__``) and override ``_convert()``, which is invoked by
    ``to_pyarrow()`` to produce the resulting PyArrow struct.
    """

    SUPPORTED_SCHEMA_ENCODINGS: ClassVar[Tuple[str, ...]]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Check that SUPPORTED_SCHEMA_ENCODINGS exists and is not empty
        if (
            not getattr(cls, "SUPPORTED_SCHEMA_ENCODINGS", None)
            and not cls.SUPPORTED_SCHEMA_ENCODINGS
        ):
            raise TypeError(
                f"{cls.__name__} must defined a non-empty SUPPORTED_SCHEMA_ENCODINGS"
            )

    @classmethod
    @abstractmethod
    def _convert(cls, mcap_schema: Schema) -> pa.StructType:
        """Abstract: subclasses convert ``mcap_schema`` into an equivalent ``pa.StructType``."""
        ...

    @classmethod
    def is_encoding_supported(cls, encoding: str):
        """Checks whether ``encoding`` is one of this converter's ``SUPPORTED_SCHEMA_ENCODINGS``.

        Args:
            encoding: The MCAP schema encoding string to check (e.g. ``"protobuf"``).

        Returns:
            ``True`` if ``encoding`` is supported by this converter, ``False`` otherwise.
        """

        return encoding in cls.SUPPORTED_SCHEMA_ENCODINGS

    @classmethod
    def to_pyarrow(cls, mcap_schema: Schema) -> pa.StructType:
        """Concrete: validates, then delegates to the subclass's _convert().

        Args:
            mcap_schema: The MCAP ``Schema`` record to convert.

        Returns:
            A ``pa.StructType`` mirroring the schema's fields, as produced by ``_convert()``,
            with a nullable ``publish_time_ns`` (int64) field appended. An empty struct (no
            message definition could be derived) is returned unchanged, without
            ``publish_time_ns``, so that callers can still detect and reject it.

        Raises:
            ValueError: If ``mcap_schema.encoding`` is not in ``cls.SUPPORTED_SCHEMA_ENCODINGS``.
        """

        if not cls.is_encoding_supported(mcap_schema.encoding):
            raise ValueError(
                f"{cls.__name__} does not support {mcap_schema.encoding} encoding. "
                f"Supported encodings are {cls.SUPPORTED_SCHEMA_ENCODINGS}"
            )

        # PyArrow struct of the message
        out = cls._convert(mcap_schema)

        if len(out) == 0:
            return out

        # Add to PyArrow struct a field representing the publish_time_ns
        out_with_time = pa.struct(
            list(out) + [pa.field("publish_time_ns", pa.int64(), nullable=True)]
        )

        return out_with_time

    @staticmethod
    @abstractmethod
    def stringify_schema_def(schema_def: bytes) -> str:
        """Converts `schema_def` (coming from schema.data) bytes into a JSON-safe string that
        `destringify_schema_def` can turn back into the exact original bytes."""

    @staticmethod
    @abstractmethod
    def destringify_schema_def(schema_def_str: str) -> bytes:
        """Inverse of `stringify_schema_def`: recovers the exact original schema_def bytes
        (representing `schema.data`)."""

    @classmethod
    @abstractmethod
    def get_schema_class(cls, schema_name: str, schema_def: bytes) -> Type[NativeType]:
        """Generic function that, given a specific schema name and schema definition, returns
        the Python object type containing the data. Such type depends on the decoder's encoding
        (i.e. for jsonschema they are always Dict while for protobuf they are created from their
        associated `.proto` file)"""
