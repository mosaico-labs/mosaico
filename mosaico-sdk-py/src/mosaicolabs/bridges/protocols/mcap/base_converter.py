from abc import ABC, abstractmethod
from typing import ClassVar, Tuple

import pyarrow as pa

from mcap.records import Schema


class McapSchemaConverter(ABC):
    """Base class all MCAP schema converters (jsonschema, protobuf, ...) inherit from
    to convert their schema content into PyArrow types.

    Each subclass must declare a non-empty ``SUPPORTED_ENCODINGS`` (enforced by
    ``__init_subclass__``) and override ``_convert()``, which is invoked by
    ``to_pyarrow()`` to produce the resulting PyArrow struct.
    """

    SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Check that SUPPORTED_ENCODINGS exists and is not empty
        if (
            not getattr(cls, "SUPPORTED_ENCODINGS", None)
            and not cls.SUPPORTED_ENCODINGS
        ):
            raise TypeError(
                f"{cls.__name__} must defined a non-empty SUPPORTED_ENCODINGS"
            )

    @classmethod
    @abstractmethod
    def _convert(cls, mcap_schema: Schema) -> pa.StructType:
        """Abstract: subclasses convert ``mcap_schema`` into an equivalent ``pa.StructType``."""
        ...

    @classmethod
    def is_encoding_supported(cls, encoding: str):
        """Checks whether ``encoding`` is one of this converter's ``SUPPORTED_ENCODINGS``.

        Args:
            encoding: The MCAP schema encoding string to check (e.g. ``"protobuf"``).

        Returns:
            ``True`` if ``encoding`` is supported by this converter, ``False`` otherwise.
        """

        return encoding in cls.SUPPORTED_ENCODINGS

    @classmethod
    def to_pyarrow(cls, mcap_schema: Schema) -> pa.StructType:
        """Concrete: validates, then delegates to the subclass's _convert().

        Args:
            mcap_schema: The MCAP ``Schema`` record to convert.

        Returns:
            A ``pa.StructType`` mirroring the schema's fields, as produced by ``_convert()``.

        Raises:
            ValueError: If ``mcap_schema.encoding`` is not in ``cls.SUPPORTED_ENCODINGS``.
        """

        if not cls.is_encoding_supported(mcap_schema.encoding):
            raise ValueError(
                f"{cls.__name__} does not support {mcap_schema.encoding} encoding. "
                f"Supported encodings are {cls.SUPPORTED_ENCODINGS}"
            )

        return cls._convert(mcap_schema)
