from abc import ABC, abstractmethod
from typing import ClassVar, Tuple

import pyarrow as pa

from mcap.records import Schema


class SchemaConverter(ABC):
    """
    TODO: explain that this is the abstract class that each subclass should implement
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
        """Abstract function that should be implemented by each subclass"""
        ...

    @classmethod
    def is_encoding_supported(cls, encoding: str):
        """Checks that encoding is within supported ones"""

        return encoding in cls.SUPPORTED_ENCODINGS

    @classmethod
    def to_pyarrow(cls, mcap_schema: Schema) -> pa.StructType:
        """Concrete: validates, then delegates to the subclass's _convert."""

        if not cls.is_encoding_supported(mcap_schema.encoding):
            raise ValueError(
                f"{cls.__name__} does not support {mcap_schema.encoding} encoding. "
                f"Supported encodings are {cls.SUPPORTED_ENCODINGS}"
            )

        return cls._convert(mcap_schema)
