from typing import ClassVar, Dict, Optional, Type, TypeVar

from mosaicolabs.bridges.protocols.mcap.base_converter import McapSchemaConverter

T = TypeVar("T", bound=McapSchemaConverter)


class McapSchemaRegistry:
    """Registry holding all the supported MCAP->PyArrow converters.

    Maps the encoding string to its converter class, as follows:

    {
        "protobuf": ProtobufConverter,
        "flatbuffer": FlatBufferConverter,
        ...
    }

    To register a new converter, use the ``@McapSchemaRegistry.register`` decorator
    to include it in the global registry.
    """

    _registry: ClassVar[Dict[str, Type[McapSchemaConverter]]] = {}

    @classmethod
    def _register_converter(cls, converter_cls: Type[T]):
        """Adds ``converter_cls`` to the registry under each of its supported encodings.

        Args:
            converter_cls: The ``McapSchemaConverter`` subclass to register.

        Raises:
            ValueError: If any of ``converter_cls.SUPPORTED_ENCODINGS`` is already
                registered to another converter.
        """

        # Check that all the encoding of converter_cls are not already present within registry
        # before adding them within the registry
        for converter_encoding in converter_cls.SUPPORTED_ENCODINGS:
            if converter_encoding in cls._registry:
                raise ValueError(
                    f"Converter {converter_cls.__name__} is already registered"
                )

        # Add all the converter encodings within McapSchemaRegistry registry
        for converter_encoding in converter_cls.SUPPORTED_ENCODINGS:
            cls._registry[converter_encoding] = converter_cls

    # --- Main Bridge API ---

    @classmethod
    def get_converter(cls, encoding: str) -> Optional[Type[McapSchemaConverter]]:
        """Looks up the converter class registered for ``encoding``.

        Args:
            encoding: The MCAP schema encoding string to look up (e.g. ``"protobuf"``).

        Returns:
            The registered ``McapSchemaConverter`` subclass, or ``None`` if no converter
            is registered for ``encoding``.
        """

        return cls._registry.get(encoding)

    @classmethod
    def is_encoding_adapted(cls, encoding: str) -> bool:
        """Check whether the passed encoding is supported.

        Args:
            encoding(str): the encoding that needs to be checked

        Returns:
            bool: True if the passed encoding is adapted. False otherwise.
        """
        return encoding in cls._registry.keys()

    @classmethod
    def register(cls, converter_class: Type[T]) -> Type[T]:
        """Class decorator that registers ``converter_class`` in the McapSchemaRegistry."""

        McapSchemaRegistry._register_converter(converter_class)
        return converter_class
