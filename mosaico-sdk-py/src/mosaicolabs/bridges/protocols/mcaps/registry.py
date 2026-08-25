from typing import ClassVar, Dict, Optional, Type, TypeVar

from mosaicolabs.bridges.protocols.mcaps.base_converter import SchemaConverter

T = TypeVar("T", bound=SchemaConverter)


class SchemaRegistry:
    """
    TODO explain that this is the central registry for MCAP schemas to PyArrow struct
    """

    """ Registry holding all the converters. It maps the encoding string to its converter:
    {
        "protobuf": ProtobufConverter,
        "flatbuffer": FlatBufferConverter,
        ...
    } 
    """
    _registry: ClassVar[Dict[str, Type[SchemaConverter]]] = {}

    @classmethod
    def _register_converter(cls, converter_cls: Type[T]):
        """Adds to registry the passed converter class"""

        # Check that all the encoding of converter_cls are not already present within registry
        # before adding them within the registry
        for converter_encoding in converter_cls.SUPPORTED_ENCODINGS:
            if converter_encoding in cls._registry:
                raise ValueError(
                    f"Converter {converter_cls.__name__} is already registered"
                )

        # Add all the converter encodings within SchemaRegistry registry
        for converter_encoding in converter_cls.SUPPORTED_ENCODINGS:
            cls._registry[converter_encoding] = converter_cls

    # --- Main Bridge API ---

    @classmethod
    def get_converter(cls, encoding: str) -> Optional[Type[SchemaConverter]]:
        return cls._registry.get(encoding)

    @classmethod
    def is_encoding_adapted(cls, encoding) -> bool:
        """TODO: add function args and return type"""
        return encoding in cls._registry.keys()

    @classmethod
    def register(cls, converter_class: Type[T]) -> Type[T]:
        """TODO: explain that this is the decorator that is used to record each converter within the SchemaRegistry"""

        SchemaRegistry._register_converter(converter_class)
        return converter_class
