import json
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Optional, Tuple, Type

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMsg
from mcap.decoder import DecoderFactory
from mcap.reader import DecodedMessageTuple
from mcap.records import Schema
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

from .helpers import JsonDecoderFactory


class MCAPMsgDecoder(ABC):
    """
    Encoding-specific runtime behavior needed to decode MCAP messages into plain dicts.

    Each MCAPDecoder is associated to a specification of `mcap.decoder.DecoderFactory`:
        - `ProtobufDecoderFactory()` (from `mcap_protobuf.decoder`)
        - `JsonDecoderFactory()` (user defined for `jsonschema`)

    that are passed to `McapReader` (from mcap.reader), allowing to turn MCAP agnostic
    data to domain specific data through iter_decoded_messages().
    The resulting domain specific data (`Message` from `google.protobuf.message` or
    `bytes` for jsonschema) are then turned into a Python dict using one of the MCAPMsgDecoder.decode().
    """

    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Checks that SUPPORTED_CHANNEL_ENCODING exists and is not empty
        if (
            not getattr(cls, "SUPPORTED_CHANNEL_ENCODING", None)
            and not cls.SUPPORTED_CHANNEL_ENCODING
        ):
            raise TypeError(
                f"{cls.__name__} must defined a non-empty SUPPORTED_CHANNEL_ENCODING"
            )

    @classmethod
    def supported_encoding(cls) -> str:
        """The `channel.message_encoding` value this decoder handles (e.g. `"protobuf"`)."""
        return cls.SUPPORTED_CHANNEL_ENCODING

    @abstractmethod
    def decoder_factory(self) -> DecoderFactory:
        """The `mcap.decoder.DecoderFactory` to register on the shared reader."""

    def register_schema(self, schema: Schema) -> None:
        """Optional per-schema bookkeeping needed before iteration starts. No-op by default."""
        return None

    def decode(self, decoded_msg: DecodedMessageTuple) -> Dict[str, Any]:
        """
        Turns decoded_msg produced by iter_decoded_messages() of `mcap.reader` into a plain dictionary
        after checking that the passed message encoding is supported by the decoder.
        """

        msg_encoding = decoded_msg.channel.message_encoding

        # Check decoded message encoding is supported
        if msg_encoding != self.supported_encoding():
            raise ValueError(
                f"{type(self).__name__} cannot decode channel with `{msg_encoding}` encoding.\
                  Supported encoding is `{self.supported_encoding()}`"
            )

        return self._to_dict(decoded_msg.decoded_message)

    @abstractmethod
    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:
        """Ensures msg_data datatype is supported and then converts native object into a plain nested dict."""


class DecoderRegistry:
    """
    Registry of `MCAPMsgDecoder` classes, keyed by the `channel.message_encoding` each one
    supports. Populated via the `@register_decoder` decorator.
    """

    _registry: ClassVar[Dict[str, Type[MCAPMsgDecoder]]] = {}

    @classmethod
    def get_decoder(cls, encoding: str) -> Optional[Type[MCAPMsgDecoder]]:
        """Looks up the `MCAPMsgDecoder` class registered for `encoding` or `None` if unregistered."""
        return cls._registry.get(encoding)

    @classmethod
    def all_decoders(cls) -> Tuple[Type[MCAPMsgDecoder], ...]:
        """Every registered `MCAPMsgDecoder` class, deduplicated."""
        return tuple(dict.fromkeys(cls._registry.values()))


def register_decoder(decoder_cls: Type[MCAPMsgDecoder]) -> Type[MCAPMsgDecoder]:
    """Class decorator that registers `decoder_cls` in the `DecoderRegistry`, keyed by its
    own `supported_encoding()`.

    Raises:
        ValueError: If a decoder for that encoding is already registered.
    """
    encoding = decoder_cls.supported_encoding()
    if encoding in DecoderRegistry._registry:
        raise ValueError(
            f"Impossible to set {decoder_cls.__name__} as a decoder for encoding '{encoding}'. \
              Another decoder is already registered with encoding '{encoding}'."
        )
    DecoderRegistry._registry[encoding] = decoder_cls
    return decoder_cls


@register_decoder
class MCAPProtobufMsgDecoder(MCAPMsgDecoder):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "protobuf"

    def __init__(self) -> None:
        self._descriptor_pool: DescriptorPool = DescriptorPool()

    def decoder_factory(self) -> DecoderFactory:
        return ProtobufDecoderFactory()

    def register_schema(self, schema: Schema) -> None:
        """Registers `schema`'s protobuf `FileDescriptorSet` into the pool so messages of this
        type can be decoded during iteration. Skips schemas already registered."""
        try:
            self._descriptor_pool.FindMessageTypeByName(schema.name)
        except KeyError:
            for file_proto in FileDescriptorSet.FromString(schema.data).file:
                self._descriptor_pool.Add(file_proto)

    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:

        # Check decoded message data type is supported
        if not isinstance(msg_data, ProtobufMsg):
            raise RuntimeError(
                f"{MCAPProtobufMsgDecoder.__name__} cannot decode messages that are not {ProtobufMsg.__name__}.\
                                 Provided message is of type {type(msg_data).__name__}"
            )

        return MessageToDict(
            msg_data,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
            descriptor_pool=self._descriptor_pool,
        )


@register_decoder
class MCAPJsonschemaMsgDecoder(MCAPMsgDecoder):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "json"

    def decoder_factory(self) -> DecoderFactory:
        return JsonDecoderFactory()

    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:

        # Check decoded message data type is supported
        if not isinstance(msg_data, bytes):
            raise RuntimeError(
                f"{MCAPJsonschemaMsgDecoder.__name__} cannot decode messages that are not {bytes.__name__}.\
                                 Provided message is of type {type(msg_data).__name__}"
            )

        return json.loads(msg_data)
