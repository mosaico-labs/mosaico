from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List

from mcap.decoder import DecoderFactory
from mcap.reader import DecodedMessageTuple
from mcap.records import Schema

from .rule import Rule


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

    def __init__(self) -> None:
        self._field_rule_mapper: Dict[str, List[Rule]] = {}

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

        return self._postprocess(self._to_dict(decoded_msg.decoded_message))

    def _postprocess(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Applies every rule registered via register_schema() surgically to the dict paths
        they target — a path may have more than one rule (see `Rule`/`match_rules`), applied
        in registration order; each `Rule.apply()` tolerates its own field being absent, so one
        rule failing to apply never prevents another rule (for this or any other path) from
        running. No-op when none are registered."""
        for path, rules in self._field_rule_mapper.items():
            for rule in rules:
                rule.apply(data, path)
        return data

    @abstractmethod
    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:
        """Ensures msg_data datatype is supported and then converts native object into a plain nested dict."""
