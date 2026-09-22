import json
from typing import Any, ClassVar, Dict, Optional

from mcap.decoder import DecoderFactory
from mcap.records import Schema

from ..decoder_base import MCAPMsgDecoder
from ..registry import register_decoder


class JsonDecoderFactory(DecoderFactory):
    """Decodes `json`-message-encoded MCAP records.

    Keys purely off `message_encoding`, matching `channel.message_encoding` (NOT
    `schema.encoding`, which is `"jsonschema"` for this same channel kind). Passes the raw
    bytes through unchanged; `MCAPJsonschemaMsgDecoder._to_dict` does the actual `json.loads`.
    """

    def decoder_for(self, message_encoding: str, schema: Optional[Schema]):
        if message_encoding != "json":
            return None
        return lambda data: data


@register_decoder
class MCAPJsonschemaMsgDecoder(MCAPMsgDecoder):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "json"

    def decoder_factory(self) -> DecoderFactory:
        return JsonDecoderFactory()

    # def register_schema(self, schema: Schema) -> None:
    #     """Computes the per-field postprocessors this schema needs for PyArrow compliance."""

    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:

        # Check decoded message data type is supported
        if not isinstance(msg_data, bytes):
            raise RuntimeError(
                f"{MCAPJsonschemaMsgDecoder.__name__} cannot decode messages that are not {bytes.__name__}.\
                                 Provided message is of type {type(msg_data).__name__}"
            )

        return json.loads(msg_data)
