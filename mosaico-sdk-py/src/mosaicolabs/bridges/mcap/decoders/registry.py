from typing import ClassVar, Dict, Optional, Tuple, Type

from .decoder_base import MCAPMsgDecoder


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
