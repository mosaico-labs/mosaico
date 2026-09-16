from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Generic, Optional, Type, TypeVar, Union

from mosaicolabs import Header, Message, Serializable, Time

T = TypeVar("T", bound=Serializable)
NativeMsgT = TypeVar("NativeMsgT")


class BridgeAdapterBase(ABC, Generic[T, NativeMsgT]):
    """
    Common interface implemented by every bridge adapter (e.g. :class:`ROSAdapterBase`,
    :class:`MCAPAdapterBase`).

    An adapter is the semantic core of a bridge: it knows how to translate one native
    message format (``NativeMsgT``, e.g. a ROS message or an MCAP record) to and from a
    strongly-typed Mosaico ontology object (``T``).
    """

    __mosaico_ontology_type__: Type[T]
    """The Mosaico ontology class this adapter produces/consumes (e.g. ``IMU``)."""

    @classmethod
    @abstractmethod
    def adapter_key(cls) -> Hashable:
        """
        Returns:
            Hashable: A unique key identifying this adapter, deduced from the
            bridge-specific attributes that determine which native messages it
            handles (e.g. ``ros_msgtype`` for ROS, ``(schema_name, schema_encoding)``
            for MCAP).
        """
        ...

    @classmethod
    @abstractmethod
    def to_native(cls, mosaico_data, **kwargs) -> NativeMsgT:
        """
        Converts a Mosaico message or ontology object into the adapter's native message type.

        Args:
            mosaico_data (Union[Message, T]): A ``Message`` wrapper or a raw ``Serializable``
                ontology instance to convert.
            **kwargs (Any): Bridge-specific extra arguments forwarded to the underlying
                conversion.

        Returns:
            NativeMsgT: The native message obtained from ``mosaico_data``.
        """
        ...

    @classmethod
    @abstractmethod
    def translate(cls, msg, **kwargs) -> Message:
        """
        Translates a native message instance into a Mosaico Message.

        Args:
            msg: The source native message yielded by the bridge's loader.

        Returns:
            Message: A Mosaico Message object containing the instantiated ontology data.

        Raises:
            Exception: If translation fails due to missing fields, type mismatches, or other errors.
        """
        ...

    @classmethod
    def unpack_mosaico_msg(cls, mosaico_msg: Union[Message, T]) -> tuple[T, Header]:
        """
        Extracts the typed Mosaico payload and its ``Header`` (if present) from a wrapped or bare message.

        Handles two input cases:

        - **``Message`` wrapper**: the typed data is extracted via ``get_data()``.
        - **Raw ontology instance**: returned as-is with

        the ``Header`` is extracted from the ontology (if supported), otherwise an default Header (empty `frame_id` and zero `Time`) is returned.

        Args:
            mosaico_msg (Union[Message, T]): Either a ``Message`` envelope or a raw instance of
                ``cls.__mosaico_ontology_type__``.

        Returns:
            tuple[T, Header]: A ``(data, header)`` tuple where *data* is the typed ontology object and
            *header* is the corresponding ``Header``, or a default ``Header`` (empty ``frame_id`` and
            zero ``Time``) if not present.

        Raises:
            TypeError: If *mosaico_msg* is neither a ``Message`` nor an instance of
                the expected ontology type.
        """
        if isinstance(mosaico_msg, Message):
            data: Optional[T] = mosaico_msg.get_data(cls.__mosaico_ontology_type__)
            if data is None:
                raise TypeError(
                    f"Adapter {cls.__name__} cannot handle {mosaico_msg.ontology_tag()} Mosaico type"
                )

        elif isinstance(mosaico_msg, cls.__mosaico_ontology_type__):
            data = mosaico_msg

        else:
            raise TypeError(
                f"Mosaico data passed to {cls.__name__} Adapter has type {type(mosaico_msg)} and it is neither a Message nor a {cls.__mosaico_ontology_type__.ontology_tag()}"
            )

        header = Header(frame_id="", timestamp=Time(seconds=0, nanoseconds=0))

        tmp = getattr(data, "header", None)

        if tmp:
            if isinstance(tmp, Header):
                header.frame_id = tmp.frame_id
                header.timestamp = tmp.timestamp

            else:
                raise TypeError(
                    f"Message {mosaico_msg.ontology_tag()} has a field called `header` that is not of type {Header.__class__.__name__}. Please rename it!"
                )

        return data, header
