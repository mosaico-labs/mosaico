from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Generic, Type, TypeVar

from mosaicolabs import Message, Serializable

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
