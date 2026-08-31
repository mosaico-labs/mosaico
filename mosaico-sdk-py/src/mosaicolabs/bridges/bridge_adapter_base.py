from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Generic, Type, TypeVar

from mosaicolabs import Message, Serializable

T = TypeVar("T", bound=Serializable)
NativeMsgT = TypeVar("NativeMsgT")


class BridgeAdapterBase(ABC, Generic[T, NativeMsgT]):
    __mosaico_ontology_type__: Type[T]

    @classmethod
    @abstractmethod
    def adapter_key(cls) -> Hashable:
        """
        Unique identifier for each adapter. It is deduced from the unique attributes of the subclasses:
            - ROS -> ros_msgtype
            - MCAP -> (schema_name, schema_encoding)
        TODO: improve doc
        """
        ...

    @classmethod
    @abstractmethod
    def to_native(cls, mosaico_data, **kwargs) -> NativeMsgT:
        """TODO"""
        ...

    @classmethod
    @abstractmethod
    def translate(cls, msg, **kwargs) -> Message:
        """TODO"""
        ...
