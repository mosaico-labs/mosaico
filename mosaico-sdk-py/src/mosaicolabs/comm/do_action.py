"""
Flight Action Dispatcher.

This module provides a type-safe wrapper (`_do_action`) for executing
PyArrow Flight `do_action` commands.

It employs a Registry Pattern (`_DoActionResponse` and subclasses) to map
specific `FlightAction` enums to concrete Data Classes. This ensures that
server responses are automatically deserialized into the correct Python objects,
providing stronger typing and validation than raw dictionaries.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Dict, Optional, Type, TypeVar

import pyarrow.flight as fl
from google.protobuf.message import Message

from mosaicolabs.platform.server_config import ServerInfo
from mosaicolabs.proto.v1 import responses_pb2

from ..enum import FlightAction
from ..logging_config import get_logger
from ..query import QueryResponse, QueryResponseItem
from .notifications import Notification

# Set the hierarchical logger
logger = get_logger(__name__)

# Generic TypeVar allowing _do_action to return the specific subclass requested
T_DoActionResponse = TypeVar("T_DoActionResponse", bound="_DoActionResponse")


class _DoActionResponse(ABC):
    """
    Abstract base class for Flight Action responses.

    This class handles the automatic registration of subclasses. When a subclass
    is defined with a list of `actions`, it is automatically added to the `_registry`.
    """

    # Registry mapping FlightAction -> Subclass Type
    _registry: ClassVar[Dict[FlightAction, Type["_DoActionResponse"]]] = {}

    # Subclasses must define which actions they handle
    actions: ClassVar[list[FlightAction]] = []

    # Subclasses must define the protobuf message type their response is wire-encoded as.
    wire_type: ClassVar[Type[Message]]

    def __init_subclass__(cls, **kwargs):
        """
        Metaclass hook to register subclasses automatically.
        """
        super().__init_subclass__(**kwargs)
        for action in getattr(cls, "actions", []):
            _DoActionResponse._registry[action] = cls

    @classmethod
    def get_class_for_action(cls, action: FlightAction) -> Type["_DoActionResponse"]:
        """
        Retrieves the registered response class for a given action.

        Args:
            action (FlightAction): The action being performed.

        Returns:
            Type[_DoActionResponse]: The class responsible for handling the response.

        Raises:
            KeyError: If no class is registered for the action.
        """
        if action not in cls._registry:
            raise KeyError(f"No subclass registered for action '{action}'")
        return cls._registry[action]

    @classmethod
    @abstractmethod
    def from_proto(cls: Type[T_DoActionResponse], msg: Message) -> T_DoActionResponse:
        """
        Abstract method to deserialize a decoded protobuf message into an instance.

        Args:
            msg (Message): The decoded protobuf response message.

        Returns:
            T_DoActionResponse: An instance of the class.
        """
        pass


def _do_action(
    client: fl.FlightClient,
    action: FlightAction,
    request: Message,
    expected_type: Optional[Type[T_DoActionResponse]],
) -> Optional[T_DoActionResponse]:
    """
    Executes a Flight `do_action` command and deserializes the response.

    Args:
        client (fl.FlightClient): The connected Flight client.
        action (FlightAction): The specific action to execute.
        request (Message): The protobuf request message for the action.
        expected_type (Optional[Type]): The expected response class. If provided,
                                        the result is checked against this type.

    Returns:
        Optional[T_DoActionResponse]: The deserialized response object, or None
                                      if the server returned no body.

    Raises:
        TypeError: If the registered response class does not match `expected_type`.
        Exception: For Flight errors or protobuf decoding failures.
    """
    action_name = action.value
    logger.debug(f"Sending Flight action: '{action_name}'")

    try:
        # Serialize the request as raw protobuf binary.
        body = request.SerializeToString()
        logger.debug(f"Action request body: '{body!r}'")

        # Execute Flight call
        action_results = client.do_action(fl.Action(action_name, body))

        # Process the result stream (usually contains 0 or 1 item)
        # Accumulate bytes in a list
        # (much faster than repeatedly concatenating immutable bytes objects)
        chunks: list[bytes] = []
        received_any = False

        for result in action_results:
            received_any = True
            if result.body is not None:
                # result.body is a PyArrow Buffer; to_pybytes() is zero-copy or
                # low-overhead. Note it may be zero-length: the server always
                # wraps a response in exactly one Result, even when the
                # message serializes to zero bytes (e.g. an empty Query or
                # NotificationList), so emptiness must not be mistaken for
                # "no result arrived".
                chunks.append(result.body.to_pybytes())

        # If the stream yielded no result at all (as opposed to one result
        # with a zero-length body)
        if not received_any:
            return None

        # Join all chunks into one contiguous byte sequence
        full_response_bytes = b"".join(chunks)

        # --- Deserialization ---
        if expected_type is not None:
            # Ensure the registered class matches what the caller expects
            response_cls = _DoActionResponse.get_class_for_action(action)
            if response_cls is not expected_type:
                raise TypeError(
                    f"Action '{action_name}' returned an unexpected type. "
                    f"Got '{response_cls.__name__}', but expected '{expected_type.__name__}'"
                )
            # Decode the raw protobuf binary as the registered wire type, then
            # convert it into the expected dataclass.
            msg = expected_type.wire_type.FromString(full_response_bytes)
            return expected_type.from_proto(msg)
        else:
            # Caller didn't ask for a specific type; nothing meaningful to return.
            return None

    except Exception as e:
        logger.exception(f"Flight action '{action_name}' failed: '{e}'")
        raise e


# --- Concrete Response Dataclasses ---


@dataclass
class _DoActionInfoResponse(_DoActionResponse):
    """Response containing the metadata of the 'info' DoAction."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.INFO,
    ]
    wire_type: ClassVar[Type[Message]] = responses_pb2.ServerInfo
    info: ServerInfo

    @classmethod
    def from_proto(cls, msg: responses_pb2.ServerInfo) -> "_DoActionInfoResponse":
        return cls(info=ServerInfo._from_proto(msg))


@dataclass
class _DoActionSessionCreateResponse(_DoActionResponse):
    """Response containing the metadata of the 'session_create' DoAction."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.SESSION_CREATE,
    ]
    wire_type: ClassVar[Type[Message]] = responses_pb2.SessionCreate
    uuid: str
    locator: str

    @classmethod
    def from_proto(
        cls, msg: responses_pb2.SessionCreate
    ) -> "_DoActionSessionCreateResponse":
        return cls(uuid=msg.uuid, locator=msg.locator)


@dataclass
class _DoActionTopicCreateResponse(_DoActionResponse):
    """Response containing the metadata of the 'topic_create' DoAction."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.TOPIC_CREATE,
    ]
    wire_type: ClassVar[Type[Message]] = responses_pb2.ResourceUuid
    uuid: str

    @classmethod
    def from_proto(
        cls, msg: responses_pb2.ResourceUuid
    ) -> "_DoActionTopicCreateResponse":
        return cls(uuid=msg.uuid)


@dataclass
class _DoActionQueryResponse(_DoActionResponse):
    """Response containing the result of a query to data platform"""

    actions: ClassVar[list[FlightAction]] = [FlightAction.QUERY]
    wire_type: ClassVar[Type[Message]] = responses_pb2.Query
    query_response: QueryResponse

    @classmethod
    def from_proto(cls, msg: responses_pb2.Query) -> "_DoActionQueryResponse":
        qresp = QueryResponse(
            items=[QueryResponseItem._from_proto(item) for item in msg.items]
        )
        return cls(query_response=qresp)


@dataclass
class _DoActionNotificationList(_DoActionResponse):
    """Response containing a list."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.SEQUENCE_NOTIFICATION_LIST,
        FlightAction.TOPIC_NOTIFICATION_LIST,
    ]
    wire_type: ClassVar[Type[Message]] = responses_pb2.NotificationList
    notifications: list[Notification]

    @classmethod
    def from_proto(
        cls, msg: responses_pb2.NotificationList
    ) -> "_DoActionNotificationList":
        return cls(
            notifications=[
                Notification._from_proto(notification)
                for notification in msg.notifications
            ]
        )
