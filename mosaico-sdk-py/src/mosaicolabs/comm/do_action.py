"""
Flight Action Dispatcher.

This module provides a type-safe wrapper (`_do_action`) for executing
PyArrow Flight `do_action` commands.

It employs a Registry Pattern (`_DoActionResponse` and subclasses) to map
specific `FlightAction` enums to concrete Data Classes. This ensures that
server responses are automatically deserialized into the correct Python objects,
providing stronger typing and validation than raw dictionaries.
"""

import json
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import logging as log
import pyarrow.flight as fl
from ..enum import FlightAction
from ..models.query import QueryResponseItem, QueryResponse

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
    def from_dict(
        cls: Type[T_DoActionResponse], data: Dict[str, Any]
    ) -> T_DoActionResponse:
        """
        Abstract method to deserialize a dictionary into an instance.

        Args:
            data (Dict[str, Any]): The raw dictionary from the server response.

        Returns:
            T_DoActionResponse: An instance of the class.
        """
        pass


def _do_action(
    client: fl.FlightClient,
    action: FlightAction,
    payload: dict[str, Any],
    expected_type: Optional[Type[T_DoActionResponse]],
) -> Optional[T_DoActionResponse]:
    """
    Executes a Flight `do_action` command and deserializes the response.

    Args:
        client (fl.FlightClient): The connected Flight client.
        action (FlightAction): The specific action to execute.
        payload (dict[str, Any]): The parameters for the action (serialized to JSON).
        expected_type (Optional[Type]): The expected response class. If provided,
                                        the result is checked against this type.

    Returns:
        Optional[T_DoActionResponse]: The deserialized response object, or None
                                      if the server returned no body.

    Raises:
        TypeError: If the registered response class does not match `expected_type`.
        Exception: For Flight errors or JSON decoding failures.
    """
    action_name = action.value
    log.debug(f"Sending Flight action: '{action_name}'")

    try:
        # Serialize payload
        body = json.dumps(payload).encode("utf-8")
        log.debug(f"Action request body: {body}")

        # Execute Flight call
        action_results = client.do_action(fl.Action(action_name, body))

        # Process the result stream (usually contains 0 or 1 item)
        for result in action_results:
            if not result.body:
                continue

            try:
                result_str = result.body.to_pybytes().decode("utf-8")
                log.debug(f"Action result body f{result_str}")
                result_dict: dict[str, Any] = json.loads(result_str)
            except Exception as decode_err:
                log.warning(
                    f"Failed to decode Flight action response for '{action_name}': {decode_err}"
                )
                return None

            # --- Validation ---
            # Verify the server is responding to the correct action
            returned_action = result_dict.get("action")
            if returned_action is None or returned_action == "empty":
                log.debug(f"Action '{action_name}' response had no 'action' field.")
                return None

            if returned_action != action_name:
                log.warning(
                    f"Unexpected action in response: got '{result_dict.get('action')}', expected '{action_name}'"
                )
                return None

            response_data = result_dict.get("response")
            if response_data is None:
                log.debug(f"Action '{action_name}' response had no 'response' field.")
                return None

            # --- Deserialization ---
            if expected_type is not None:
                # Ensure the registered class matches what the caller expects
                response_cls = _DoActionResponse.get_class_for_action(action)
                if response_cls is not expected_type:
                    raise TypeError(
                        f"Action '{action_name}' returned an unexpected type. "
                        f"Got {response_cls.__name__}, but expected {expected_type.__name__}"
                    )
                # Parse data
                return expected_type.from_dict(response_data)
            else:
                # Caller didn't ask for a specific type (or return value might be raw)
                return response_data

        log.debug(f"No response body found for Flight action '{action_name}'.")
        return None

    except Exception as e:
        log.exception(f"Flight action '{action_name}' failed: {e}")
        raise e


# --- Concrete Response Dataclasses ---


@dataclass
class _DoActionResponseKey(_DoActionResponse):
    """Response containing a generated resource key (e.g., after creation)."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.SEQUENCE_CREATE,
        FlightAction.TOPIC_CREATE,
    ]
    key: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "_DoActionResponseKey":
        return cls(**data)


@dataclass
class _DoActionResponseSysInfo(_DoActionResponse):
    """Response containing system information (size, dates, locks)."""

    actions: ClassVar[list[FlightAction]] = [
        FlightAction.SEQUENCE_SYSTEM_INFO,
        FlightAction.TOPIC_SYSTEM_INFO,
    ]
    total_size_bytes: int
    created_datetime: datetime.datetime
    is_locked: bool
    chunks_number: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "_DoActionResponseSysInfo":
        return cls(**data)


@dataclass
class _DoActionQueryResponse(_DoActionResponse):
    """Response containing the result of a query to data platform"""

    actions: ClassVar[list[FlightAction]] = [FlightAction.QUERY]
    query_response: QueryResponse

    def __init__(self, qresp: QueryResponse) -> None:
        self.query_response = qresp

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "_DoActionQueryResponse":
        if data.get("items") is None:
            raise KeyError("Unable to find 'items' key in data dict.")
        qresp = QueryResponse(
            items=[QueryResponseItem(**item) for item in data["items"]]
        )
        return _DoActionQueryResponse(qresp=qresp)
