"""
Flight Action Dispatcher.

This module provides a type-safe wrapper (`_do_action_page`) for executing
PyArrow Flight `do_action` commands returning datastreams.

It employs a Registry Pattern (`_DoActionPageResponse` and subclasses) to map
specific `FlightAction` enums to concrete Data Classes. This ensures that
server responses are automatically deserialized into the correct Python objects,
providing stronger typing and validation than raw dictionaries.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar

import pyarrow.flight as fl

from ..enum import FlightAction
from ..logging_config import get_logger
from ..query.topic_cluster import TopicCluster

# Set the hierarchical logger
logger = get_logger(__name__)

# Generic TypeVar allowing _do_action_page to return the specific subclass requested
T_DoActionPageResponse = TypeVar(
    "T_DoActionPageResponse", bound="_DoActionPageResponse"
)


class _DoActionPageResponse(ABC):
    """
    Abstract base class for Flight Action responses. Differently from the _DoActionPageResponse
    this class handles responses composed of list of dictionaries rather than just a single dictionary.
    Indeed it defines from_list() rather than from_dict().

    This class handles the automatic registration of subclasses. When a subclass
    is defined with a list of `actions`, it is automatically added to the `_registry`.
    """

    # Registry mapping FlightAction -> Subclass Type
    _registry: ClassVar[Dict[FlightAction, Type["_DoActionPageResponse"]]] = {}

    # Subclasses must define which actions they handle
    actions: ClassVar[list[FlightAction]] = []

    def __init_subclass__(cls, **kwargs):
        """
        Metaclass hook to register subclasses automatically.
        """
        super().__init_subclass__(**kwargs)
        for action in getattr(cls, "actions", []):
            if action in _DoActionPageResponse._registry:
                raise ValueError(
                    f"{action} already maps {_DoActionPageResponse._registry[action].__name__} adapter and cannot therefore map also '{cls.__name__}'"
                )

            _DoActionPageResponse._registry[action] = cls

    @classmethod
    def get_class_for_action(
        cls, action: FlightAction
    ) -> Type["_DoActionPageResponse"]:
        """
        Retrieves the registered response class for a given action.

        Args:
            action (FlightAction): The action being performed.

        Returns:
            Type[_DoActionPageResponse]: The class responsible for handling the response.

        Raises:
            KeyError: If no class is registered for the action.
        """
        if action not in cls._registry:
            raise KeyError(f"No subclass registered for action '{action}'")
        return cls._registry[action]

    @classmethod
    @abstractmethod
    def from_list(
        cls: Type[T_DoActionPageResponse], data: list[Dict[str, Any]]
    ) -> T_DoActionPageResponse:
        """
        Abstract method to deserialize a list of dictionaries into an instance.

        Args:
            data (list[Dict[str, Any]]): The raw dictionary from the server response.

        Returns:
            T_DoActionPageResponse: An instance of the class.
        """
        pass


def _do_action_page(
    client: fl.FlightClient,
    action: FlightAction,
    payload: dict[str, Any],
    expected_type: Type[T_DoActionPageResponse],
) -> Optional[T_DoActionPageResponse]:
    """
    Executes a Flight `do_action` command and deserializes the response.
    Differently from the _do_action, the _do_action_page receives multiple responses altogether and merges them

    Args:
        client (fl.FlightClient): The connected Flight client.
        action (FlightAction): The specific action to execute.
        payload (dict[str, Any]): The parameters for the action (serialized to JSON).
        expected_type (Optional[Type]): The expected response class. If provided,
                                        the result is checked against this type.

    Returns:
        Optional[T_DoActionPageResponse]: The deserialized response object, or None
                                          if the server returned no body or returned
                                          action is not consitant with input one.

    Raises:
        TypeError: If returned responses do not have the expected FlightAction type.
        Exception: For Flight errors or JSON decoding failures.
    """
    action_name = action.value
    logger.debug(f"Sending Flight action: '{action_name}'")

    try:
        # Serialize payload
        body = json.dumps(payload).encode("utf-8")
        logger.debug(f"Action request body: '{body}'")

        # Execute Flight call
        action_results = client.do_action(fl.Action(action_name, body))

        # Process the result stream (usually contains 0 or N item)
        returned_responses: list[dict[str, Any]] = []

        for result in action_results:
            if result.body:
                # result.body is a PyArrow Buffer; to_pybytes() is zero-copy or low-overhead
                buffer = result.body.to_pybytes()

                # If no data was received
                if not buffer:
                    return None

                # Decode and Parse exactly once
                result_str = buffer.decode("utf-8")
                result_dict: dict[str, Any] = json.loads(result_str)

                # --- Validation ---
                # Verify the server response is not empty
                r_act = result_dict.get("action")
                if r_act is None or r_act == "empty":
                    logger.debug(
                        f"Action '{action_name}' response had no 'action' field."
                    )
                    return None

                # Verify the server is responding to the correct action and that all actions are the same
                if r_act != action_name:
                    logger.warning(
                        f"Unexpected action in response: got '{r_act}', expected '{action_name}'"
                    )
                    return None

                r_data = result_dict.get("response")
                if r_data is None:
                    logger.debug(
                        f"Action '{action_name}' response had no 'response' field."
                    )
                    return None

                returned_responses.append(r_data)

        # --- Deserialization ---
        # Ensure the registered class matches what the caller expects
        response_cls = _DoActionPageResponse.get_class_for_action(action)
        if response_cls is not expected_type:
            raise TypeError(
                f"Action '{action_name}' returned an unexpected type. "
                f"Got '{response_cls.__name__}', but expected '{expected_type.__name__}'"
            )
        # Parse data
        return expected_type.from_list(returned_responses)

    except Exception as e:
        logger.exception(f"Flight action '{action_name}' failed: '{e}'")
        raise e


# --- Concrete Response Dataclasses ---
@dataclass
class _DoActionPageResponseFilterClusterize(_DoActionPageResponse):
    """Response containing the metadata of the 'topic_filter_clusterize' DoActionPage."""

    actions: ClassVar[list[FlightAction]] = [FlightAction.TOPIC_FILTER_CLUSTERIZE]
    clusters: list[TopicCluster]

    @classmethod
    def from_list(
        cls, data: list[dict[str, Any]]
    ) -> "_DoActionPageResponseFilterClusterize":

        clusters = []
        for resp in data:
            clusters.append(TopicCluster._from_dict(resp))

        return cls(clusters=clusters)


@dataclass
class _DoActionPageResponseFilterIntersect(_DoActionPageResponseFilterClusterize):
    """Response containing the metadata of the 'topic_filter_intersect' DoActionPage."""

    actions: ClassVar[list[FlightAction]] = [FlightAction.TOPIC_FILTER_INTERSECT]
