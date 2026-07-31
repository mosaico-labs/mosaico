from typing import Any, Dict, Generic, Optional, Tuple, Type, TypeVar, Union, cast

import numpy as np
from rosbags.interfaces.typing import Fielddefs, Nodetype
from rosbags.typesys.store import Typestore

from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled
from mosaicolabs.ros_bridge.ros_message import ROSMessage

from ..adapter_base import ROSAdapterBase

T = TypeVar("T", bound=Unmodeled)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


def pack_unmodeled(
    raw_data: dict[str, Any], msg_def: Fielddefs, typestore: Typestore
) -> dict[str, Any]:
    """
    Recursively replaces nested-message dicts in `raw_data` with the actual
    ros message instances required to construct a ROS message.

    `Unmodeled.raw_data` stores nested ROS messages as plain dicts, but rosbags
    message classes expect their nested-message fields to be instances of the
    corresponding rosbags type, not dicts. This walks `msg_def` alongside
    `raw_data` and, for every field describing a nested message
    (`Nodetype.NAME`), instantiates that message type from its (recursively
    packed) dict. Fields holding a base type, array or sequence are left
    as-is, since they can be passed straight through via `**raw_data`.

    Args:
        raw_data (dict[str, Any]): The payload to pack, keyed by field name exactly as declared
            in `msg_def`. Mutated in place for nested-message fields.
        msg_def (Fielddefs): The rosbags field definitions (`Typestore.get_msgdef(...).fields`)
            describing the expected shape of `raw_data`.
        typestore (Typestore): The rosbags typestore used to resolve nested message types
            by name and look up their own field definitions.

    Returns:
        dict[str, Any]: The updated `raw_data`, with every nested-message dict replaced by an instance of
            its corresponding rosbags message type.

    Raises:
        TypeError: If a field's definition doesn't match any of the node types
            rosbags is expected to produce (`NAME`, `BASE`, `SEQUENCE`, `ARRAY`).
    """

    # field_def item example for simple types:   ("x"               , (Nodetype.BASE    , ("float64", 0)                 ))
    # field_def item example for complex types:  ("pos"             , (Nodetype.NAME    , "geometry_msg/msg/Point"))
    # field_def item example for sequence types: ('cell_temperature', (Nodetype.SEQUENCE, ((T.BASE, ('float32', 0)), 0) )) -> unknown size lists
    # field_def item example for sequence types: ('k'               , (Nodetype.ARRAY   , ((T.BASE, ('float64', 0)), 9) )) -> fixed size lists
    for field_name, field_descr in msg_def:
        node_type, content = field_descr

        if node_type is Nodetype.BASE:
            pass  # do nothing, the content base type can be unpacked using **

        elif node_type == Nodetype.NAME and isinstance(content, str):
            msgtype = content
            RosNestedObjectType = typestore.types[msgtype]

            raw_data[field_name] = RosNestedObjectType(
                **pack_unmodeled(
                    raw_data[field_name],
                    typestore.get_msgdef(msgtype).fields,
                    typestore,
                )
            )

        elif node_type in (Nodetype.SEQUENCE, Nodetype.ARRAY):
            # Check that raw_data[field_name] is a list
            if not isinstance(raw_data[field_name], list):
                raise TypeError(
                    f"Expected {list.__name__} type within raw_data but got {type(raw_data[field_name]).__name__}"
                )

            # Check whether contained type is a basetype or a nested one
            list_content, _ = content
            item_node_type, item_content = list_content

            item_node_type = cast(Nodetype, item_node_type)  # just for typechecker

            if item_node_type is Nodetype.BASE:
                inner_item_type, _ = item_content

                # list of strings does not need to be converted to np.array
                if inner_item_type != "string":
                    raw_data[field_name] = np.array(
                        raw_data[field_name], dtype=inner_item_type
                    )

            elif item_node_type is Nodetype.NAME and isinstance(item_content, str):
                # Check that all elements of raw_data[field_name] are dict
                if any(not isinstance(x, dict) for x in raw_data[field_name]):
                    raise TypeError(
                        f"{field_name} is expected to be a {list.__name__} of {dict.__name__} but at least one {list.__name__}'s element is not a {dict.__name__}"
                    )

                inner_msgtype = item_content
                RosListItemObjectType = typestore.types[inner_msgtype]

                raw_data[field_name] = [
                    RosListItemObjectType(
                        **pack_unmodeled(
                            x,
                            typestore.get_msgdef(inner_msgtype).fields,
                            typestore,
                        )
                    )
                    for x in raw_data[field_name]
                ]

            else:
                raise TypeError(
                    f"Parameter {field_name} of type Sequence/Array containes unexpected inner NodeType {item_node_type}"
                )

        else:
            raise TypeError(
                f"Unsupported field definition for '{field_name}': "
                f"node type {node_type!r} with content {content!r}"
            )

    return raw_data


class UnmodeledAdapter(ROSAdapterBase[T], Generic[T]):
    """
    Adapter for translating ROS messages to Mosaico `Unmodeled` subclasses.
    """

    ros_msgtype: str | Tuple[str, ...] = ()

    __mosaico_ontology_type__: Type[T]

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg (ROSMessage): The source ROS message yielded by the loader.
            **kwargs: Additional context for the translation.

        Returns:
            Message: A Mosaico `Message` containing the normalized `Unmodeled` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> T:
        """
        Converts the raw dictionary data into the specific Mosaico `Unmodeled` type.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            T: The constructed `Unmodeled` subclass instance wrapping `ros_data`.
        """
        return cls.__mosaico_ontology_type__(raw_data=ros_data)

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, T],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> Any:
        """
        Converts a Mosaico `Unmodeled` subclass (or a ``Message`` wrapping one) into the
        corresponding ROS message.

        Args:
            mosaico_data (Union[Message, T]): A ``Message`` wrapping an `Unmodeled` instance, or a raw instance.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
                the adapter's default ROS type if `None`.

        Returns:
            Any: The constructed ROS message, or raises an error if:

                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
        """
        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        RosUnmodeled = typestore.types.get(resolved_rosmsg_type)
        if RosUnmodeled is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        unmodeled_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        rosbag_msgdef = typestore.get_msgdef(resolved_rosmsg_type)

        return RosUnmodeled(
            **pack_unmodeled(unmodeled_data.raw_data, rosbag_msgdef.fields, typestore)
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)

    @classmethod
    def get_or_create(
        cls, ontology_type: Type[T], msgtype: str
    ) -> Type["UnmodeledAdapter"]:
        """
        Gets or create an unmodeled adapter for the provided ontology type and msgtype.
        If the adapter if found within _UNMODELED_ADAPTERS_REGISTRY it is returned immediately.
        Conversely, it is first registered and then returned.
        """
        key = ontology_type.__registry_key__ or ontology_type.ontology_tag()
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)
        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (UnmodeledAdapter,),
                {"__mosaico_ontology_type__": ontology_type, "ros_msgtype": msgtype},
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
