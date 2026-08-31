"""
Builtin interfaces Adaptation Module.

This module provides specialized adapters for translating ROS `builtin_interfaces` into the
standardized Mosaico Ontology.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs import Duration, Message, Time

from ..adapter_base import ROSAdapterBase
from ..bridge import register_default_adapter
from ..ros_message import ROSMessage
from .helpers import _validate_msgdata


@register_default_adapter(is_default=True)
class TimeAdapter(ROSAdapterBase[Time]):
    """
    Adapter for translating ROS Time messages to Mosaico `Time`.

    **Supported ROS Types:**

    - [`builtin_interfaces/msg/Time`](https://docs.ros2.org/foxy/api/builtin_interfaces/msg/Time.html)

    Example:
        ```python
        # Internal usage within the ROS Bridge
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/clock",
            msg_type="builtin_interfaces/msg/Time",
            data={
                "sec": 1000,
                "nanosec": 1000000000
            }
        )
        # Automatically resolves to a flat Mosaico Time with attached metadata
        mosaico_time = TimeAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("builtin_interfaces/msg/Time",)

    __mosaico_ontology_type__: Type[Time] = Time
    _REQUIRED_KEYS = ("sec", "nanosec")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg (ROSMessage): The source ROS message yielded by the loader.
            **kwargs (Any): Additional context for the translation.

        Returns:
            Message: A Mosaico `Message` containing the normalized `Time` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Time:
        """
        Parses a dictionary to extract a `Time` object.

        Example:
            ```python
            ros_data = {
                "sec": 1000,
                "nanosec": 1000000000
            }
            # Automatically resolves to a flat Mosaico Time with attached metadata
            mosaico_time = TimeAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Time: The constructed Mosaico Time object.

        Raises:
            ValueError: If required keys are missing.
        """

        _validate_msgdata(cls, ros_data)
        return Time(
            seconds=ros_data["sec"],
            nanoseconds=ros_data["nanosec"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Time],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Time`` (or a ``Message`` wrapping one) into a
        ``builtin_interfaces/msg/Time`` message.

        Supported output types (selectable via *ros_msg_type*):
        - ``builtin_interfaces/msg/Time``

        Args:
            mosaico_data (Union[Message, Time]): A ``Message`` wrapping a ``Time`` instance, or a raw ``Time``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
                ``builtin_interfaces/msg/Time`` if ``None``.

        Returns:
            MsgType: A ``builtin_interfaces/msg/Time`` instance, or raises an error if:

                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        time_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosTime = typestore.types["builtin_interfaces/msg/Time"]

        ros_time = RosTime(
            sec=time_data.seconds,
            nanosec=time_data.nanoseconds,
        )

        if resolved_rosmsg_type == "builtin_interfaces/msg/Time":
            return ros_time

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )


@register_default_adapter(is_default=True)
class DurationAdapter(ROSAdapterBase[Duration]):
    """
    Adapter for translating ROS Duration messages to Mosaico `Duration`.

    **Supported ROS Types:**

    - [`builtin_interfaces/msg/Duration`](https://docs.ros2.org/foxy/api/builtin_interfaces/msg/Duration.html)

    Example:
        ```python
        # Internal usage within the ROS Bridge
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/duration",
            msg_type="builtin_interfaces/msg/Duration",
            data={
                "sec": 1000,
                "nanosec": 1000000000
            }
        )
        # Automatically resolves to a flat Mosaico Duration with attached metadata
        mosaico_duration = DurationAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("builtin_interfaces/msg/Duration",)

    __mosaico_ontology_type__: Type[Duration] = Duration
    _REQUIRED_KEYS = ("sec", "nanosec")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg (ROSMessage): The source ROS message yielded by the loader.
            **kwargs (Any): Additional context for the translation.

        Returns:
            Message: A Mosaico `Message` containing the normalized `Duration` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Duration:
        """
        Parses a dictionary to extract a `Duration` object.

        Example:
            ```python
            ros_data = {
                "sec": 1000,
                "nanosec": 1000000000
            }
            # Automatically resolves to a flat Mosaico Duration with attached metadata
            mosaico_duration = DurationAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Duration: The constructed Mosaico Duration object.

        Raises:
            ValueError: If required keys are missing.
        """

        _validate_msgdata(cls, ros_data)
        return Duration(
            seconds=ros_data["sec"],
            nanoseconds=ros_data["nanosec"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Duration],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Duration`` (or a ``Message`` wrapping one) into a
        ``builtin_interfaces/msg/Duration`` message.

        Supported output types (selectable via *ros_msg_type*):
        - ``builtin_interfaces/msg/Duration``

        Args:
            mosaico_data (Union[Message, Duration]): A ``Message`` wrapping a ``Duration`` instance, or a raw ``Duration``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
                ``builtin_interfaces/msg/Duration`` if ``None``.

        Returns:
            MsgType: A ``builtin_interfaces/msg/Duration`` instance, or raises an error if:

                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        duration_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosDuration = typestore.types["builtin_interfaces/msg/Duration"]

        ros_duration = RosDuration(
            sec=duration_data.seconds,
            nanosec=duration_data.nanoseconds,
        )

        if resolved_rosmsg_type == "builtin_interfaces/msg/Duration":
            return ros_duration

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )
