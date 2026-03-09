from typing import Any

import numpy as np


def _to_dict(message: Any) -> Any:
    """
    Recursively converts a rosbags message object and its nested fields
    to a standard Python dictionary or a list/primitive type if encountered
    during recursion.
    """
    if hasattr(message, "__msgtype__"):
        data_dict = {}
        fields = getattr(
            message,
            "__slots__",
            [k for k in dir(message) if not k.startswith("_") and k != "__msgtype__"],
        )
        for field_name in fields:
            if field_name.startswith("_") or field_name == "__msgtype__":
                continue
            try:
                field_value = getattr(message, field_name)
                data_dict[field_name] = _to_dict(field_value)
            except AttributeError:
                continue
        return data_dict
    elif isinstance(message, (list, tuple)):
        return [_to_dict(item) for item in message]
    elif isinstance(message, np.ndarray):
        return message.tolist()
    elif hasattr(message, "sec") and hasattr(message, "nanosec"):
        try:
            # Convert ROS time structure to a single float timestamp (seconds)
            return message.sec + message.nanosec * 1e-9
        except Exception:
            return message
    return message
