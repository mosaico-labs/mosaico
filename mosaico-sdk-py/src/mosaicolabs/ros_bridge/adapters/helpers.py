from typing import List, Type

from ..adapter_base import ROSAdapterBase


def _validate_msgdata(
    cls: Type[ROSAdapterBase], ros_data: dict, case_insensitive: bool = False
):
    missing_keys = [
        key
        for key in cls._REQUIRED_KEYS
        if key not in ros_data.keys()
        and (
            not case_insensitive
            or (
                key.lower() not in ros_data.keys()
                and key.upper() not in ros_data.keys()
            )
        )
    ]

    if missing_keys:
        raise ValueError(
            f"Malformed ROS message '{cls.ros_msgtype}': missing required keys {missing_keys}. "
            f"Available keys: {list(ros_data.keys())}"
        )


def _validate_required_fields(
    cls: Type[ROSAdapterBase], required_fields: List, data: dict
):
    """
    Validate that all required fields are present in the decoded data dictionary.

    Args:
        cls (Type[ROSAdapterBase]): The adapter class being validated.
        data (dict): The decoded data dictionary to validate against.

    Raises:
        ValueError: If one or more required fields are missing from the data.
    """
    if not all(field in data for field in required_fields):
        raise ValueError(
            f"Required fields of {cls.__name__} are missing: "
            f"Required = {cls._REQUIRED_KEYS}, Actual =  {data.keys()}"
        )
