from typing import List, Optional, Type

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


def _is_valid_covariance(covariance_list: Optional[List[float]]) -> bool:
    """
    Check if a ROS covariance matrix is valid (i.e., not the 'all zeros' sentinel).

    In ROS messages, a covariance matrix filled with all zeros indicates that
    the covariance is unknown or not provided.

    Args:
        covariance_list (Optional[List[float]]): Flattened covariance matrix (usually 3x3 or 6x6).

    Returns:
        bool: True if covariance contains meaningful values, False otherwise.
    """
    if not covariance_list:
        return False

    return any(value != 0.0 for value in covariance_list)
