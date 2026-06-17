import json
from dataclasses import is_dataclass
from typing import Any

from pydantic import BaseModel


def _fix_empty_dicts(obj):
    """
    Recursively replaces dictionaries where all values are None
    with a single None value.

    Notes:
        Fixes a schema issue with Parquet v2 deserialization:
        Fields defined as `Vector3d = None` may be incorrectly
        deserialized as `Vector3d(x=None, y=None, z=None)`.
        This function cleans up that structure back to `None`.
    """
    if isinstance(obj, dict):
        # Recursively fix all values in the dictionary
        fixed = {k: _fix_empty_dicts(v) for k, v in obj.items()}

        # If all values in the fixed dict are None, return None
        if all(v is None for v in fixed.values()):
            return None
        # Otherwise, return the fixed dictionary
        return fixed
    # If not a dict, return the object unchanged
    return obj


def encode_to_dict(obj: Any, exclude_none: bool = False) -> Any:
    """
    Recursively converts a Pydantic model, dataclass, or nested structures (lists, tuples)
    into a standard Python dictionary representation.

    Args:
        obj: The input object to encode. Can be a Pydantic model, dataclass, list, tuple, or primitive.
        skip_none (bool): If True, omit fields with None values from the resulting dictionary.

    Returns:
        Any: A dictionary (for models/dataclasses), a list/tuple (for iterables),
             or the original primitive value if not a supported structure.
    """

    # Handle explicit None values
    if obj is None:
        return None

    # --- Handle Pydantic model instances ---
    # Pydantic models provide a built-in method `.model_dump()` which converts the model
    # (and all nested models) into a plain Python dictionary recursively.
    if isinstance(obj, BaseModel):
        return obj.model_dump(exclude_none=exclude_none)

    # --- Handle dataclass instances ---
    # Convert dataclasses into dictionaries, recursively encoding their attributes.
    # We skip private fields (those starting with '_') and optionally skip None values.
    if is_dataclass(obj) and not isinstance(obj, type):
        return {
            key: encode_to_dict(value, exclude_none=exclude_none)
            for key, value in obj.__dict__.items()
            if not key.startswith("_") and (value is not None or not exclude_none)
        }

    # --- Handle iterable types (lists and tuples) ---
    # Recursively apply encoding to each element in the collection.
    if isinstance(obj, (list, tuple)):
        # Preserve the original container type (list or tuple)
        return type(obj)(
            encode_to_dict(item, exclude_none=exclude_none) for item in obj
        )

    # --- Handle dict types ---
    if isinstance(obj, dict):
        # convert all the value of obj to dict in case of nested structure or Pydantic model
        to_json_dump = {
            key: encode_to_dict(value, exclude_none=exclude_none)
            for key, value in obj.items()
            if (value is not None or not exclude_none)
        }
        return json.dumps(to_json_dump)

    # --- Base case: primitive or non-special object ---
    # Return primitive types (int, str, float, datetime, etc.) as-is.
    return obj
