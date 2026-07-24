"""
Dummy Instance Factory.

Generic utility for building a fully-populated instance of any `Serializable`
(or plain pydantic `BaseModel`) subclass, purely by introspecting its pydantic
fields - no per-class filling code required. Used by tests that need to
exercise every ontology class without hand-writing a constructor call for each
one (e.g. schema round-trip / fingerprint-consistency checks across the whole
model catalog).

Handles, recursively:
- Required and `Optional[...]` fields (`Optional` fields are always filled
  with a real value rather than left `None`, to maximize schema coverage).
- Nested `Serializable`/`BaseModel` fields (e.g. `IMU.acceleration: Vector3d`).
- `Enum` fields (a real enum member is used, not an arbitrary primitive).
- Variable-length list fields (`MosaicoType.list_(...)` without `list_size`),
  filled with a small, arbitrary number of elements.
- Fixed-size list/matrix/tensor fields (`MosaicoType.list_(..., list_size=N)`,
  `.matrix(rows=M, cols=N)`, `.tensor3d(depth=D, rows=M, cols=N)`), filled with
  *exactly* the declared number of elements at every nesting level - getting
  this wrong would fail PyArrow serialization (a `FixedSizeListType` column
  requires every row's list to have precisely that length).
"""

import itertools
import typing
from enum import Enum
from typing import Any, Dict, Optional, Type, TypeVar

import pyarrow as pa

from mosaicolabs.models.core.base_model import BaseModel

_BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)

# Monotonically increasing, process-wide counter used to vary generated scalar
# values across fields/instances instead of producing all-identical data.
_counter = itertools.count(1)


def make_dummy_instance(
    cls: Type[_BaseModelT], overrides: Optional[Dict[str, Any]] = None
) -> _BaseModelT:
    """
    Builds an instance of `cls` with every field populated.

    Args:
        cls: The `Serializable`/`BaseModel` subclass to instantiate.
        overrides: Optional explicit values for specific top-level field names,
            for the rare class with a cross-field validator (e.g. `Range`'s
            `min_range <= range <= max_range`) that independent per-field
            generation can't be expected to satisfy generically.

    Returns:
        A fully populated, valid instance of `cls`.
    """
    kwargs = {
        name: _dummy_value(field_info.annotation, field_info.metadata)
        for name, field_info in cls.model_fields.items()
    }
    kwargs.update(overrides or {})
    return cls(**kwargs)


def _dummy_value(annotation: Any, metadata: list) -> Any:
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    # Annotated[X, pa_type] - unwrap, keeping the pyarrow type as fresh metadata
    # for this specific level (mirrors Serializable._resolve_type's own handling).
    if origin is typing.Annotated:
        return _dummy_value(args[0], [args[1]])

    # Optional[X] / Union[X, None] - always fill with a real value for X.
    # Pydantic does not hoist Annotated metadata through an Optional wrapper,
    # so any pyarrow type hint lives inside `X` itself, not in `metadata` here.
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _dummy_value(non_none[0], [])
        raise NotImplementedError(f"Union with multiple types not supported: {args}")

    if origin is dict:
        key_t = args[0] if args else str
        val_t = args[1] if args else str
        return {_dummy_value(key_t, []): _dummy_value(val_t, [])}

    if origin is list:
        inner_t = args[0] if args else float
        pa_meta = metadata[0] if metadata else None
        # A fixed-size list/matrix/tensor row MUST get exactly this many
        # elements; a variable-length list just needs a plausible, small count.
        size = pa_meta.list_size if isinstance(pa_meta, pa.FixedSizeListType) else 2
        return [_dummy_value(inner_t, []) for _ in range(size)]

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return next(iter(annotation))

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation(
            **{
                name: _dummy_value(field_info.annotation, field_info.metadata)
                for name, field_info in annotation.model_fields.items()
            }
        )

    return _dummy_scalar(annotation)


def _dummy_scalar(python_type: type) -> Any:
    n = next(_counter)
    if python_type is bool:
        return bool(n % 2)
    if python_type is int:
        # Kept small (1-50) to stay within range for every integer width used
        # across the ontology (as narrow as int8/uint8), regardless of which
        # specific width this particular field actually resolves to.
        return (n % 50) + 1
    if python_type is float:
        return round(((n % 50) + 1) * 1.25, 3)
    if python_type is str:
        return f"value_{n}"
    if python_type is bytes:
        return f"bytes_{n}".encode()
    raise NotImplementedError(f"No dummy scalar generator for type {python_type!r}")
