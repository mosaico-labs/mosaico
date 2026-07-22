import datetime
from typing import Dict, Type

import pyarrow as pa

from ..protocols import _QueryableMixinProtocol
from .mixins import (
    _DynamicFieldFactoryMixin,
    _QueryableBool,
    _QueryableDateTime,
    _QueryableNumeric,
    _QueryableString,
    _QueryableUnsupported,
)

# -------------------------------------------------------------------------
# Type to Queryable Mixin Mapping
# -------------------------------------------------------------------------
_PYTHON_TYPE_TO_QUERYABLE_MIXIN: Dict[type | None, Type[_QueryableMixinProtocol]] = {
    None: _QueryableUnsupported,
    # Numeric Types
    int: _QueryableNumeric,
    float: _QueryableNumeric,
    bool: _QueryableBool,
    # String Type
    str: _QueryableString,
    # Date/Time Types
    datetime.datetime: _QueryableDateTime,
    datetime.date: _QueryableDateTime,
    datetime.time: _QueryableDateTime,
    # Dictionary Type
    dict: _DynamicFieldFactoryMixin,
}

# -------------------------------------------------------------------------
# Pyarrow Type to Python Type Mapping
# This dictionary maps specific PyArrow data types to their corresponding
# python types. This mapping does not include composed types (list, dict)
# that are managed separately in the QueryProxy
# -------------------------------------------------------------------------
_PYARROW_TO_PYTHON_BASE_TYPE: Dict[pa.DataType, type] = {
    # Boolean types
    pa.bool_(): bool,
    # Numeric types → use _QueryableNumeric
    pa.int8(): int,
    pa.int16(): int,
    pa.int32(): int,
    pa.int64(): int,
    pa.uint8(): int,
    pa.uint16(): int,
    pa.uint32(): int,
    pa.uint64(): int,
    pa.float16(): float,
    pa.float32(): float,
    pa.float64(): float,
    # Date/time types
    pa.date32(): datetime.date,
    pa.date64(): datetime.date,
    pa.time32("s"): datetime.time,
    pa.time32("ms"): datetime.time,
    pa.time64("us"): datetime.time,
    pa.time64("ns"): datetime.time,
    pa.timestamp("s"): datetime.datetime,
    pa.timestamp("ms"): datetime.datetime,
    pa.timestamp("us"): datetime.datetime,
    pa.timestamp("ns"): datetime.datetime,
    # String types
    pa.string(): str,
    pa.large_string(): str,
}


class _QueryableList(dict):
    pass


class _QueryableOptional(dict):
    pass
