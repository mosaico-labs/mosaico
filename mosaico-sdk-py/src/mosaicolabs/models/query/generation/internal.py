import datetime
from typing import Dict, Type

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
_PYTHON_TYPE_TO_QUERYABLE: Dict[type | None, Type[_QueryableMixinProtocol]] = {
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
