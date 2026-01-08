import datetime
import inspect
from typing import Any, Tuple, Type, Union
from ..expressions import _QueryExpression


# -------------------------------------------------------------------------
# Queryable Mixins
# -------------------------------------------------------------------------
# The following mixins are designed to be composed with _QueryableField
# to provide specific query capabilities based on the field's data type.
# Each mixin adds methods for comparison operators relevant to its type.
# For example, _QueryableString adds string-specific operators like 'match',
# while _QueryableNumeric adds numeric comparison operators like 'lt', 'gt', etc.
#
# Operator functions (e.g., eq, lt, gt) validate input types and delegate
# the actual expression creation to the underlying _cmp method of _QueryableField.
# This modular design allows for flexible composition of queryable fields
# with appropriate behaviors based on their data types.
#
# NOTE: Calling _cmp and other helper methods is done via getattr to avoid
# direct dependencies between mixins and the base class and to prevent
# IDE warnings about missing methods.
# -------------------------------------------------------------------------


# -------------------------------------------------------------------------
# Numeric Queryable Mixin
# -------------------------------------------------------------------------
class _QueryableComparable:
    """
    Mixin for all comparable fields: numeric, date, timestamp.

    - Supports eq, neq, lt, leq, gt, geq.
    - Validates and transforms Python values before calling _cmp.
    """

    __slots__ = ()
    # Allowed Python types per subclass
    __mixin_supported_types__: tuple[type, ...] = (int, float)  # default: numeric

    # --- Operators ---
    def eq(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", getattr(self, "_transform_value")(value))

    def neq(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$neq", getattr(self, "_transform_value")(value))

    def lt(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$lt", getattr(self, "_transform_value")(value))

    def leq(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$leq", getattr(self, "_transform_value")(value))

    def gt(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$gt", getattr(self, "_transform_value")(value))

    def geq(self, value: Any):
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$geq", getattr(self, "_transform_value")(value))

    def in_(self, *values):
        """
        Finds if the field's value is in the provided list of values.
        Accept either in_(v1, v2, ...) or in_([v1, v2, ...])
        """
        return getattr(self, "_in")(
            *values, allowed_types=self.__mixin_supported_types__
        )

    def between(self, *values):
        """
        Checks if the field's value is between two provided values (inclusive).
        Accept either between(v1, v2, ...) or between([v1, v2, ...])
        """
        return getattr(self, "_between")(
            *values, allowed_types=self.__mixin_supported_types__
        )


class _QueryableNumeric(_QueryableComparable):
    """Numeric fields: int, float, optionally bool."""

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (int, float)


# TODO: Top be better implemented. Disable for now
# # -------------------------------------------------------------------------
# # Optional Field Queryable Base Mixin
# # This class should be added to other mixin classes, when dealing with
# # optional fields
# # -------------------------------------------------------------------------
# class _QueryableOptionalBase:
#     __slots__ = ()
#     __mixin_supported_types__: tuple[type, ...] = (type,)

#     def ex(self):
#         """Checks for existence of the key in the dictionary field."""
#         return getattr(self, "_cmp")("$ex", None)

#     def nex(self):
#         """Checks for non-existence of the key in the dictionary field."""
#         return getattr(self, "_cmp")("$nex", None)


# -------------------------------------------------------------------------
# DateTime Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableDateTime(_QueryableComparable):
    """Represents a queryable date/time/timestamp field for comparisons in the backend.

    Supports Python temporal types (date, time, datetime) as well as numeric timestamps
    (int, representing nanoseconds since epoch) for filtering or ordering.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (
        datetime.date,
        datetime.time,
        datetime.datetime,
        int,
    )

    def _transform_value(self, value: Any) -> str:
        """Convert a Python date/time or numeric timestamp into a string suitable
        for backend comparison.

        - datetime.date, datetime.time, datetime.datetime → ISO 8601 string.
        - int → string representation of nanoseconds since epoch.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)

        if isinstance(value, int):
            # Treat numeric values as timestamps in nanoseconds since epoch
            return str(value)
        else:
            # Convert date/time/datetime to ISO 8601 string
            return value.isoformat()


# -------------------------------------------------------------------------
# Bool Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableBool:
    """
    Provides string comparison operators for queryable fields.
    Automatically validates that operands are strings.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (bool,)

    def eq(self, value: Any) -> "_QueryExpression":
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", value)


# -------------------------------------------------------------------------
# String Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableString:
    """
    Provides string comparison operators for queryable fields.
    Automatically validates that operands are strings.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (str,)

    def eq(self, value: Any) -> "_QueryExpression":
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", value)

    def neq(self, value: Any) -> "_QueryExpression":
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$neq", value)

    def match(self, value: Any) -> "_QueryExpression":
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$match", value)

    def in_(self, *values):
        """
        Finds if the field's value is in the provided list of values.
        Accept either in_(v1, v2, ...) or in_([v1, v2, ...])
        """
        return getattr(self, "_in")(
            *values, allowed_types=self.__mixin_supported_types__
        )


# -------------------------------------------------------------------------
# Dynbamic (Multi-Type) Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableDynamicValue:
    """
    A promiscuous mixin for dynamic dict values (e.g., metadata).

    It provides all operators (numeric, string, bool) and performs
    NO client-side type validation, passing all values straight to _cmp.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (type,)

    # --- From _QueryableComparable (Numeric/DateTime) ---
    def eq(self, value: Any):
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__
            + _QueryableBool.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$eq", value)

    def lt(self, value: Any):
        getattr(self, "_validate_value_type")(
            value, _QueryableComparable.__mixin_supported_types__
        )
        return getattr(self, "_cmp")("$lt", value)

    def leq(self, value: Any):
        getattr(self, "_validate_value_type")(
            value, _QueryableComparable.__mixin_supported_types__
        )
        return getattr(self, "_cmp")("$leq", value)

    def gt(self, value: Any):
        getattr(self, "_validate_value_type")(
            value, _QueryableComparable.__mixin_supported_types__
        )
        return getattr(self, "_cmp")("$gt", value)

    def geq(self, value: Any):
        getattr(self, "_validate_value_type")(
            value, _QueryableComparable.__mixin_supported_types__
        )
        return getattr(self, "_cmp")("$geq", value)

    def between(self, *values):
        """
        Checks if the field's value is between two provided values (inclusive).
        Accept either between(v1, v2, ...) or between([v1, v2, ...])
        """
        return getattr(self, "_between")(*values, allowed_types=None)


class _DynamicFieldFactoryMixin:
    """
    Mixin for dict fields (like user_metadata) that allows dynamic key access.

    It provides __getitem__ to dynamically create a queryable field
    for a specific key, e.g., `Topic.Q.user_metadata["mission"]`.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (type,)

    def __getitem__(self, key: str) -> Any:
        """
        Enables the indexing operations using square bracket notation ([]) to
        dynamically create a queryable field for a given dict key.
        e.g., Topic.Q.user_metadata["mission"]
        """
        if not isinstance(key, str):
            raise TypeError(
                f"Dictionary key must be a string, got '{type(key).__name__}'"
            )

        # NOTE: This mixin is always combined with _QueryableField,
        # so self.full_path and self._expr_cls are available.

        # The new path is the key nested under the base path
        # e.g., "user_metadata.mission"
        new_path = f"{self.full_path}.{key}"

        # Create a dynamic class that has all queryable behaviors.
        # A value in a Dict[str, Any] could be anything, so we
        # provide all operator sets.
        _QueryableDynamicValueField = type(
            "_QueryableDynamicValueField",
            (
                _QueryableDynamicValue,  # "do-it-all" mixin
                _QueryableField,  # Base implementation
            ),
            {},
        )

        # Return an instance of this new dynamic field
        return _QueryableDynamicValueField(full_path=new_path, expr_cls=self._expr_cls)

    def __getattr__(self, name: str):
        # Override __getattr__ to give a more helpful error
        if name.startswith("_"):
            # Allow access to internal attributes like __path__
            return object.__getattribute__(self, name)

        raise AttributeError(
            f"Field '{self.full_path}' is a queryable dictionary. "
            f"Use square brackets `[]` to access keys, e.g., "
            f'`{self.full_path}["your_key"].eq("value")`. '
            f"Do not use dot-notation (.{name})."
        )


# -------------------------------------------------------------------------
# Unsupported Type Queryable (Non-queryable fields)
# -------------------------------------------------------------------------


class _QueryableUnsupported:
    """
    Mixin for fields that do not support any query operations.

    Attempting to call any comparison operator or access any method on such
    a field will raise an informative error message.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = ()

    def __getattr__(self, name: str):
        raise AttributeError(
            f"'{self.__class__.__name__}' provides no operators. "
            f"You are querying a non-queryable field."
        )


# -------------------------------------------------------------------------
# Main Queryable Field Class
# -------------------------------------------------------------------------


class _QueryableField:
    """
    Represents a single queryable field.
    This is the core class that holds state (path, expr_cls)
    and provides the implementation for creating expressions (_cmp)
    AND all general-purpose helper methods.
    """

    __slots__ = ("full_path", "_expr_cls")

    def __init__(self, full_path: str, expr_cls: Type[_QueryExpression]):
        self.full_path = full_path
        self._expr_cls = expr_cls

    # --- Core Implementation ---

    def _cmp(self, op: str, value: Any) -> _QueryExpression:
        """
        Internal helper to create an atomic comparison expression.
        """
        return self._expr_cls(self.full_path, op, value)

    def _transform_value(self, value: Any) -> Any:
        """
        Transform the value before comparison.
        Default: identity.
        Subclasses can override to normalize types.
        """
        return value

    def _validate_value_type(
        self, value: Any, req_type: Union[Type, Tuple[Type, ...], None]
    ):
        """
        Validate that:
        • values share the same type
        • req_type may be a single type or tuple of allowed types
            (mirrors isinstance() semantics)
        """
        # Normalize to list
        if not isinstance(value, (list, tuple)):
            values = [value]
        else:
            values = list(value)

        # --- Check that all values share the same type ---
        first_type = type(values[0])
        if not all(type(v) is first_type for v in values):
            raise TypeError(
                "All values must be of the same type. "
                f"Got: {[f"'{type(v).__name__}'" for v in values]}"
            )

        # --- Check required type(s), if provided ---
        if req_type is not None:
            if not isinstance(req_type, tuple):
                allowed = (req_type,)
            else:
                allowed = req_type

            if not all(type(v) in allowed for v in values):
                raise TypeError(
                    f"Invalid type for '{self.__class__.__name__}' comparison: "
                    f"'{type(value).__name__}'. Expected: ({', '.join(f"'{t.__name__}'" for t in allowed)})"
                )
        return True

    def _in(self, *values, allowed_types: Union[Type, Tuple[Type, ...], None]):
        """
        Finds if the field's value is in the provided list of values.
        Accept either in_(v1, v2, ...) or in_([v1, v2, ...])
        """

        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            values = values[0]  # unpack list/tuple
        else:
            values = list(values)

        # --- THIS IS THE BUG FIX ---
        if not values:
            raise ValueError("'in_' operator requires at least one value.")

        # Validate type of each value
        getattr(self, "_validate_value_type")(values, allowed_types)

        transformed = [getattr(self, "_transform_value")(v) for v in values]
        return getattr(self, "_cmp")("$in", transformed)

    def _between(self, *values, allowed_types: Union[Type, Tuple[Type, ...], None]):
        """
        Checks if the field's value is between two provided values (inclusive).
        Accept either between(v1, v2, ...) or between([v1, v2, ...])
        """

        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            values = values[0]  # unpack list/tuple
        else:
            values = list(values)

        if len(values) != 2:
            raise ValueError("'between' operator requires exactly two numeric values.")

        # Validate type of each value
        getattr(self, "_validate_value_type")(values, allowed_types)

        # Ensure first <= second
        if values[0] > values[1]:
            raise ValueError(
                "'between' operator expects the first value less than (or equal to) the second."
            )

        transformed = [getattr(self, "_transform_value")(v) for v in values]
        return getattr(self, "_cmp")("$between", transformed)

    def __getattr__(self, name: str):
        """This is callesd when an attribute is not found normally. Raise a helpful error."""
        valid_operators = [
            m
            for m, func in inspect.getmembers(
                self.__class__, predicate=inspect.isfunction
            )
            if not m.startswith("_")
        ]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no operator '{name}'. "
            f"Available methods: {', '.join([f"'{meth}'" for meth in sorted(valid_operators)])}"
        )
