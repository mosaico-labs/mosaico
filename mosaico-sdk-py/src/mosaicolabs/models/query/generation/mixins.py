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
    Mixin providing comparison operators for ordered field types.

    Composed with [`_QueryableField`][mosaicolabs.models.query.generation.mixins._QueryableField]
    to produce a concrete queryable field for any type with a natural
    ordering - numeric ([`_QueryableNumeric`][mosaicolabs.models.query.generation.mixins._QueryableNumeric])
    or date/time ([`_QueryableDateTime`][mosaicolabs.models.query.generation.mixins._QueryableDateTime]).
    Every operator validates its operand(s) against `__mixin_supported_types__`
    (transforming them first via `_transform_value()` if the subclass
    overrides it, e.g. to serialize a `datetime` to ISO 8601), then delegates
    to `_QueryableField._cmp()` to build the atomic comparison expression sent
    to the server.
    """

    __slots__ = ()
    # Allowed Python types per subclass
    __mixin_supported_types__: tuple[type, ...] = (int, float)  # default: numeric

    # --- Operators ---
    def eq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is exactly equal to `value`.

        Args:
            value: The value to compare against. Must match one of the types
                in `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$eq`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", getattr(self, "_transform_value")(value))

    def neq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is not equal to `value`.

        Args:
            value: The value to compare against. Must match one of the types
                in `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$neq`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$neq", getattr(self, "_transform_value")(value))

    def lt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is strictly less than `value`.

        Args:
            value: The exclusive upper bound. Must match one of the types in
                `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$lt`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$lt", getattr(self, "_transform_value")(value))

    def leq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is less than or equal to `value`.

        Args:
            value: The inclusive upper bound. Must match one of the types in
                `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$leq`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$leq", getattr(self, "_transform_value")(value))

    def gt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is strictly greater than `value`.

        Args:
            value: The exclusive lower bound. Must match one of the types in
                `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$gt`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$gt", getattr(self, "_transform_value")(value))

    def geq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is greater than or equal to `value`.

        Args:
            value: The inclusive lower bound. Must match one of the types in
                `__mixin_supported_types__` for this field.

        Returns:
            The atomic comparison expression (`$geq`).

        Raises:
            TypeError: If `value` isn't one of the supported types.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$geq", getattr(self, "_transform_value")(value))

    def in_(self, *values: Any) -> "_QueryExpression":
        """
        Matches records where the field's value is one of `values`.

        Args:
            *values (Any): The candidate values, passed either as separate
                positional arguments (`in_(v1, v2)`) or as a single list or
                tuple (`in_([v1, v2])`). Every value must share the same type,
                matching one of the types in `__mixin_supported_types__` for
                this field.

        Returns:
            The atomic comparison expression (`$in`).

        Raises:
            ValueError: If no values are provided.
            TypeError: If the values don't all share the same, supported type.
        """
        return getattr(self, "_in")(
            *values, allowed_types=self.__mixin_supported_types__
        )

    def between(self, *values: Any) -> "_QueryExpression":
        """
        Matches records where the field's value falls within an inclusive range.

        Args:
            *values (Any): Exactly two values `(lower, upper)`, passed either as two
                positional arguments (`between(lo, hi)`) or as a single list
                or tuple (`between([lo, hi])`), with `lower <= upper`. Both
                must match one of the types in `__mixin_supported_types__` for
                this field.

        Returns:
            The atomic comparison expression (`$between`).

        Raises:
            ValueError: If not exactly two values are provided, or if the
                first value is greater than the second.
            TypeError: If the values don't share the same, supported type.
        """
        return getattr(self, "_between")(
            *values, allowed_types=self.__mixin_supported_types__
        )


class _QueryableNumeric(_QueryableComparable):
    """
    Queryable numeric field: `int` or `float`.

    Inherits every comparison operator from
    [`_QueryableComparable`][mosaicolabs.models.query.generation.mixins._QueryableComparable]
    (`eq`, `neq`, `lt`, `leq`, `gt`, `geq`, `in_`, `between`), restricting
    accepted operand values to `int`/`float`. This is the mixin behind numeric
    `.Q` proxy fields (e.g. `IMU.Q.acceleration.x`) and the class-free
    [`QueryableNumeric`][mosaicolabs.models.query.queryable_fields.QueryableNumeric].
    """

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
    """
    Queryable date/time/timestamp field for comparisons in the backend.

    Inherits every comparison operator from
    [`_QueryableComparable`][mosaicolabs.models.query.generation.mixins._QueryableComparable]
    (`eq`, `neq`, `lt`, `leq`, `gt`, `geq`, `in_`, `between`). Accepts Python
    temporal types (`datetime.date`, `datetime.time`, `datetime.datetime`) as
    well as numeric timestamps (`int`, nanoseconds since epoch); both are
    normalized to a string via `_transform_value()` before being sent to the
    server.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (
        datetime.date,
        datetime.time,
        datetime.datetime,
        int,
    )

    def _transform_value(self, value: Any) -> str:
        """
        Converts a Python date/time or numeric timestamp into a string
        suitable for backend comparison.

        Args:
            value: A `datetime.date`, `datetime.time`, `datetime.datetime`, or
                an `int` (nanoseconds since epoch).

        Returns:
            The ISO 8601 representation of `value`, or the string form of the
            nanosecond timestamp if `value` is an `int`.
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
    Mixin providing comparison operators for boolean fields.

    Composed with [`_QueryableField`][mosaicolabs.models.query.generation.mixins._QueryableField]
    to build queryable `bool` fields (e.g. `ROI.Q.do_rectify`). Booleans only
    support equality: there's no meaningful `.lt()`/`.gt()` ordering, and the
    backend doesn't currently support `.in_()`/`.between()` on this type.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (bool,)

    def eq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field equals `value`.

        Args:
            value: `True` or `False`.

        Returns:
            The atomic comparison expression (`$eq`).

        Raises:
            TypeError: If `value` isn't a `bool`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", value)


# -------------------------------------------------------------------------
# String Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableString:
    """
    Mixin providing comparison operators for string fields.

    Composed with [`_QueryableField`][mosaicolabs.models.query.generation.mixins._QueryableField]
    to build queryable `str` fields (e.g. `IMU.Q.frame_id`). Every operator
    validates that its operand is a `str` before delegating to
    `_QueryableField._cmp()`. Ordering operators (`.lt()`, `.leq()`, `.gt()`,
    `.geq()`) compare strings lexicographically.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (str,)

    def eq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is exactly equal to `value`.

        Args:
            value: The string to compare against.

        Returns:
            The atomic comparison expression (`$eq`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$eq", value)

    # def neq(self, value: Any) -> "_QueryExpression":
    #     getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
    #     return getattr(self, "_cmp")("$neq", value)

    def match(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field contains `value` as a substring.

        Args:
            value: The substring to search for.

        Returns:
            The atomic comparison expression (`$match`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$match", value)

    def lt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field sorts strictly before `value`,
        lexicographically.

        Args:
            value: The exclusive upper bound.

        Returns:
            The atomic comparison expression (`$lt`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$lt", value)

    def leq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field sorts at or before `value`,
        lexicographically.

        Args:
            value: The inclusive upper bound.

        Returns:
            The atomic comparison expression (`$leq`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$leq", value)

    def gt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field sorts strictly after `value`,
        lexicographically.

        Args:
            value: The exclusive lower bound.

        Returns:
            The atomic comparison expression (`$gt`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$gt", value)

    def geq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field sorts at or after `value`,
        lexicographically.

        Args:
            value: The inclusive lower bound.

        Returns:
            The atomic comparison expression (`$geq`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """
        getattr(self, "_validate_value_type")(value, self.__mixin_supported_types__)
        return getattr(self, "_cmp")("$geq", value)

    def in_(self, *values: str) -> "_QueryExpression":
        """
        Matches records where the field's value is one of `values`.

        Args:
            *values (str): The candidate strings, passed either as separate
                positional arguments (`in_(v1, v2)`) or as a single list or
                tuple (`in_([v1, v2])`).

        Returns:
            The atomic comparison expression (`$in`).

        Raises:
            ValueError: If no values are provided.
            TypeError: If any value isn't a `str`.
        """
        return getattr(self, "_in")(
            *values, allowed_types=self.__mixin_supported_types__
        )


# -------------------------------------------------------------------------
# Dynbamic (Multi-Type) Queryable Mixin
# -------------------------------------------------------------------------


class _QueryableDynamicValue:
    """
    A promiscuous mixin for dynamic dict values (e.g. `user_metadata`).

    Composed with [`_QueryableField`][mosaicolabs.models.query.generation.mixins._QueryableField]
    to build queryable fields for entries of a `Dict[str, Any]` field, reached
    via [`_DynamicFieldFactoryMixin`][mosaicolabs.models.query.generation.mixins._DynamicFieldFactoryMixin]'s
    bracket notation (e.g. `<Model>.Q.metadata["mission"]`). Because a
    dictionary value's type isn't known ahead of time, every operator here
    accepts numeric, string, and boolean values with only loose client-side
    type checking, and passes the value straight through to the server.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (type,)

    # --- From _QueryableComparable (Numeric/DateTime) ---
    def eq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field equals `value`.

        Args:
            value: A numeric, string, or boolean value.

        Returns:
            The atomic comparison expression (`$eq`).

        Raises:
            TypeError: If `value` isn't numeric, string, or boolean.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__
            + _QueryableBool.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$eq", value)

    def neq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is not equal to `value`.

        Args:
            value: A numeric, string, or boolean value.

        Returns:
            The atomic comparison expression (`$neq`).

        Raises:
            TypeError: If `value` isn't numeric, string, or boolean.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__
            + _QueryableBool.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$neq", value)

    def match(self, value: Any):
        """
        Matches records where the field contains `value` as a substring.

        Args:
            value: The substring to search for.

        Returns:
            The atomic comparison expression (`$match`).

        Raises:
            TypeError: If `value` isn't a `str`.
        """

        getattr(self, "_validate_value_type")(
            value, _QueryableString.__mixin_supported_types__
        )
        return getattr(self, "_cmp")("$match", value)

    def in_(self, *values: Any):
        """
        Matches records where the field's value is one of `values`.

        Args:
            *values (Any): The candidate values, passed either as separate
                positional arguments (`in_(v1, v2)`) or as a single list or
                tuple (`in_([v1, v2])`).

        Returns:
            The atomic comparison expression (`$in`).

        Raises:
            ValueError: If no values are provided.
        """
        return getattr(self, "_in")(*values, allowed_types=None)

    def lt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is strictly less than `value`.

        Args:
            value: A numeric or string value (booleans aren't ordered).

        Returns:
            The atomic comparison expression (`$lt`).

        Raises:
            TypeError: If `value` isn't numeric or string.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$lt", value)

    def leq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is less than or equal to `value`.

        Args:
            value: A numeric or string value (booleans aren't ordered).

        Returns:
            The atomic comparison expression (`$leq`).

        Raises:
            TypeError: If `value` isn't numeric or string.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$leq", value)

    def gt(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is strictly greater than `value`.

        Args:
            value: A numeric or string value (booleans aren't ordered).

        Returns:
            The atomic comparison expression (`$gt`).

        Raises:
            TypeError: If `value` isn't numeric or string.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$gt", value)

    def geq(self, value: Any) -> "_QueryExpression":
        """
        Matches records where the field is greater than or equal to `value`.

        Args:
            value: A numeric or string value (booleans aren't ordered).

        Returns:
            The atomic comparison expression (`$geq`).

        Raises:
            TypeError: If `value` isn't numeric or string.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableComparable.__mixin_supported_types__
            + _QueryableString.__mixin_supported_types__,
        )
        return getattr(self, "_cmp")("$geq", value)

    def between(self, *values) -> "_QueryExpression":
        """
        Matches records where the field's value falls within an inclusive range.

        Args:
            *values: Exactly two values `(lower, upper)`, passed either as two
                positional arguments (`between(lo, hi)`) or as a single list
                or tuple (`between([lo, hi])`), with `lower <= upper`.

        Returns:
            The atomic comparison expression (`$between`).

        Raises:
            ValueError: If not exactly two values are provided, or if the
                first value is greater than the second.
        """
        return getattr(self, "_between")(*values, allowed_types=None)

    def ex(self, value: bool) -> "_QueryExpression":
        """
        Checks whether the dictionary key backing this field exists.

        Args:
            value: `True` to require the key's existence (`$ex`), `False` to
                require its absence (`$nex`).

        Returns:
            The atomic existence (or non-existence) expression.

        Raises:
            TypeError: If `value` isn't a `bool`.
        """
        getattr(self, "_validate_value_type")(
            value,
            _QueryableBool.__mixin_supported_types__,
        )
        if value:
            return getattr(self, "_cmp")("$ex", None)
        else:
            return getattr(self, "_cmp")("$nex", None)


class _DynamicFieldFactoryMixin:
    """
    Mixin for dict fields (like user_metadata) that allows dynamic key access.

    It provides __getitem__ to dynamically create a queryable field
    for a specific key, e.g., `<DataModel>.Q.dict_field["mission"]`.
    """

    __slots__ = ()
    __mixin_supported_types__: tuple[type, ...] = (type,)

    def __getitem__(self, key: str) -> Any:
        """
        Enables the indexing operations using square bracket notation ([]) to
        dynamically create a queryable field for a given dict key.
        e.g., <DataModel>.Q.dict_field["key"]
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
    Represents a single queryable field, addressed by its fully-qualified path.

    This is the core class that holds state (the field's path and the concrete
    [`_QueryExpression`][mosaicolabs.models.query.expressions._QueryExpression]
    subclass to build) and implements the machinery every `_Queryable*` mixin
    operator relies on: `_cmp()` builds an atomic expression, and
    `_validate_value_type()`, `_in()`, `_between()` back the shared
    `.in_()`/`.between()` operators. The public operators themselves (`eq`,
    `lt`, `match`, ...) come from whichever `_Queryable*` mixin is composed
    alongside this class - see
    [`_make_queryable_field_type()`][mosaicolabs.models.query.generation.mixins._make_queryable_field_type].
    """

    __slots__ = ("_full_path", "_expr_cls")

    def __init__(self, full_path: str, expr_cls: Type[_QueryExpression]):
        """
        Args:
            full_path: The fully-qualified, dot-notated field path, prefixed
                by the ontology tag (e.g. `"IMU.acceleration.x"`).
            expr_cls: The [`_QueryExpression`][mosaicolabs.models.query.expressions._QueryExpression]
                subclass used to build comparison expressions for this field
                (e.g. `_QueryCatalogExpression`).
        """
        self._full_path = full_path
        self._expr_cls = expr_cls

    # --- Core Implementation ---

    def _cmp(self, op: str, value: Any) -> _QueryExpression:
        """
        Internal helper to create an atomic comparison expression.
        """
        return self._expr_cls(self._full_path, op, value)

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
            type_error = [f"'{type(v).__name__}'" for v in values]
            raise TypeError(f"All values must be of the same type. Got: {type_error}")

        # --- Check required type(s), if provided ---
        if req_type is not None:
            if not isinstance(req_type, tuple):
                allowed = (req_type,)
            else:
                allowed = req_type

            if not all(type(v) in allowed for v in values):
                type_error = {", ".join(f"'{t.__name__}'" for t in allowed)}
                raise TypeError(
                    f"Invalid type for '{self.__class__.__name__}' comparison: "
                    f"'{type(value).__name__}'. Expected: {type_error}"
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
        """This is called when an attribute is not found normally. Raise a helpful error."""
        valid_operators = [
            m
            for m, func in inspect.getmembers(
                self.__class__, predicate=inspect.isfunction
            )
            if not m.startswith("_")
        ]
        type_error = [f"'{meth}'" for meth in sorted(valid_operators)]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no operator '{name}'. "
            f"Available methods: {', '.join(type_error)}"
        )


def _make_queryable_field_type(MixinType: Type) -> Type:
    return type(f"{MixinType.__name__}Field", (MixinType, _QueryableField), {})


def _make_queryable_field_intance(
    queryable_type: Type, field_full_path: str, expression_type: Type[_QueryExpression]
) -> Any:
    cls = type(f"{queryable_type.__name__}Field", (queryable_type, _QueryableField), {})
    return cls(full_path=field_full_path, expr_cls=expression_type)
