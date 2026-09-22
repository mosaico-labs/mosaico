import json
from typing import Dict, List

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.timestamp_pb2 import Timestamp

from ..rule import (
    FieldContainer,
    FieldKey,
    FieldStep,
    ListStep,
    MapValuesStep,
    Rule,
    RulePath,
    match_rules,
)

# ------------------------------------------------ #
# ------------------ PREDICATES ------------------ #
# ------------------------------------------------ #


def is_field_int64(field: FieldDescriptor) -> bool:
    """Whether `field` is one of the int64-family protobuf types that need coercion
    into a native Python int."""

    return field.type in (
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_FIXED64,
        FieldDescriptor.TYPE_SFIXED64,
        FieldDescriptor.TYPE_SINT64,
        FieldDescriptor.TYPE_UINT64,
    )


def is_field_any(field: FieldDescriptor) -> bool:
    """Whether `field` is a `google.protobuf.Any` message field."""

    return bool(field.message_type and field.message_type.name == "Any")


def is_field_timestamp(field: FieldDescriptor) -> bool:
    """Whether `field` is a `google.protobuf.Any` message field."""

    return bool(field.message_type and field.message_type.name == "Timestamp")


def is_field_oneof(field: FieldDescriptor) -> bool:
    """Whether `field` is a member of a `oneof` group."""
    return bool(field.containing_oneof)


def is_field_nested(field: FieldDescriptor) -> bool:
    """Whether `field` holds a nested message (or group) that should be recursed into."""

    if field.type in (FieldDescriptor.TYPE_GROUP, FieldDescriptor.TYPE_MESSAGE):
        return True
    return False


def is_field_singular_message(field: FieldDescriptor) -> bool:
    """Whether `field` is a non-`repeated` message field. Like `oneof` members, such fields
    have explicit presence, so `always_print_fields_with_no_presence` doesn't force
    `MessageToDict` to print them when unset (unlike `repeated`/`map` fields, which have no
    presence and are always printed)."""

    return is_field_nested(field) and not field.is_repeated


def is_field_map(field: FieldDescriptor) -> bool:
    """Whether `field` is a protobuf `map<K, V>` field. Maps are implemented as a `repeated`
    field of a synthesized `MapEntry` message (with `key`/`value` fields), so this must be
    checked before treating a `repeated` field as an ordinary list."""

    return bool(
        field.is_repeated
        and field.message_type
        and field.message_type.GetOptions().map_entry
    )


# ------------------------------------------------ #
# -------------------- EFFECTS ------------------- #
# ------------------------------------------------ #


def _require_present(data: FieldContainer, key: FieldKey) -> None:
    """Raises `KeyError` if `key` is absent from `data`, for callbacks that need the field to
    actually be there (unlike `_fill_absent_field`, which handles absence itself).

    Only meaningful for a `dict` container, where a missing key means `MessageToDict` omitted
    a field that has explicit presence (see `is_field_singular_message`/`is_field_oneof`). For
    `data` with a `List` type this is a no-op."""
    if isinstance(data, dict) and key not in data:
        raise KeyError(
            f"{key} key is not present. Available keys are: {list(data.keys())}"
        )


def _coerce_int64_field(data: FieldContainer, key: FieldKey) -> None:
    """protobuf's JSON mapping renders int64-family fields as strings unless
    `unquote_int64_if_possible` narrows the value into range; this normalizes whatever
    `MessageToDict` produced into a native Python int."""

    _require_present(data, key)

    data[key] = int(data[key])


def _modify_any_field(data: FieldContainer, key: FieldKey) -> None:
    """`MessageToDict` renders `google.protobuf.Any` as `{"@type": ..., <expanded fields>}`.
    PyArrow's schema instead expects the well-known `{"type_url": ..., "value": <json string>}`
    shape, so this reshapes the dict at `key` in place."""

    _require_present(data, key)

    any_data = data[key]
    type_url = any_data.pop("@type")
    data[key] = {"type_url": type_url, "value": json.dumps(any_data)}


def _modify_timestamp_field(data: FieldContainer, key: FieldKey):
    """
    `MessageToDict` encodes `google.protobuf.Timestamp as a string with RFC 3339 format
    ("{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z"). PyArrow instead expects the protobuf
    original schema `{"seconds": ..., "nanos": ...}` shape, so this function modifies the `data`
    dict returning to the
    See here for more info: https://protobuf.dev/reference/php/api-docs/Google/Protobuf/Timestamp.html"""

    _require_present(data, key)

    rfc_time = data[key]

    ts = Timestamp()
    ts.FromJsonString(rfc_time)

    data[key] = {"seconds": ts.seconds, "nanos": ts.nanos}


def _fill_absent_field(data: FieldContainer, key: FieldKey) -> None:
    """`MessageToDict` omits a field entirely whenever it has explicit presence and is unset:
    this covers both `oneof` members (PyArrow's schema declares every member of the group as
    its own column, so each sibling must be present, `None` when unset) and singular message
    fields (PyArrow's schema declares them as a nullable struct column). This callback fills
    the key with `None` if `MessageToDict` didn't already populate it, leaving a set value
    untouched.

    Unlike `_coerce_int64_field`/`_modify_any_field`/`_modify_timestamp_field`, this callback
    is expected to run on an absent key and never raises — so when a field also matches one of
    those (e.g. an `Any` field that is also `oneof`), this rule still fills in `None` after the
    other rule's `KeyError` is caught by `Rule.apply()`."""
    if key not in data:
        data[key] = None


# ------------------------------------------------ #
# -------------------- MATCHER ------------------- #
# ------------------------------------------------ #


class ProtobufRulesMatcher:
    PROTOBUF_RULES: List[Rule[FieldDescriptor]] = [
        Rule(is_field_int64, _coerce_int64_field),
        Rule(is_field_any, _modify_any_field),
        Rule(is_field_timestamp, _modify_timestamp_field),
        Rule(is_field_oneof, _fill_absent_field),
        Rule(is_field_singular_message, _fill_absent_field),
    ]

    @classmethod
    def from_descriptor(cls, descr: Descriptor) -> Dict[RulePath, List[Rule]]:
        """
        Walks `descr`'s fields (recursing into nested messages) and returns a dict mapping
        each field's `RulePath` to the ordered list of `PROTOBUF_RULES` entries whose
        predicate matches that field (see `match_rules`) — a field can match more than one,
        e.g. an `Any` field that is also a `oneof` member matches both `is_field_any` and
        `is_field_oneof`. `MCAPMsgDecoder.postprocess()` applies them in this same order.
        """
        return cls._descriptor_to_rules(descr, {}, ())

    @classmethod
    def _descriptor_to_rules(
        cls,
        descr: Descriptor,
        field_rule_mapper: Dict[RulePath, List[Rule]],
        path: RulePath,
    ) -> Dict[RulePath, List[Rule]]:

        for f in descr.fields:
            cls._field_to_rule(f, field_rule_mapper, path)

        return field_rule_mapper

    @classmethod
    def _field_to_rule(
        cls,
        field: FieldDescriptor,
        field_rule_mapper: Dict[RulePath, List[Rule]],
        path: RulePath,
    ):
        """
        Turns one protobuf FieldDescriptor into a list of Rules to be applied to that field
        by adding them to the passed field_rule_mapper
        """

        if is_field_map(field):
            # Maps: match/recurse the VALUE type at a MapValuesStep-terminated path, so it's
            # fixed up per-entry exactly like a repeated message element already is via
            # ListStep — reusing _to_rule unchanged. Map keys are left alone for now.
            if field.message_type is None:
                raise RuntimeError(
                    f"`{field.full_name}` is a map field but holds no message_type information."
                )
            value_field = field.message_type.fields_by_name["value"]
            cls._to_rule(
                value_field,
                field_rule_mapper,
                path + (FieldStep(field.name), MapValuesStep()),
            )
        elif field.is_repeated:
            cls._to_rule(
                field, field_rule_mapper, path + (FieldStep(field.name), ListStep())
            )
        else:
            cls._to_rule(field, field_rule_mapper, path + (FieldStep(field.name),))

    @classmethod
    def _to_rule(
        cls,
        field: FieldDescriptor,
        field_rule_mapper: Dict[RulePath, List[Rule]],
        path: RulePath,
    ):
        """Mapping a field (that is NEITHER a list NOR a map) and all its sub-fields into a Rule"""

        matched_rules = match_rules(field, cls.PROTOBUF_RULES)
        if matched_rules:
            field_rule_mapper[path] = matched_rules

        if (
            is_field_nested(field)
            and not is_field_timestamp(field)  # do NOT unpack Timestamps types
            and not is_field_any(field)  # do NOT unpack Any types
        ):
            if field.message_type is None:
                raise RuntimeError(
                    f"`{field.full_name}` of type {field.type} does not hold any information about its message type."
                )

            cls._descriptor_to_rules(field.message_type, field_rule_mapper, path)
