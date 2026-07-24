import pyarrow as pa
import pydantic
import pytest

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled, make_unmodeled_ontology_class

# Unmodeled ontology definition for local test usage
UnmodeledAcceleration = make_unmodeled_ontology_class(
    "UnmodeledAcceleration",
    None,
    SerializationFormat.Default,
    pa.struct(
        [
            pa.field(
                "acceleration",
                pa.struct(
                    [
                        # Left nullable (default) on purpose: exercises the "optional
                        # nested field can be omitted" case in the schema-mismatch tests.
                        pa.field("x", pa.float32()),
                        pa.field("y", pa.float32()),
                        pa.field("z", pa.float32()),
                    ]
                ),
                nullable=False,
                metadata={
                    "description": "Test Implementation of unmodeled accelerometer"
                },
            ),
        ]
    ),
)

# A second, distinct dynamic ontology with non-nullable nested fields, used to test
# that missing *required* nested fields are rejected (as opposed to the optional
# x/y/z fields above).
UnmodeledStrictVector = make_unmodeled_ontology_class(
    "UnmodeledStrictVector",
    "unmodeled_strict_vector",
    SerializationFormat.Default,
    pa.struct(
        [
            pa.field(
                "vector",
                pa.struct(
                    [
                        pa.field("x", pa.float32(), nullable=False),
                        pa.field("y", pa.float32(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
        ]
    ),
)


def test_unmodeled_subclass_has_Q_proxy():
    # Unmodeled derived class has Q proxy (__skip_query_proxy_ingestion__ = False)
    assert hasattr(UnmodeledAcceleration, "Q")
    assert hasattr(UnmodeledStrictVector, "Q")
    # Unmodeled class does not have Q proxy (__skip_query_proxy_ingestion__ = True)
    assert not hasattr(Unmodeled, "Q")


def test_unmodeled_class_serializable():
    # Test Serializability of unmodeled ontology
    assert UnmodeledAcceleration.is_registered()

    # Test queryability of pyarrow schema
    assert "timestamp_ns" in UnmodeledAcceleration.Q._queryable_fields
    assert "acceleration" in UnmodeledAcceleration.Q._queryable_fields
    assert all(
        item in UnmodeledAcceleration.Q.acceleration._queryable_fields
        for item in ("x", "y", "z")
    )

    # If this is not an expression, will raise
    UnmodeledAcceleration.Q.timestamp_ns.eq(0)
    UnmodeledAcceleration.Q.acceleration.x.eq(0)

    raw_data = {"acceleration": {"x": 0, "y": 0, "z": 0}}
    # Test embedding into a Message
    msg = Message(
        timestamp_ns=123456789,
        data=UnmodeledAcceleration(raw_data=raw_data),
    )

    # Test retrieving from Message
    # Convert to Actual type
    umodeled_acc = msg.get_data(UnmodeledAcceleration)
    assert umodeled_acc is not None
    assert umodeled_acc.raw_data == raw_data

    # Test retrieving from Message
    # Convert to base type, common to all unmodeled ontologies
    umodeled_acc = msg.get_data(Unmodeled)
    assert umodeled_acc is not None
    assert umodeled_acc.raw_data == raw_data


def test_unmodeled_missing_required_top_level_field_raises():
    # 'acceleration' is a required (non-nullable) top-level field
    with pytest.raises(
        pydantic.ValidationError,
        match="missing required field 'acceleration'",
    ):
        UnmodeledAcceleration(raw_data={})


def test_unmodeled_unknown_field_raises():
    raw_data = {
        "acceleration": {"x": 0, "y": 0, "z": 0},
        # Not part of the declared schema
        "extra_unexpected_field": 1,
    }
    with pytest.raises(
        pydantic.ValidationError,
        match="unknown field 'extra_unexpected_field'",
    ):
        UnmodeledAcceleration(raw_data=raw_data)


def test_unmodeled_nested_field_wrong_type_raises():
    # 'acceleration' must be a nested object, not a scalar
    with pytest.raises(
        pydantic.ValidationError,
        match="field 'acceleration' expected a nested object",
    ):
        UnmodeledAcceleration(raw_data={"acceleration": 9.81})


def test_unmodeled_optional_nested_field_can_be_omitted():
    # x/y/z are nullable: omitting one is valid, unlike the required 'acceleration' struct itself
    umodeled_acc = UnmodeledAcceleration(raw_data={"acceleration": {"x": 1, "y": 2}})
    assert umodeled_acc.raw_data == {"acceleration": {"x": 1, "y": 2}}


def test_unmodeled_missing_required_nested_field_raises():
    # 'vector.y' is a required (non-nullable) nested field
    with pytest.raises(
        pydantic.ValidationError,
        match=r"missing required field 'vector\.y'",
    ):
        UnmodeledStrictVector(raw_data={"vector": {"x": 1}})


def test_unmodeled_multiple_mismatches_are_all_reported():
    # Missing required 'acceleration' AND an unknown top-level field at once:
    # both errors should be surfaced together in a single exception.
    with pytest.raises(pydantic.ValidationError) as exc_info:
        UnmodeledAcceleration(raw_data={"unexpected": 1})

    err_msg = str(exc_info.value)
    assert "missing required field 'acceleration'" in err_msg
    assert "unknown field 'unexpected'" in err_msg


def test_unmodeled_class_creation_with_empty_schema():
    # Regression test: a schema-less ontology (e.g. a heartbeat/trigger message with
    # only a timestamp and no payload fields) must not crash.
    EmptyOntology = make_unmodeled_ontology_class(
        "UnmodeledEmptySchema",
        "unmodeled_empty_schema",
        SerializationFormat.Default,
        pa.struct([]),
    )

    assert EmptyOntology.__msco_pyarrow_struct__ == pa.struct([])
    assert EmptyOntology.__skip_schema_generation__ is True

    # Must be constructible with an empty payload, with no fields to mismatch against
    instance = EmptyOntology(raw_data={})
    assert instance.raw_data == {}
