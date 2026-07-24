import pyarrow as pa

from mosaicolabs.models.core.serializable import _compute_schema_fingerprint


def test_schema_fingerprint_is_deterministic_across_separate_instances():
    # Two independently-built pa.StructType instances with the same shape must
    # produce the same fingerprint, since this is the basis for detecting whether
    # two dynamically-resolved ontology classes represent the same schema.
    struct_a = pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])
    struct_b = pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])

    assert struct_a is not struct_b
    assert _compute_schema_fingerprint(struct_a) == _compute_schema_fingerprint(
        struct_b
    )


def test_schema_fingerprint_ignores_field_metadata():
    # Field metadata (e.g. descriptions) must not affect the fingerprint: two
    # schemas that are structurally identical but differ only in metadata should
    # be treated as the same schema variant.
    struct_with_metadata = pa.struct(
        [pa.field("x", pa.float32(), metadata={b"description": b"some value"})]
    )
    struct_without_metadata = pa.struct([pa.field("x", pa.float32())])

    assert _compute_schema_fingerprint(
        struct_with_metadata
    ) == _compute_schema_fingerprint(struct_without_metadata)


def test_schema_fingerprint_changes_with_field_name():
    base = pa.struct([pa.field("x", pa.float32())])
    renamed = pa.struct([pa.field("renamed", pa.float32())])

    assert _compute_schema_fingerprint(base) != _compute_schema_fingerprint(renamed)


def test_schema_fingerprint_changes_with_field_type():
    base = pa.struct([pa.field("x", pa.float32())])
    retyped = pa.struct([pa.field("x", pa.int32())])

    assert _compute_schema_fingerprint(base) != _compute_schema_fingerprint(retyped)


def test_schema_fingerprint_changes_with_nullability():
    nullable = pa.struct([pa.field("x", pa.float32(), nullable=True)])
    required = pa.struct([pa.field("x", pa.float32(), nullable=False)])

    assert _compute_schema_fingerprint(nullable) != _compute_schema_fingerprint(
        required
    )


def test_schema_fingerprint_changes_with_nesting():
    flat = pa.struct([pa.field("x", pa.float32())])
    nested = pa.struct([pa.field("x", pa.struct([pa.field("y", pa.float32())]))])

    assert _compute_schema_fingerprint(flat) != _compute_schema_fingerprint(nested)
