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


def test_schema_fingerprint_treats_string_view_as_string():
    # A query engine reading data back (e.g. DataFusion's Parquet reader) may
    # return pa.string_view/pa.large_string for a column the SDK declared as
    # pa.string. The fingerprint must be blind to that physical-representation
    # difference, or a correctly modeled ontology gets misidentified as Unmodeled.
    classic = pa.struct([pa.field("x", pa.string())])
    view = pa.struct([pa.field("x", pa.string_view())])
    large = pa.struct([pa.field("x", pa.large_string())])

    assert (
        _compute_schema_fingerprint(classic)
        == _compute_schema_fingerprint(view)
        == _compute_schema_fingerprint(large)
    )


def test_schema_fingerprint_treats_binary_view_as_binary():
    classic = pa.struct([pa.field("x", pa.binary())])
    view = pa.struct([pa.field("x", pa.binary_view())])
    large = pa.struct([pa.field("x", pa.large_binary())])

    assert (
        _compute_schema_fingerprint(classic)
        == _compute_schema_fingerprint(view)
        == _compute_schema_fingerprint(large)
    )


def test_schema_fingerprint_when_nested():
    # Ontology fields normalisazion should stop just at top level
    # and not influence inner types of structs

    classic_nested = pa.struct(
        [
            pa.field(
                "name_outer",
                pa.struct(
                    [
                        pa.field("name_inner", pa.string()),
                    ]
                ),
            ),
        ]
    )

    view_nested = pa.struct(
        [
            pa.field(
                "name_outer",
                pa.struct(
                    [
                        pa.field("name_inner", pa.string_view()),
                    ]
                ),
            ),
        ]
    )

    assert _compute_schema_fingerprint(classic_nested) != _compute_schema_fingerprint(
        view_nested
    )


def test_schema_fingerprint_when_list():
    # Ontology fields can be lists of strings (e.g. MosaicoType.list_(MosaicoType.string))
    #  and the normalization must NOT apply to nested types. It should be limited to top-level
    classic = pa.struct([pa.field("x", pa.list_(pa.string()))])
    view = pa.struct([pa.field("x", pa.list_(pa.string_view()))])

    assert _compute_schema_fingerprint(classic) != _compute_schema_fingerprint(view)
