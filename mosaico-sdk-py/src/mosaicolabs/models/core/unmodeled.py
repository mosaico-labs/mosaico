"""
Unmodeled Ontology Module.

This module lets the SDK ingest and query data schemas (ontologies) that have no
corresponding hand-authored Python class. The mosaico backend only requires data
to be representable as an Arrow schema - it has no notion of Python classes - so
`Unmodeled` bridges that gap by wrapping an arbitrary pyarrow schema into a
dynamically-generated [`Serializable`][mosaicolabs.models.core.Serializable]
subclass at runtime.

This is what makes it possible, for example, to ingest ROS bags via the
`ros_bridge` even when a given ROS message type has not been explicitly adapted
into an SDK class (see [`sensor_msgs.py`][mosaicolabs.ros_bridge.adapters]
for an example of an *adapted* type): the message's ROS schema is translated
into a pyarrow schema, which is then handed to
[`make_unmodeled_ontology_class`][mosaicolabs.models.core.unmodeled.make_unmodeled_ontology_class]
to produce a class that's fully serializable and ingestible via the normal
writing/reading workflow, with no adapter required.
"""

from typing import Any, Dict, List, Optional, Type

import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat

from .internal.helpers import _fix_empty_dicts
from .serializable import Serializable


def _diff_against_struct(
    data: Dict[str, Any], struct: pa.StructType, path: str = ""
) -> List[str]:
    """
    Recursively compares a raw payload dict against a pyarrow StructType.

    Walks `data` and `struct` in lockstep, following nested structs, and collects
    every mismatch found along the way instead of stopping at the first one.

    Args:
        data (Dict[str, Any]): The raw payload dict to validate (e.g. `Unmodeled.raw_data`).
        struct (pa.StructType): The pyarrow struct schema `data` is expected to conform to.
        path (str): The dotted field path accumulated so far, used to qualify nested
            field names in the returned error messages (e.g. `"gyro.x"`).
            Callers should omit this; it's populated internally during recursion.

    Returns:
        List[str]: A list of human-readable mismatch descriptions - missing required
            fields, unknown fields, and type mismatches on nested structs. Empty if
            `data` fully matches `struct`.
    """
    errors: List[str] = []
    schema_fields = {field.name: field for field in struct}
    provided_keys = set(data.keys())
    schema_keys = set(schema_fields.keys())

    for name in sorted(schema_keys - provided_keys):
        if not schema_fields[name].nullable:
            errors.append(f"missing required field '{path}{name}'")

    for name in sorted(provided_keys - schema_keys):
        errors.append(f"unknown field '{path}{name}' (not present in schema)")

    for name in sorted(provided_keys & schema_keys):
        field = schema_fields[name]
        value = data[name]
        if value is None or not isinstance(field.type, pa.StructType):
            continue
        if not isinstance(value, dict):
            errors.append(
                f"field '{path}{name}' expected a nested object matching "
                f"'{field.type}', got {type(value).__name__}"
            )
        else:
            errors.extend(_diff_against_struct(value, field.type, f"{path}{name}."))

    return errors


class Unmodeled(
    Serializable,
    skip_schema_generation=True,
    skip_query_proxy_ingestion=True,
):
    """
    Base class for ontology data that has no hand-authored Python class.

    Where a normal [`Serializable`][mosaicolabs.models.core.Serializable]
    subclass (e.g. `IMU`) declares one typed field per schema field, `Unmodeled`
    instead stores the entire payload in a single generic
    [`raw_data`][mosaicolabs.models.core.unmodeled.Unmodeled.raw_data] dict, and
    carries its actual Arrow schema as data (`__msco_pyarrow_struct__`) rather
    than deriving it from typed class fields. This lets a single class shape
    represent *any* ontology schema, decided entirely at runtime.

    Warning: Not meant to be subclassed directly
        Don't subclass `Unmodeled` by hand. Instead, use the
        [`make_unmodeled_ontology_class`][mosaicolabs.models.core.unmodeled.make_unmodeled_ontology_class]
        factory (or, in most cases, the higher-level
        [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class]
        helper) to generate a properly-configured subclass for a specific
        ontology tag and pyarrow schema.

    ### Validation
    Every `Unmodeled` instance validates
    [`raw_data`][mosaicolabs.models.core.unmodeled.Unmodeled.raw_data] against
    the class's declared `__msco_pyarrow_struct__` schema at construction time -
    missing required fields, unknown fields, and nested-object type mismatches
    all raise a `ValueError` immediately, rather than surfacing later as an
    opaque error during Arrow serialization.

    ### Querying with the **`.Q` Proxy**
    Classes generated via `make_unmodeled_ontology_class` are still fully
    queryable via the `.Q` proxy, exactly like a hand-authored ontology, since
    the proxy is built from the class's pyarrow schema rather than its Python
    field declarations.
    """

    raw_data: Dict[str, Any]
    """
    The full ontology payload, keyed by field name exactly as declared in the
    class's `__msco_pyarrow_struct__` schema. Nested struct fields are
    represented as nested dicts (e.g. `{"gyro": {"x": 1.0, "y": 2.0, "z": 3.0}}`).
    """

    def model_post_init(self, context: Any) -> None:
        """
        Validates `raw_data` against the class's declared pyarrow schema.

        Args:
            context (Any): The Pydantic validation context passed by the base class.

        Raises:
            ValueError: If `raw_data` is missing a required field, contains a
                field not present in the schema, or has a nested field whose
                value isn't a dict where the schema expects a nested struct.
                All mismatches found are reported together in one exception.
        """
        super().model_post_init(context)
        errors = _diff_against_struct(self.raw_data, self.__msco_pyarrow_struct__)
        if errors:
            raise ValueError(
                f"'{type(self).__name__}' raw_data does not match its declared "
                f"pyarrow schema:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    def _encode(self) -> Dict[str, Any]:
        """Returns `raw_data` as-is: it's already a plain, flat-friendly dict."""
        return self.raw_data

    @classmethod
    def _decode(cls, *_, **kwargs) -> "Unmodeled":
        """
        Reconstructs an instance from decoded Arrow row data.

        Args:
            **kwargs: The decoded field values for this row, keyed by field name.

        Returns:
            Unmodeled: A new instance with `raw_data` set to `kwargs`, after normalizing
                away the Parquet/Arrow deserialization artifact where an all-`None`
                nested struct comes back as e.g. `{"x": None, "y": None}` instead of
                plain `None` (see `_fix_empty_dicts`).
        """
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else {}
        return cls(raw_data=fixed_kwargs)


def make_unmodeled_ontology_class(
    class_name: str,
    ontology_tag: Optional[str],
    serialization_format: SerializationFormat,
    pyarrow_schema: pa.StructType,
    registry_key: Optional[str] = None,
) -> Type[Unmodeled]:
    """
    Dynamically creates an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled]
    subclass for a specific pyarrow schema.

    This is the factory that turns an arbitrary Arrow schema into a class the
    SDK can serialize, ingest, query and retrieve just like any hand-authored
    ontology - the schema is attached to the generated class as data
    (`__msco_pyarrow_struct__`) instead of being derived from Python field
    declarations.

    Note: `registry_key` is for advanced use only
        Most callers should omit `registry_key` entirely. It exists so that
        [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class]
        can create a *schema variant* of an existing `ontology_tag`: a second
        class reporting the same `ontology_tag` to the platform (so it remains
        discoverable under one consistent tag) while still occupying a
        distinct, collision-free key in the SDK's local class registry. See
        [`Serializable.__registry_key__`][mosaicolabs.models.core.Serializable]
        for the full rationale.

    Note: Schema generation is intentionally skipped
        The returned class is created with `skip_schema_generation=True`, so
        `pyarrow_schema` is used verbatim as `__msco_pyarrow_struct__` rather
        than being (re)derived from `Unmodeled.raw_data`'s `Dict[str, Any]`
        annotation, which wouldn't produce a useful schema on its own.
        `skip_query_proxy_ingestion` is left at its default (`False`), so the
        `.Q` query proxy is still generated from `pyarrow_schema`.

    Example:
        ```python
        import pyarrow as pa
        from mosaicolabs.enum import SerializationFormat
        from mosaicolabs.models.core.unmodeled import make_unmodeled_ontology_class

        UnmodeledGyro = make_unmodeled_ontology_class(
            class_name="UnmodeledGyro",
            ontology_tag="gyro_raw",
            serialization_format=SerializationFormat.Default,
            pyarrow_schema=pa.struct([
                pa.field("gyro", pa.struct([
                    pa.field("x", pa.float32()),
                    pa.field("y", pa.float32()),
                    pa.field("z", pa.float32()),
                ])),
            ]),
        )

        # Fully usable like any other ontology class, e.g.:
        # topic_writer.push(Message(timestamp_ns=..., data=UnmodeledGyro(
        #     raw_data={"gyro": {"x": 0.1, "y": 0.0, "z": -0.2}}
        # )))
        ```

    Args:
        class_name (str): The Python class name assigned to the generated class
            (e.g. shown in `repr()` and error messages).
        ontology_tag (Optional[str]): The unique ontology identifier to register the class
            under. If `None`, it's auto-generated from `class_name`, matching the behavior
            of a normal `Serializable` subclass.
        serialization_format (SerializationFormat): The batching/serialization strategy for topics
            using this ontology (see
            [`Serializable.__serialization_format__`][mosaicolabs.models.core.Serializable]).
        pyarrow_schema (pa.StructType): The Arrow struct schema describing the ontology's data
            payload, used verbatim as the class's `__msco_pyarrow_struct__`.
        registry_key (Optional[str]): Advanced/internal use - see the note above. Defaults to
            `None`, meaning the class's local registry key is `ontology_tag`
            itself (the common case for every direct caller of this factory).

    Returns:
        Type[Unmodeled]: A new `Unmodeled` subclass, already registered in the
            [`Serializable`][mosaicolabs.models.core.Serializable] factory under
            `registry_key` (or `ontology_tag`, if `registry_key` is omitted), and
            reporting `ontology_tag` to the platform either way.

    Raises:
        ValueError: If the resolved registry key is already registered for a
            different class.
    """
    return type(
        class_name,
        (Unmodeled,),
        {
            "__ontology_tag__": ontology_tag,
            "__registry_key__": registry_key,
            "__serialization_format__": serialization_format,
            "__msco_pyarrow_struct__": pyarrow_schema,
        },
        skip_schema_generation=True,  # keep skip_query_proxy_ingestion=False (default)
    )
