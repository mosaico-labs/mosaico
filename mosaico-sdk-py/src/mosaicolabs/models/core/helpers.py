import threading
from typing import Optional, Type

import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat

from .serializable import Serializable, _compute_schema_fingerprint
from .unmodeled import make_unmodeled_ontology_class

_creation_lock = threading.Lock()


def resolve_ontology_class(
    *,
    ontology_tag: str,
    schema: Optional[pa.StructType] = None,
    schema_fingerprint: Optional[str] = None,
    serialization_format: Optional[SerializationFormat] = None,
) -> Type[Serializable]:
    """
    Resolves an ontology tag to a concrete `Serializable` class, creating a
    dynamic `Unmodeled` fallback class on demand when no hand-authored class is
    registered for the tag.

    ### Schema Variants
    A single tag can end up associated with more than one schema shape within a
    single process (e.g. two rosbags recorded with different versions of the same
    ROS message type, both mapped to the same inferred ontology tag). When the
    schema passed in doesn't match the one already registered for `ontology_tag`,
    a distinct variant class is resolved (or created) instead of silently reusing
    the wrong schema. The variant still reports the *same* `ontology_tag` to the
    platform (so all of its data stays discoverable under one consistent tag);
    only its SDK-local `__registry_key__` differs, deterministically derived as
    `f"{ontology_tag}__{fingerprint}"`.

    Args:
        ontology_tag: The ontology identifier to resolve. If a `Serializable`
            class is already registered under this tag, it's returned directly
            (subject to the schema-variant check above); otherwise a dynamic
            `Unmodeled` class is created and registered under it.
        schema: The pyarrow schema of the incoming data. Required when
            `ontology_tag` isn't already registered, since it's needed to build
            the fallback class. When provided for an already-registered tag,
            it's compared against the registered schema to detect drift.
        schema_fingerprint: The fingerprint of `schema`, if the caller already
            computed it (e.g. once at stream-connect time, since the schema is
            invariant for the life of a stream). Avoids re-hashing `schema` on
            every call. Computed from `schema` on demand if omitted.
        serialization_format: The serialization format to use if a dynamic
            class needs to be created. Defaults to `SerializationFormat.Default`
            if omitted.

    Returns:
        The resolved `Serializable` class: the already-registered class for
            `ontology_tag`, a newly created `Unmodeled` fallback class, or a
            distinct schema-variant class, depending on the case above.

    Raises:
        ValueError: If `ontology_tag` isn't registered and no `schema` is
            provided to build a fallback class from.
    """
    DataClass = Serializable._get_class_type(ontology_tag)

    if DataClass is not None:
        if schema is None:
            return DataClass
        fingerprint = schema_fingerprint or _compute_schema_fingerprint(schema)
        if DataClass.__schema_fingerprint__ == fingerprint:
            return DataClass
    elif schema is None:
        raise ValueError(
            f"No ontology registered with tag '{ontology_tag}'. "
            f"Available tags: {Serializable._list_registered()}. "
            "Try passing a pyarrow schema for inferring a fallback ontology type."
        )

    # If here: DataClass is None, i.e. nothing usable was found lock-free,
    # so a class may need to be created (or a variant resolved).
    fingerprint = schema_fingerprint or _compute_schema_fingerprint(schema)

    # Acquire the lock and re-derive the registry key from *current* registry
    # state before trusting it; a racing thread resolving a different schema
    # for the same tag must never be handed back this thread's class (or vice versa).
    with _creation_lock:
        DataClass = Serializable._get_class_type(ontology_tag)
        if DataClass is not None and DataClass.__schema_fingerprint__ != fingerprint:
            # Schema drift under the same tag: resolve (or create) a dedicated
            # variant instead of silently decoding against the wrong schema.
            # The variant still reports `ontology_tag` to the platform; only
            # its local registry key is disambiguated.
            registry_key = f"{ontology_tag}__{fingerprint}"
        else:
            registry_key = ontology_tag

        DataClass = Serializable._get_class_type(registry_key)
        if DataClass is not None:
            return DataClass

        # NOTE: this happens only once per (ontology tag, schema fingerprint)
        # pair. Once created, the dynamic class is registered in the
        # Serializable factory under `registry_key`, so subsequent calls for
        # this tag+schema hit the lock-free fast path above.
        return make_unmodeled_ontology_class(
            class_name=registry_key,  # Ensure unique class name
            ontology_tag=ontology_tag,
            registry_key=registry_key if registry_key != ontology_tag else None,
            serialization_format=serialization_format or SerializationFormat.Default,
            pyarrow_schema=schema,
        )
