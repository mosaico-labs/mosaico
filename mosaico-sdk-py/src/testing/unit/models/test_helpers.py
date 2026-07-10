from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest

from mosaicolabs.models.core.helpers import resolve_ontology_class
from mosaicolabs.models.core.serializable import Serializable

from .my_project import RegisteredSensor

_SCHEMA_V1 = pa.struct([pa.field("x", pa.float32())])
_SCHEMA_V2 = pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])


def test_resolve_known_tag_without_schema_returns_registered_class():
    resolved = resolve_ontology_class(ontology_tag=RegisteredSensor.ontology_tag())
    assert resolved is RegisteredSensor


def test_resolve_unknown_tag_without_schema_raises():
    with pytest.raises(ValueError, match="No ontology registered with tag"):
        resolve_ontology_class(ontology_tag="test_helpers__never_registered_tag")


def test_resolve_unknown_tag_with_schema_creates_class():
    tag = "test_helpers__new_tag"
    resolved = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)

    assert resolved.__ontology_tag__ == tag
    assert resolved.__msco_pyarrow_struct__.equals(_SCHEMA_V1)
    assert Serializable._get_class_type(tag) is resolved


def test_resolve_same_tag_same_schema_is_idempotent():
    tag = "test_helpers__idempotent_tag"
    first = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)
    second = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)

    # Must be the exact same class object - not a second class re-created under the same tag
    # (which would have raised "Duplicate ontology registry key" during registration anyway).
    assert first is second


def test_resolve_schema_variant_creates_distinct_class():
    tag = "test_helpers__variant_tag"
    canonical = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)
    variant = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)

    assert variant is not canonical
    # Both report the SAME ontology tag to the platform - only their SDK-local
    # registry key differs. This is what keeps both variants' data discoverable
    # under one consistent tag server-side.
    assert canonical.__ontology_tag__ == tag
    assert variant.__ontology_tag__ == tag
    assert canonical.__registry_key__ == tag
    assert variant.__registry_key__ == f"{tag}__{variant.__schema_fingerprint__}"
    assert variant.__msco_pyarrow_struct__.equals(_SCHEMA_V2)


def test_resolve_schema_variant_reports_same_ontology_tag_via_public_accessor():
    # The public .ontology_tag() accessor - the one used to build server-facing
    # queries and topic_create payloads - must agree for both variants.
    tag = "test_helpers__variant_public_tag"
    canonical = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)
    variant = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)

    assert canonical.ontology_tag() == variant.ontology_tag() == tag
    assert canonical.__registry_key__ != variant.__registry_key__


def test_resolve_schema_variant_is_idempotent():
    tag = "test_helpers__variant_idempotent_tag"
    resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)  # canonical

    variant_first = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)
    variant_second = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)

    assert variant_first is variant_second


def test_resolve_no_schema_after_variant_created_still_returns_canonical():
    tag = "test_helpers__variant_canonical_lookup_tag"
    canonical = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)
    resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)  # creates a variant

    # A schema-less lookup by the base tag must still resolve to the original,
    # canonical class - not to whichever variant happened to be created most recently.
    assert resolve_ontology_class(ontology_tag=tag) is canonical


def test_resolve_wrong_precomputed_fingerprint_is_honored_over_recomputation():
    # If a caller passes a precomputed `schema_fingerprint` that does not actually
    # match `schema`, resolve_ontology_class must trust the precomputed value
    # rather than silently recomputing it - otherwise a stale/incorrect cached
    # fingerprint (e.g. a bug in a caller) would go undetected.
    tag = "test_helpers__wrong_fingerprint_tag"
    canonical = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)

    bogus_fingerprint = "0" * 10
    resolved = resolve_ontology_class(
        ontology_tag=tag,
        schema=_SCHEMA_V1,
        schema_fingerprint=bogus_fingerprint,
    )

    assert resolved is not canonical
    assert resolved.__ontology_tag__ == tag
    assert resolved.__registry_key__ == f"{tag}__{bogus_fingerprint}"


def test_resolve_ontology_class_is_thread_safe():
    # Regression test for the check-then-create race: many threads resolving the
    # same brand-new tag concurrently must all get the identical class object,
    # with no "Duplicate ontology registry key" exception from a lost race.
    tag = "test_helpers__concurrent_tag"

    def _resolve():
        return resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(lambda _: _resolve(), range(64)))

    assert all(cls is results[0] for cls in results)
