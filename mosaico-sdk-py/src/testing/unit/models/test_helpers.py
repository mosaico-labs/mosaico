import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import pyarrow as pa
import pytest

from mosaicolabs.enum.serialization_format import SerializationFormat
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


def test_resolve_concurrent_schema_race_each_thread_gets_own_schema():
    # When two threads race to resolve the SAME tag with two DIFFERENT schemas
    # at (as close as possible to) the same instant, each thread must get back
    # the class matching ITS OWN schema. A naive double-checked-locking
    # implementation can hand the losing thread the winning thread's class if
    # the "recheck inside the lock" only verifies that *some* class exists
    # under the tag, without re-checking whether it actually matches the
    # caller's own schema.
    #
    # Repeated across many independent tags (via a `threading.Barrier` to force
    # near-simultaneous entry each time) since a single repetition might not
    # reliably hit the exact interleaving that triggers the bug.

    schema_a = pa.struct([pa.field("x", pa.float32())])
    schema_b = pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])

    for i in range(50):
        tag = f"test_helpers__concurrent_schema_race_tag_{i}"
        results = {}
        barrier = threading.Barrier(2)

        def worker(name, schema):
            barrier.wait()
            results[name] = resolve_ontology_class(ontology_tag=tag, schema=schema)

        t_a = threading.Thread(target=worker, args=("a", schema_a))
        t_b = threading.Thread(target=worker, args=("b", schema_b))
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        assert results["a"].__msco_pyarrow_struct__.equals(schema_a), (
            f"rep {i}: thread 'a' (schema_a) got a class with the wrong schema"
        )
        assert results["b"].__msco_pyarrow_struct__.equals(schema_b), (
            f"rep {i}: thread 'b' (schema_b) got a class with the wrong schema"
        )
        assert results["a"] is not results["b"]


def test_resolve_concurrent_multi_schema_stress_converges_correctly():

    # Broader, higher-fan-out version of the race above: many threads
    # concurrently resolve the SAME tag, each using one of three distinct
    # schemas. Every thread must end up with the class matching the schema it
    # actually passed in, and exactly one class must exist per distinct schema
    # (no duplicate/over-created variants, no cross-contamination between
    # schema groups).

    tag = "test_helpers__multi_schema_stress_tag"
    schemas = [
        pa.struct([pa.field("x", pa.float32())]),
        pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())]),
        pa.struct(
            [
                pa.field("x", pa.float32()),
                pa.field("y", pa.float32()),
                pa.field("z", pa.float32()),
            ]
        ),
    ]

    n_workers = 30
    barrier = threading.Barrier(n_workers)
    results: List[Optional[type[Serializable]]] = [None] * n_workers

    def worker(idx):
        schema = schemas[idx % len(schemas)]
        barrier.wait()
        results[idx] = resolve_ontology_class(ontology_tag=tag, schema=schema)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every worker's result must match the schema it actually requested.
    for idx, cls in enumerate(results):
        assert cls is not None
        expected_schema = schemas[idx % len(schemas)]
        assert cls.__msco_pyarrow_struct__.equals(expected_schema), (
            f"worker {idx} got a class with the wrong schema"
        )

    # Exactly one distinct class per distinct schema - no over-creation.
    distinct_classes = {id(cls) for cls in results}
    assert len(distinct_classes) == len(schemas)


def test_resolve_variant_creation_does_not_pollute_registry():
    # After resolving a canonical class and one schema variant for the same
    # tag, exactly those two registry keys should be newly registered - no
    # stray/duplicate entries from intermediate lock-free attempts.
    tag = "test_helpers__registry_integrity_tag"
    before = set(Serializable._list_registered())

    canonical = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)
    variant = resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V2)

    after = set(Serializable._list_registered())
    added = after - before

    assert added == {canonical.__registry_key__, variant.__registry_key__}


def test_resolve_repeat_call_does_not_mutate_already_registered_class():
    # A second call for the SAME tag+schema but with a DIFFERENT class_name/
    # serialization_format must return the already-registered class untouched
    # - not rename or reconfigure it. These params only matter the first time
    # a given (tag, schema) pair is actually created.
    tag = "test_helpers__no_mutation_tag"
    first = resolve_ontology_class(
        ontology_tag=tag,
        class_name="FirstName",
        schema=_SCHEMA_V1,
        serialization_format=SerializationFormat.Default,
    )

    second = resolve_ontology_class(
        ontology_tag=tag,
        class_name="SomeOtherName",
        schema=_SCHEMA_V1,
        serialization_format=SerializationFormat.Ragged,
    )

    assert second is first
    assert second.__name__ == "FirstName"
    assert second.__serialization_format__ == SerializationFormat.Default


def test_resolve_concurrent_lookup_without_schema_against_first_creation_does_not_crash():
    # A thread doing a schema-less lookup (`schema=None`) on a tag that another
    # thread is concurrently creating for the first time is an inherently racy
    # scenario: the lookup may legitimately succeed (if it happens to run after
    # the creation) or raise `ValueError` (if it runs before). Neither outcome
    # should crash with anything else, and a successful lookup must return a
    # fully valid, correctly registered class.

    tag = "test_helpers__racy_lookup_tag"
    barrier = threading.Barrier(2)
    outcome = {}

    def creator():
        barrier.wait()
        resolve_ontology_class(ontology_tag=tag, schema=_SCHEMA_V1)

    def lookup():
        barrier.wait()
        try:
            outcome["class"] = resolve_ontology_class(ontology_tag=tag)
        except ValueError as e:
            outcome["error"] = e

    t_creator = threading.Thread(target=creator)
    t_lookup = threading.Thread(target=lookup)
    t_creator.start()
    t_lookup.start()
    t_creator.join()
    t_lookup.join()

    # Exactly one outcome: a resolved class, or a clean ValueError - never both, never neither.
    assert ("class" in outcome) != ("error" in outcome)

    if "class" in outcome:
        cls = outcome["class"]
        assert cls.__ontology_tag__ == tag
        assert cls.__msco_pyarrow_struct__.equals(_SCHEMA_V1)
