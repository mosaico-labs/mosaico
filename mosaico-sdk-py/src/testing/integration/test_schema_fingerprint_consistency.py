"""
Schema Fingerprint Consistency Test.

Verifies that, for every concrete ontology model in `mosaicolabs.models.data`,
`mosaicolabs.models.futures`, and `mosaicolabs.models.sensors` (excluding
`Unmodeled`), the schema the SDK declares when creating a topic and the schema
the server reports back when that topic is later inspected are the same
schema, from the fingerprint's point of view (i.e. `_compute_schema_fingerprint`
of the two structs agree). This is a real, server-round-trip check that the
platform doesn't silently reshape, reorder, widen, or otherwise alter a
declared schema on its way to storage and back.

Every model class is filled automatically via `make_dummy_instance()` -
introspecting its pydantic fields - so no per-class instantiation code needs
to be hand-written or kept in sync as new ontologies are added.
"""

import importlib
import inspect
import pkgutil
from typing import Dict, Type

import mosaicolabs.models.data as data_pkg
import mosaicolabs.models.futures as futures_pkg
import mosaicolabs.models.sensors as sensors_pkg
from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.serializable import (
    Serializable,
    _compute_schema_fingerprint,
)
from mosaicolabs.models.core.unmodeled import Unmodeled
from testing.integration.dummy_instances import make_dummy_instance

# A handful of ontology classes enforce cross-field invariants (e.g. `Range`'s
# `min_range <= range <= max_range`) that independent, per-field dummy value
# generation can't be expected to satisfy generically. Explicit field
# overrides for those specific classes go here.
_FIELD_OVERRIDES: Dict[str, Dict[str, float]] = {
    "Range": {"min_range": 0.0, "max_range": 100.0, "range": 50.0},
}

# Known, tracked server-side limitation: a topic whose schema contains a
# doubly-nested variable-length list (`List[List[float]]`, as used by
# `MultiEchoLaserScan.ranges`/`.intensities`, i.e. `list<item: list<item:
# float>>`) fails when the platform finalizes the topic, even for a single,
# well-formed row. The client sees an opaque gRPC-wrapped "Internal error:
# undefined"; the server's own log reports the actual cause: "unable to cast
# arrow array to numeric type" - consistent with the server's ingestion path
# only expecting one level of list nesting and attempting to cast the *inner*
# list (still a list of floats) directly to a numeric array, as if the schema
# were only singly-nested. Reproduced in isolation (a single topic, no other
# classes involved) - this is a platform bug, not a test/data issue. Excluded
# here until it's fixed server-side, so it doesn't block verifying the other
# ontology classes.
# FIXME: to be removed
_KNOWN_UNSUPPORTED_CLASSES = {"MultiEchoLaserScan"}


def _discover_ontology_classes(*packages) -> Dict[str, Type[Serializable]]:
    """
    Discovers every concrete `Serializable` subclass defined directly within
    the given packages (recursing into submodules), excluding `Serializable`
    itself and `Unmodeled`.
    """
    found: Dict[str, Type[Serializable]] = {}
    for package in packages:
        for _, modname, _ in pkgutil.walk_packages(
            package.__path__, package.__name__ + "."
        ):
            module = importlib.import_module(modname)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, Serializable)
                    and obj is not Serializable
                    and obj is not Unmodeled
                    and obj.__module__ == modname  # defined here, not just imported
                ):
                    found[name] = obj
    return found


def test_schema_fingerprint_consistency_across_all_ontologies(
    mosaico_client: MosaicoClient,
):
    ontology_classes = _discover_ontology_classes(data_pkg, futures_pkg, sensors_pkg)
    assert ontology_classes, "Expected to discover at least one ontology class"
    ontology_classes = {
        name: cls
        for name, cls in ontology_classes.items()
        if name not in _KNOWN_UNSUPPORTED_CLASSES
    }

    sequence_name = "schema_fingerprint_consistency_seq"

    with mosaico_client:
        with mosaico_client.sequence_create(
            sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
        ) as seqw:
            for name, cls in ontology_classes.items():
                instance = make_dummy_instance(
                    cls, overrides=_FIELD_OVERRIDES.get(name)
                )
                topic_name = f"/schema_check/{name}"
                tw = seqw.topic_create(topic_name, {}, cls)
                assert tw is not None, f"Failed to create topic for {name}"
                tw.push(Message(timestamp_ns=1, data=instance))

        mismatches = []
        for name, cls in ontology_classes.items():
            topic_name = f"/schema_check/{name}"
            th = mosaico_client.topic_handler(sequence_name, topic_name)
            assert th is not None, f"Failed to open topic handler for {name}"

            sent_fingerprint = _compute_schema_fingerprint(cls.__msco_pyarrow_struct__)
            received_fingerprint = _compute_schema_fingerprint(th.ontology_schema)

            if sent_fingerprint != received_fingerprint:
                mismatches.append(
                    f"{name}: sent={sent_fingerprint} vs received={received_fingerprint}\n"
                    f"  sent schema:     {cls.__msco_pyarrow_struct__}\n"
                    f"  received schema: {th.ontology_schema}"
                )

        # Free resources before asserting, so a failure doesn't leak the sequence.
        mosaico_client.sequence_delete(sequence_name)

        assert not mismatches, "Schema fingerprint mismatches found:\n" + "\n".join(
            mismatches
        )
