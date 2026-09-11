from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

from mosaicolabs.logging_config import get_logger

T = TypeVar("T")

FieldPredicate = Callable[[T], bool]

# A rule's callback runs against either a dict keyed by field name, or (when the field is
# `repeated`, i.e. decoded to a list) a list indexed by position — Python's dict/list share
# the same subscript protocol, so callbacks don't need to distinguish the two.
FieldContainer = Union[Dict[str, Any], List[Any]]
FieldKey = Union[str, int]
FieldPostprocessor = Callable[[FieldContainer, FieldKey], None]


# Set the hierarchical logger
logger = get_logger(__name__)


@dataclass(frozen=True)
class Rule(Generic[T]):
    """
    A single postprocessing rule, applied by `MCAPMsgDecoder.postprocess()` to
    fix up one specific field of a decoded message dict so it complies with
    the target PyArrow schema.

    A `Rule` is used in two stages:

    1. **Schema inspection**, during `register_schema()`: an encoding-specific matcher (e.g.
       `ProtobufRulesMatcher.from_descriptor`) walks the schema and, for every field whose
       schema node satisfies `predicate` (checked via `is_respected`), associates that
       field's dot-separated path with this `Rule`. The result is a `Dict[str, List[Rule]]`
       (`MCAPMsgDecoder._field_rule_mapper`) mapping each field path to every `Rule` matched
       for it — a field can satisfy more than one predicate (e.g. an `Any` field that is also
       a `oneof` member), in which case all of them apply, in `PROTOBUF_RULES` order.
    2. **Postprocessing**, during `decode()`: `MCAPMsgDecoder.postprocess()` iterates that
       mapping and calls `rule.apply(data, path)` for every `(path, rule)` pair, so each
       `callback` runs only on the specific field `path` its rule was matched against — never
       on the rest of the decoded dict. If a field is absent and `callback` raises `KeyError`
       to signal that, `apply()` logs it and moves on, so a sibling rule registered for the
       same path (e.g. a `_fill_absent_field` fallback) still gets to run.
    """

    predicate: FieldPredicate
    callback: FieldPostprocessor

    def is_respected(self, node: T) -> bool:
        """Whether this rule's `predicate` matches `node` (a schema node, e.g. a protobuf
        `FieldDescriptor`), meaning this rule should be registered for that field."""
        return self.predicate(node)

    def apply(self, data: Dict[str, Any], path: str) -> None:
        """Locates every dict that directly contains dot-separated `path`'s leaf key and
        invokes `callback(container, leaf_key)` on each one, touching nothing else in `data`.
        A `repeated` protobuf field decodes to a list, so this transparently fans out over
        one wherever it's encountered — mid-path (e.g. a `repeated Inner` message field) or
        at the leaf itself (e.g. a `repeated int64` field, where `callback` then runs once
        per list index). This is what `MCAPMsgDecoder.postprocess()` calls for every rule
        registered against a given field path.

        `callback` may raise `KeyError` to signal that the field it needs is absent (e.g.
        `_coerce_int64_field` on a field `MessageToDict` omitted); that's caught and logged at
        debug level rather than propagated, so it never aborts postprocessing of the rest of
        `data`."""
        *parents, leaf = path.split(".")
        for container in self._resolve_containers(data, parents):
            try:
                self._apply_to_leaf(container, leaf)
            except KeyError as e:
                logger.debug(
                    f"Impossible to apply `{self.callback.__name__}` to `{path}` because: {e}"
                )

    def _resolve_containers(
        self, node: Any, parents: List[str]
    ) -> Iterator[Dict[str, Any]]:
        """Yields every dict reachable by walking `parents` from `node`, fanning out over any
        list encountered along the way: a repeated field's list doesn't correspond to its own
        path segment, so each element is walked with the same remaining `parents`.

        Stops (yields nothing) as soon as `node` is `None` or a step's key isn't found: an
        intermediate message field can itself be an unset singular message (surfaced as `None`
        by a `_fill_absent_field` rule, or simply absent if not yet processed), in which case
        there is nothing further down that path to postprocess."""
        if node is None:
            return
        if isinstance(node, list):
            for item in node:
                yield from self._resolve_containers(item, parents)
            return
        if not parents:
            yield node
            return
        head, *rest = parents
        if head not in node:
            return
        yield from self._resolve_containers(node[head], rest)

    def _apply_to_leaf(self, container: Dict[str, Any], leaf: str) -> None:
        """Runs `callback` on `container[leaf]`, or once per index if that value is itself a
        list (a repeated scalar/message/Any/Timestamp field). Does not itself check whether
        `leaf` is present in `container` — `callback` is invoked unconditionally, and it is
        each callback's own responsibility to handle (or reject, via `KeyError`) an absent
        field; see `_fill_absent_field` vs. `_coerce_int64_field`/`_modify_any_field`/
        `_modify_timestamp_field` in `rules_matcher.py`."""
        value = container.get(leaf)
        if isinstance(value, list):
            for index in range(len(value)):
                self.callback(value, index)
        else:
            self.callback(container, leaf)


def match_rules(node: T, rules: List[Rule[T]]) -> Optional[List[Rule[T]]]:
    """Returns every rule in `rules` whose predicate matches `node`, in the same relative
    order as `rules`. Returns `None`, not an empty list, if no rule applies."""
    matched_rules = []
    for rule in rules:
        if rule.is_respected(node):
            matched_rules.append(rule)

    return matched_rules if matched_rules else None
