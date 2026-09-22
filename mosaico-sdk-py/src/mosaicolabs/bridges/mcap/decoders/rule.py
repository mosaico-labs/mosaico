from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
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
class FieldStep:
    """Descend into container[name] — a struct field lookup (a decoded protobuf submessage
    dict, keyed by field name)."""

    name: str


@dataclass(frozen=True)
class ListStep:
    """Fan out over every element of the list found at this position (a decoded `repeated`
    field). As the last step, the list itself is the container and each index is the key
    (the rule's callback runs once per element); otherwise the remaining steps are resolved
    independently against each element."""


@dataclass(frozen=True)
class MapValuesStep:
    """Fan out over every value of the map dict found at this position (a decoded protobuf
    `map<K, V>` field). Unlike `FieldStep`, does not look up one named key — a map's keys are
    arbitrary data, not schema-known field names. As the last step, the map dict itself is
    the container and each key addresses one value; otherwise the remaining steps are
    resolved independently against each value."""


RulePath = Tuple[Union[FieldStep, ListStep, MapValuesStep], ...]


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
       field's `RulePath` with this `Rule`. The result is a `Dict[RulePath, List[Rule]]`
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

    def apply(self, data: Dict[str, Any], path: RulePath) -> None:
        """Resolves `path` against `data`, fanning out at every `ListStep`/`MapValuesStep`,
        and invokes `callback(container, key)` once per (container, key) pair `path`
        resolves to. This is what `MCAPMsgDecoder.postprocess()` calls for every rule
        registered against a given field path.

        `callback` may raise `KeyError` to signal that the field it needs is absent (e.g.
        `_coerce_int64_field` on a field `MessageToDict` omitted); that's caught and logged at
        debug level rather than propagated, so it never aborts postprocessing of the rest of
        `data`."""
        for container, key in self._resolve(data, path):
            try:
                self.callback(container, key)
            except KeyError as e:
                logger.debug(
                    f"Impossible to apply `{self.callback.__name__}` to `{path}` because: {e}"
                )

    def _resolve(
        self, node: Any, path: RulePath
    ) -> Iterator[Tuple[FieldContainer, FieldKey]]:
        """Yields every `(container, key)` pair `callback` should run on, resolving `path`
        against `node`. Stops (yields nothing) as soon as `node` is `None`: an intermediate
        message field can itself be an unset singular message (surfaced as `None` by a
        `_fill_absent_field` rule, or simply absent if not yet processed), in which case
        there is nothing further down that path to postprocess."""
        if node is None or not path:
            return

        step, *rest = path
        rest = tuple(rest)

        if isinstance(step, FieldStep):
            if not isinstance(node, dict):
                return
            if not rest:
                # Terminal: yield unconditionally, even if `step.name` is absent — the
                # callback decides (e.g. `_fill_absent_field` specifically wants to run when
                # the key is missing).
                yield node, step.name
                return
            if step.name not in node:
                return  # nothing further down this path to visit
            yield from self._resolve(node[step.name], rest)

        elif isinstance(step, ListStep):
            if not isinstance(node, list):
                return
            if not rest:
                for index in range(len(node)):
                    yield node, index
                return
            for item in node:
                yield from self._resolve(item, rest)

        elif isinstance(step, MapValuesStep):
            if not isinstance(node, dict):
                return
            if not rest:
                for key in list(node.keys()):
                    yield node, key
                return
            for key in list(node.keys()):
                yield from self._resolve(node[key], rest)


def match_rules(node: T, rules: List[Rule[T]]) -> Optional[List[Rule[T]]]:
    """Returns every rule in `rules` whose predicate matches `node`, in the same relative
    order as `rules`. Returns `None`, not an empty list, if no rule applies."""
    matched_rules = []
    for rule in rules:
        if rule.is_respected(node):
            matched_rules.append(rule)

    return matched_rules if matched_rules else None
