from typing import Any, Dict

from mosaicolabs.proto.v1 import query_pb2

_OPERATORS = {
    "$eq": query_pb2.OPERATOR_EQ,
    "$neq": query_pb2.OPERATOR_NE,
    "$lt": query_pb2.OPERATOR_LT,
    "$leq": query_pb2.OPERATOR_LE,
    "$gt": query_pb2.OPERATOR_GT,
    "$geq": query_pb2.OPERATOR_GE,
    "$between": query_pb2.OPERATOR_BETWEEN,
    "$outside": query_pb2.OPERATOR_OUTSIDE,
    "$in": query_pb2.OPERATOR_IN,
    "$match": query_pb2.OPERATOR_MATCH,
    "$ex": query_pb2.OPERATOR_EX,
    "$nex": query_pb2.OPERATOR_NEX,
}


def _value_to_proto(value: Any) -> query_pb2.Value:
    if isinstance(value, bool):
        return query_pb2.Value(boolean=value)
    if isinstance(value, int):
        return query_pb2.Value(integer=value)
    if isinstance(value, float):
        return query_pb2.Value(float=value)
    if isinstance(value, str):
        return query_pb2.Value(text=value)
    if isinstance(value, (list, tuple)) and value:
        # Lists are homogeneous, the query builders reject mixed types
        first = value[0]
        if isinstance(first, bool):
            return query_pb2.Value(boolean_array=query_pb2.BooleanArray(values=value))
        if isinstance(first, int):
            return query_pb2.Value(integer_array=query_pb2.IntegerArray(values=value))
        if isinstance(first, float):
            return query_pb2.Value(float_array=query_pb2.FloatArray(values=value))
        if isinstance(first, str):
            return query_pb2.Value(text_array=query_pb2.TextArray(values=value))
    raise TypeError(f"Unsupported query value: {value!r}")


def _condition_to_proto(expr: Dict[str, Any]) -> query_pb2.Condition:
    """Converts `{"$op": value}` into a `Condition`."""
    ((op, value),) = expr.items()
    if op not in _OPERATORS:
        raise ValueError(f"Unsupported query operator '{op}'")

    condition = query_pb2.Condition(op=_OPERATORS[op])
    # `$ex` / `$nex` carry no value
    if value is not None:
        condition.value.CopyFrom(_value_to_proto(value))
    return condition


def _fields_to_proto(fields: Dict[str, Any], msg) -> None:
    """Fills a `SequenceFilter` / `TopicFilter` from its query dictionary."""
    for key, expr in fields.items():
        if key == "user_metadata":
            for mkey, mexpr in expr.items():
                msg.user_metadata[mkey].CopyFrom(_condition_to_proto(mexpr))
        else:
            getattr(msg, key).CopyFrom(_condition_to_proto(expr))


def ontology_filter_to_proto(exprs: Dict[str, Any]) -> query_pb2.OntologyFilter:
    """Converts `{"IMU.acceleration.x": {"$gt": 5}, ...}`
    into an `OntologyFilter`.
    """
    return query_pb2.OntologyFilter(
        predicates=[
            query_pb2.OntologyPredicate(
                field=field, condition=_condition_to_proto(expr)
            )
            for field, expr in exprs.items()
        ]
    )


def filter_to_proto(query_dict: Dict[str, Any]) -> query_pb2.Filter:
    """Converts the `{"sequence": ..., "topic": ..., "ontology": ...}`
    dictionary into a `Filter`.
    """
    msg = query_pb2.Filter()
    if "sequence" in query_dict:
        _fields_to_proto(query_dict["sequence"], msg.sequence)
    if "topic" in query_dict:
        _fields_to_proto(query_dict["topic"], msg.topic)
    if "ontology" in query_dict:
        msg.ontology.CopyFrom(ontology_filter_to_proto(query_dict["ontology"]))
    return msg
