use mosaicod_proto::v1::query as proto_query;
use mosaicod_query::{self as query, IsSupportedOp};
use std::collections::HashMap;

// ////////////////////////////////////////////////////////////////////////////
// Values
// ////////////////////////////////////////////////////////////////////////////

fn value(value: proto_query::Value) -> Result<query::Value, query::OpError> {
    use proto_query::value::Kind;
    Ok(match value.kind.ok_or(query::OpError::WrongType)? {
        Kind::Integer(v) => query::Value::Integer(v),
        Kind::Float(v) => query::Value::Float(v),
        Kind::Text(v) => query::Value::Text(v),
        Kind::Boolean(v) => query::Value::Boolean(v),
        Kind::IntegerArray(a) => query::Value::IntegerArray(a.values),
        Kind::FloatArray(a) => query::Value::FloatArray(a.values),
        Kind::TextArray(a) => query::Value::TextArray(a.values),
        Kind::BooleanArray(a) => query::Value::BooleanArray(a.values),
    })
}

/// Scalar extractor for fields that only accept text
fn text(v: query::Value) -> Result<query::Text, query::OpError> {
    match v {
        query::Value::Text(v) => Ok(v),
        _ => Err(query::OpError::WrongType),
    }
}

/// Scalar extractor for timestamp fields
fn timestamp(v: query::Value) -> Result<query::Timestamp, query::OpError> {
    match v {
        query::Value::Integer(v) => Ok(v.into()),
        _ => Err(query::OpError::WrongType),
    }
}

/// Splits an array value into its scalar elements
fn array_elements(v: query::Value) -> Result<Vec<query::Value>, query::OpError> {
    Ok(match v {
        query::Value::IntegerArray(a) => a.into_iter().map(query::Value::Integer).collect(),
        query::Value::FloatArray(a) => a.into_iter().map(query::Value::Float).collect(),
        query::Value::TextArray(a) => a.into_iter().map(query::Value::Text).collect(),
        query::Value::BooleanArray(a) => a.into_iter().map(query::Value::Boolean).collect(),
        _ => return Err(query::OpError::WrongType),
    })
}

fn range<T: PartialOrd>(
    v: query::Value,
    scalar: fn(query::Value) -> Result<T, query::OpError>,
) -> Result<query::Range<T>, query::OpError> {
    let [min, max]: [query::Value; 2] = array_elements(v)?
        .try_into()
        .map_err(|_| query::OpError::WrongType)?;
    query::Range::try_new(scalar(min)?, scalar(max)?)
}

// ////////////////////////////////////////////////////////////////////////////
// Conditions
// ////////////////////////////////////////////////////////////////////////////

fn condition_from_proto<T: PartialOrd + IsSupportedOp>(
    condition: proto_query::Condition,
    scalar: fn(query::Value) -> Result<T, query::OpError>,
) -> Result<query::Op<T>, query::OpError> {
    use proto_query::Operator;

    let operator =
        Operator::try_from(condition.op).map_err(|_| query::OpError::UnsupportedOperation)?;

    let val = condition
        .value
        .ok_or(query::OpError::WrongType)
        .and_then(value);

    let op = match operator {
        Operator::Eq => query::Op::Eq(scalar(val?)?),
        Operator::Ne => query::Op::Neq(scalar(val?)?),
        Operator::Lt => query::Op::Lt(scalar(val?)?),
        Operator::Le => query::Op::Leq(scalar(val?)?),
        Operator::Gt => query::Op::Gt(scalar(val?)?),
        Operator::Ge => query::Op::Geq(scalar(val?)?),
        Operator::Match => query::Op::Match(scalar(val?)?),
        Operator::Ex => query::Op::Ex,
        Operator::Nex => query::Op::Nex,
        Operator::Between => query::Op::Between(range(val?, scalar)?),
        Operator::Outside => query::Op::Outside(range(val?, scalar)?),
        Operator::In => query::Op::In(
            array_elements(val?)?
                .into_iter()
                .map(scalar)
                .collect::<Result<_, _>>()?,
        ),
        Operator::Unspecified => return Err(query::OpError::UnsupportedOperation),
    };

    if !op.is_supported_op() {
        return Err(query::OpError::UnsupportedOperation);
    }

    Ok(op)
}

fn aggregator(aggr: Option<i32>) -> Result<Option<query::Aggregator>, query::AggregatorError> {
    use proto_query::Aggregator;

    let Some(aggr) = aggr else {
        return Ok(None);
    };

    let aggregator = match Aggregator::try_from(aggr) {
        Ok(Aggregator::Max) => query::Aggregator::Max,
        Ok(Aggregator::Min) => query::Aggregator::Min,
        Ok(Aggregator::Count) => query::Aggregator::Count,
        Ok(Aggregator::Range) => query::Aggregator::Range,
        Ok(Aggregator::First) => query::Aggregator::First,
        Ok(Aggregator::Last) => query::Aggregator::Last,
        Ok(Aggregator::Std) => query::Aggregator::Std,
        Ok(Aggregator::Mean) => query::Aggregator::Mean,
        Ok(Aggregator::Diff) => query::Aggregator::Diff,
        Ok(Aggregator::Rate) => query::Aggregator::Rate,
        Ok(Aggregator::P50) => query::Aggregator::P50,
        Ok(Aggregator::P90) => query::Aggregator::P90,
        Ok(Aggregator::P99) => query::Aggregator::P99,
        Ok(Aggregator::Unspecified) | Err(_) => {
            return Err(query::AggregatorError::UnsupportedAggregator);
        }
    };

    Ok(Some(aggregator))
}

/// Converts an optional field condition.
fn optional_condition_from_proto<T: PartialOrd + IsSupportedOp>(
    field: &str,
    condition: Option<proto_query::Condition>,
    scalar: fn(query::Value) -> Result<T, query::OpError>,
) -> Result<Option<query::Op<T>>, query::Error> {
    condition
        .map(|c| condition_from_proto(c, scalar))
        .transpose()
        .map_err(|err| query::Error::OpError {
            field: field.to_owned(),
            err,
        })
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.contains("--")
        && !key.contains("***")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b" _-*[].".contains(&b))
}

fn user_metadata(
    m: HashMap<String, proto_query::Condition>,
) -> Result<HashMap<String, query::Op<query::Value>>, query::Error> {
    m.into_iter()
        .map(|(k, c)| {
            if !valid_key(&k) {
                return Err(query::Error::DeserializationError(format!(
                    "Found invalid key in user metadata query filter: {k}"
                )));
            }

            let condition = condition_from_proto(c, Ok).map_err(|e| query::Error::OpError {
                field: k.clone(),
                err: e,
            })?;

            Ok((k, condition))
        })
        .collect()
}

// ////////////////////////////////////////////////////////////////////////////
// Filters
// ////////////////////////////////////////////////////////////////////////////

fn sequence_filter_from_proto(
    seq_filter: proto_query::SequenceFilter,
) -> Result<query::SequenceFilter, query::Error> {
    Ok(query::SequenceFilter {
        name: optional_condition_from_proto("sequence.name", seq_filter.name, text)?,
        created_at: optional_condition_from_proto(
            "sequence.created_at",
            seq_filter.created_at_ns,
            timestamp,
        )?,
        user_metadata: user_metadata(seq_filter.user_metadata)?,
    })
}

fn topic_filter_from_proto(
    t: proto_query::TopicFilter,
) -> Result<query::TopicFilter, query::Error> {
    Ok(query::TopicFilter {
        name: optional_condition_from_proto("topic.name", t.name, text)?,
        created_at: optional_condition_from_proto("topic.created_at", t.created_at_ns, timestamp)?,
        ontology_tag: optional_condition_from_proto("topic.ontology_tag", t.ontology_tag, text)?,
        serialization_format: optional_condition_from_proto(
            "topic.serialization_format",
            t.serialization_format,
            text,
        )?,
        user_metadata: user_metadata(t.user_metadata)?,
    })
}

fn to_err(e: query::Error) -> super::Error {
    super::Error::DeserializationError(e.to_string())
}

pub fn query_filter_from_proto(f: proto_query::Filter) -> Result<query::Filter, super::Error> {
    Ok(query::Filter {
        sequence: f
            .sequence
            .map(sequence_filter_from_proto)
            .transpose()
            .map_err(to_err)?,
        topic: f
            .topic
            .map(topic_filter_from_proto)
            .transpose()
            .map_err(to_err)?,
        ontology: f.ontology.map(ontology_filter_from_proto).transpose()?,
    })
}

pub fn ontology_filter_from_proto(
    o: proto_query::OntologyFilter,
) -> Result<query::OntologyFilter, super::Error> {
    let ontology = o
        .predicates
        .into_iter()
        .map(|p| {
            let proto_condition = p.condition.ok_or_else(|| {
                query::Error::DeserializationError(format!(
                    "missing condition for ontology field `{}`",
                    p.field
                ))
            })?;

            let condition =
                condition_from_proto(proto_condition, Ok).map_err(|e| query::Error::OpError {
                    field: p.field.clone(),
                    err: e,
                })?;

            let aggr = aggregator(p.aggregator).map_err(|e| query::Error::AggregatorError {
                field: p.field.clone(),
                err: e,
            })?;

            let field = query::OntologyField::try_new(p.field.clone())?;

            if let Some(aggr) = &aggr {
                aggr.validate(&field, &condition)
                    .map_err(|e| query::Error::AggregatorError {
                        field: p.field.clone(),
                        err: e,
                    })?;
            }

            Ok((field, condition, aggr).into())
        })
        .collect::<Result<_, query::Error>>()
        .map_err(to_err)?;

    Ok(query::OntologyFilter::new(ontology))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn condition(
        op: proto_query::Operator,
        kind: Option<proto_query::value::Kind>,
    ) -> proto_query::Condition {
        proto_query::Condition {
            op: op as i32,
            value: kind.map(|k| proto_query::Value { kind: Some(k) }),
        }
    }

    fn integers(values: Vec<i64>) -> Option<proto_query::value::Kind> {
        Some(proto_query::value::Kind::IntegerArray(
            proto_query::IntegerArray { values },
        ))
    }

    fn int_condition(op: proto_query::Operator, val: i64) -> proto_query::Condition {
        condition(op, Some(proto_query::value::Kind::Integer(val)))
    }

    fn text_condition(op: proto_query::Operator, val: &str) -> proto_query::Condition {
        condition(op, Some(proto_query::value::Kind::Text(val.to_owned())))
    }

    #[test]
    fn test_convert_user_metadata_empty_map_returns_empty_map() {
        let input = HashMap::new();
        let result = user_metadata(input);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_convert_user_metadata_valid_metadata_success() {
        let mut input_map = HashMap::new();
        input_map.insert(
            "valid_key_123".to_string(),
            int_condition(proto_query::Operator::Eq, 42),
        );
        input_map.insert(
            "another-valid key*".to_string(),
            text_condition(proto_query::Operator::Eq, "hello"),
        );
        input_map.insert(
            "another.*.key".to_string(),
            text_condition(proto_query::Operator::Eq, "hello"),
        );
        input_map.insert(
            "**.glob_key".to_string(),
            text_condition(proto_query::Operator::Eq, "hello"),
        );
        input_map.insert(
            "**[*].glob_array_key".to_string(),
            text_condition(proto_query::Operator::Eq, "hello"),
        );

        let result = user_metadata(input_map);
        assert!(result.is_ok());

        let output_map = result.unwrap();
        assert_eq!(output_map.len(), 5);
        assert_eq!(
            output_map.get("valid_key_123"),
            Some(&query::Op::Eq(query::Value::Integer(42)))
        );
        assert_eq!(
            output_map.get("another-valid key*"),
            Some(&query::Op::Eq(query::Value::Text("hello".to_string())))
        );
        assert_eq!(
            output_map.get("another.*.key"),
            Some(&query::Op::Eq(query::Value::Text("hello".to_string())))
        );
        assert_eq!(
            output_map.get("**.glob_key"),
            Some(&query::Op::Eq(query::Value::Text("hello".to_string())))
        );
        assert_eq!(
            output_map.get("**[*].glob_array_key"),
            Some(&query::Op::Eq(query::Value::Text("hello".to_string())))
        );
    }

    #[test]
    fn test_convert_user_metadata_invalid_key_empty() {
        let mut input_map = HashMap::new();
        input_map.insert("".to_string(), int_condition(proto_query::Operator::Eq, 42));

        let result = user_metadata(input_map);
        assert!(result.is_err());

        match result.unwrap_err() {
            query::Error::DeserializationError(msg) => {
                assert!(msg.contains("Found invalid key in user metadata query filter"));
            }
            _ => panic!("Expected DeserializationError"),
        }
    }

    #[test]
    fn test_convert_user_metadata_invalid_key_contains_double_dash() {
        let mut input_map = HashMap::new();
        input_map.insert(
            "invalid--key".to_string(),
            int_condition(proto_query::Operator::Eq, 42),
        );

        let result = user_metadata(input_map);
        assert!(result.is_err());

        if let query::Error::DeserializationError(msg) = result.unwrap_err() {
            assert!(msg.contains("invalid--key"));
        } else {
            panic!("Expected DeserializationError");
        }
    }

    #[test]
    fn test_convert_user_metadata_invalid_key_contains_more_than_two_consecutive_asterisks() {
        let mut input_map = HashMap::new();
        input_map.insert(
            "invalid_key.***.status".to_string(),
            int_condition(proto_query::Operator::Eq, 42),
        );

        let result = user_metadata(input_map);
        assert!(result.is_err());

        if let query::Error::DeserializationError(msg) = result.unwrap_err() {
            assert!(msg.contains("invalid_key.***.status"));
        } else {
            panic!("Expected DeserializationError");
        }
    }

    #[test]
    fn test_convert_user_metadata_invalid_key_non_ascii_or_forbidden_special_chars() {
        let invalid_keys = vec!["key#1", "user@name", "dollar$sign", "ñaca"];

        for key in invalid_keys {
            let mut input_map = HashMap::new();
            input_map.insert(
                key.to_string(),
                int_condition(proto_query::Operator::Eq, 100),
            );

            let result = user_metadata(input_map);
            assert!(
                result.is_err(),
                "Expected key '{}' to fail validation, but it succeeded.",
                key
            );
        }
    }

    #[test]
    fn test_between_requires_two_bounds() {
        let ok = condition_from_proto(
            condition(proto_query::Operator::Between, integers(vec![1, 5])),
            Ok,
        )
        .unwrap();
        assert!(matches!(ok, query::Op::Between(_)));

        let err = condition_from_proto(
            condition(proto_query::Operator::Between, integers(vec![1, 5, 9])),
            Ok,
        );
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_between_rejects_empty_range() {
        let err = condition_from_proto(
            condition(proto_query::Operator::Between, integers(vec![5, 1])),
            Ok,
        );
        assert!(matches!(err, Err(query::OpError::EmptyRange)));
    }

    #[test]
    fn test_in_requires_an_array() {
        let ok = condition_from_proto(
            condition(proto_query::Operator::In, integers(vec![1, 2])),
            Ok,
        )
        .unwrap();
        assert_eq!(
            ok,
            query::Op::In(vec![query::Value::Integer(1), query::Value::Integer(2)])
        );

        let err = condition_from_proto(int_condition(proto_query::Operator::In, 42), Ok);
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_unspecified_and_unknown_operators_are_rejected() {
        let unspecified =
            condition_from_proto(int_condition(proto_query::Operator::Unspecified, 42), Ok);
        assert!(matches!(
            unspecified,
            Err(query::OpError::UnsupportedOperation)
        ));

        let unknown = condition_from_proto(
            proto_query::Condition {
                op: 99,
                ..int_condition(proto_query::Operator::Eq, 42)
            },
            Ok,
        );
        assert!(matches!(unknown, Err(query::OpError::UnsupportedOperation)));
    }

    #[test]
    fn test_missing_operand_is_rejected() {
        let err = condition_from_proto(condition(proto_query::Operator::Eq, None), Ok);
        assert!(matches!(err, Err(query::OpError::WrongType)));

        // `ex`/`nex` take no operand.
        assert_eq!(
            condition_from_proto(condition(proto_query::Operator::Ex, None), Ok).unwrap(),
            query::Op::Ex
        );
    }

    #[test]
    fn test_text_field_rejects_non_text_operand() {
        let err = condition_from_proto(int_condition(proto_query::Operator::Eq, 42), text);
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_empty_filter_converts_to_empty_domain_filter() {
        let filter = query_filter_from_proto(proto_query::Filter::default()).unwrap();
        assert!(filter.is_empty());
    }

    #[test]
    fn test_text_field_rejects_ordering_operator() {
        let filter = proto_query::TopicFilter {
            name: Some(text_condition(proto_query::Operator::Gt, "topic")),
            ..Default::default()
        };
        let err = topic_filter_from_proto(filter).unwrap_err();
        assert!(matches!(
            err,
            query::Error::OpError { ref field, err: query::OpError::UnsupportedOperation } if field == "topic.name"
        ));
    }

    fn aggregated_filter(
        field: &str,
        aggregator: proto_query::Aggregator,
        condition: proto_query::Condition,
    ) -> Result<query::OntologyFilter, crate::Error> {
        ontology_filter_from_proto(proto_query::OntologyFilter {
            predicates: vec![proto_query::OntologyPredicate {
                field: field.to_owned(),
                aggregator: Some(aggregator as i32),
                condition: Some(condition),
            }],
        })
    }

    fn assert_aggregator_err(res: Result<query::OntologyFilter, crate::Error>, msg: &str) {
        let Err(crate::Error::DeserializationError(err)) = res else {
            panic!("expected a deserialization error, got: {res:?}");
        };
        assert!(err.contains(msg), "expected `{msg}`, got: {err}");
    }

    #[test]
    fn test_aggregator_accepts_supported_predicates() {
        use proto_query::{Aggregator, Operator};

        aggregated_filter("mock.x", Aggregator::Max, int_condition(Operator::Gt, 5)).unwrap();
        aggregated_filter("mock.x", Aggregator::Range, int_condition(Operator::Lt, 5)).unwrap();
        aggregated_filter(
            "mock.x",
            Aggregator::Range,
            condition(Operator::Outside, integers(vec![1, 5])),
        )
        .unwrap();
        aggregated_filter("mock.s", Aggregator::Min, text_condition(Operator::Lt, "b")).unwrap();
        aggregated_filter(
            "mock.s",
            Aggregator::Max,
            text_condition(Operator::Match, "a*"),
        )
        .unwrap();
    }

    #[test]
    fn test_aggregator_rejects_unsupported_operation() {
        use proto_query::{Aggregator, Operator};

        assert_aggregator_err(
            aggregated_filter(
                "mock.s",
                Aggregator::Range,
                text_condition(Operator::Match, "a*"),
            ),
            "unsupported operation for the given aggregator",
        );
        assert_aggregator_err(
            aggregated_filter("mock.x", Aggregator::Max, condition(Operator::Ex, None)),
            "unsupported operation for the given aggregator",
        );
    }

    #[test]
    fn test_aggregator_rejects_unsupported_value_type() {
        use proto_query::{Aggregator, Operator};

        assert_aggregator_err(
            aggregated_filter(
                "mock.s",
                Aggregator::Range,
                text_condition(Operator::Gt, "a"),
            ),
            "unsupported value type for the given aggregator",
        );

        for aggregator in [Aggregator::Max, Aggregator::Min, Aggregator::Range] {
            assert_aggregator_err(
                aggregated_filter(
                    "mock.b",
                    aggregator,
                    condition(Operator::Eq, Some(proto_query::value::Kind::Boolean(true))),
                ),
                "unsupported value type for the given aggregator",
            );
        }
    }

    #[test]
    fn test_aggregator_rejects_list_fields() {
        use proto_query::{Aggregator, Operator};

        for field in ["mock.v[?]", "mock.v[!]", "mock.v[0]"] {
            assert_aggregator_err(
                aggregated_filter(field, Aggregator::Max, int_condition(Operator::Gt, 5)),
                "aggregators are not supported on list fields",
            );
        }
    }
}
