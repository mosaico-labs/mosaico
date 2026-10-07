use mosaicod_proto::v1::query as proto;
use mosaicod_query::{self as query, IsSupportedOp};
use std::collections::HashMap;

// ////////////////////////////////////////////////////////////////////////////
// Values
// ////////////////////////////////////////////////////////////////////////////

fn value(value: proto::Value) -> Result<query::Value, query::OpError> {
    use proto::value::Kind;
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

fn op<T: PartialOrd + IsSupportedOp>(
    condition: proto::Condition,
    scalar: fn(query::Value) -> Result<T, query::OpError>,
) -> Result<query::Op<T>, query::OpError> {
    use proto::Operator;

    let operator =
        Operator::try_from(condition.op).map_err(|_| query::OpError::UnsupportedOperation)?;

    let val = condition
        .value
        .ok_or(query::OpError::WrongType)
        .and_then(value);

    let op = match operator {
        Operator::Eq => query::Op::Eq(scalar(val?)?),
        Operator::Neq => query::Op::Neq(scalar(val?)?),
        Operator::Lt => query::Op::Lt(scalar(val?)?),
        Operator::Leq => query::Op::Leq(scalar(val?)?),
        Operator::Gt => query::Op::Gt(scalar(val?)?),
        Operator::Geq => query::Op::Geq(scalar(val?)?),
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

/// Converts an optional field condition.
fn optional_op<T: PartialOrd + IsSupportedOp>(
    field: &str,
    condition: Option<proto::Condition>,
    scalar: fn(query::Value) -> Result<T, query::OpError>,
) -> Result<Option<query::Op<T>>, query::Error> {
    condition
        .map(|c| op(c, scalar))
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
    m: HashMap<String, proto::Condition>,
) -> Result<HashMap<String, query::Op<query::Value>>, query::Error> {
    m.into_iter()
        .map(|(k, c)| {
            if !valid_key(&k) {
                return Err(query::Error::DeserializationError(format!(
                    "Found invalid key in user metadata query filter: {k}"
                )));
            }

            let op = op(c, Ok).map_err(|e| query::Error::OpError {
                field: k.clone(),
                err: e,
            })?;

            Ok((k, op))
        })
        .collect()
}

// ////////////////////////////////////////////////////////////////////////////
// Filters
// ////////////////////////////////////////////////////////////////////////////

fn sequence_filter(
    seq_filter: proto::SequenceFilter,
) -> Result<query::SequenceFilter, query::Error> {
    Ok(query::SequenceFilter {
        name: optional_op("sequence.name", seq_filter.name, text)?,
        created_at: optional_op("sequence.created_at", seq_filter.created_at_ns, timestamp)?,
        user_metadata: user_metadata(seq_filter.user_metadata)?,
    })
}

fn topic_filter(t: proto::TopicFilter) -> Result<query::TopicFilter, query::Error> {
    Ok(query::TopicFilter {
        name: optional_op("topic.name", t.name, text)?,
        created_at: optional_op("topic.created_at", t.created_at_ns, timestamp)?,
        ontology_tag: optional_op("topic.ontology_tag", t.ontology_tag, text)?,
        serialization_format: optional_op("topic.serialization_format", t.serialization_format, text)?,
        user_metadata: user_metadata(t.user_metadata)?,
    })
}

fn ontology_filter(o: proto::OntologyFilter) -> Result<query::OntologyFilter, query::Error> {
    // TODO aggregator
    let ontology = o
        .exprs
        .into_iter()
        .map(|p| {
            let condition = p.condition.ok_or_else(|| {
                query::Error::DeserializationError(format!(
                    "missing condition for ontology field `{}`",
                    p.field
                ))
            })?;

            let op = op(condition, Ok).map_err(|e| query::Error::OpError {
                field: p.field.clone(),
                err: e,
            })?;
            
            Ok((query::OntologyField::try_new(p.field)?, op).into())
        })
        .collect::<Result<_, query::Error>>()?;

    Ok(query::OntologyFilter::new(ontology))
}

fn to_err(e: query::Error) -> super::Error {
    super::Error::DeserializationError(e.to_string())
}

pub fn query_filter_from_proto(f: proto::Filter) -> Result<query::Filter, super::Error> {
    Ok(query::Filter {
        sequence: f
            .sequence
            .map(sequence_filter)
            .transpose()
            .map_err(to_err)?,
        topic: f.topic.map(topic_filter).transpose().map_err(to_err)?,
        ontology: f
            .ontology
            .map(ontology_filter)
            .transpose()
            .map_err(to_err)?,
    })
}

pub fn ontology_filter_from_proto(
    o: proto::OntologyFilter,
) -> Result<query::OntologyFilter, super::Error> {
    ontology_filter(o).map_err(to_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn condition(op: proto::Operator, kind: Option<proto::value::Kind>) -> proto::Condition {
        proto::Condition {
            op: op as i32,
            value: kind.map(|k| proto::Value { kind: Some(k) }),
        }
    }

    fn integers(values: Vec<i64>) -> Option<proto::value::Kind> {
        Some(proto::value::Kind::IntegerArray(proto::IntegerArray {
            values,
        }))
    }

    fn int_condition(op: proto::Operator, val: i64) -> proto::Condition {
        condition(op, Some(proto::value::Kind::Integer(val)))
    }

    fn text_condition(op: proto::Operator, val: &str) -> proto::Condition {
        condition(op, Some(proto::value::Kind::Text(val.to_owned())))
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
            int_condition(proto::Operator::Eq, 42),
        );
        input_map.insert(
            "another-valid key*".to_string(),
            text_condition(proto::Operator::Eq, "hello"),
        );
        input_map.insert(
            "another.*.key".to_string(),
            text_condition(proto::Operator::Eq, "hello"),
        );
        input_map.insert(
            "**.glob_key".to_string(),
            text_condition(proto::Operator::Eq, "hello"),
        );
        input_map.insert(
            "**[*].glob_array_key".to_string(),
            text_condition(proto::Operator::Eq, "hello"),
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
        input_map.insert("".to_string(), int_condition(proto::Operator::Eq, 42));

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
            int_condition(proto::Operator::Eq, 42),
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
            int_condition(proto::Operator::Eq, 42),
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
            input_map.insert(key.to_string(), int_condition(proto::Operator::Eq, 100));

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
        let ok = op(
            condition(proto::Operator::Between, integers(vec![1, 5])),
            Ok,
        )
        .unwrap();
        assert!(matches!(ok, query::Op::Between(_)));

        let err = op(
            condition(proto::Operator::Between, integers(vec![1, 5, 9])),
            Ok,
        );
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_between_rejects_empty_range() {
        let err = op(
            condition(proto::Operator::Between, integers(vec![5, 1])),
            Ok,
        );
        assert!(matches!(err, Err(query::OpError::EmptyRange)));
    }

    #[test]
    fn test_in_requires_an_array() {
        let ok = op(condition(proto::Operator::In, integers(vec![1, 2])), Ok).unwrap();
        assert_eq!(
            ok,
            query::Op::In(vec![query::Value::Integer(1), query::Value::Integer(2)])
        );

        let err = op(int_condition(proto::Operator::In, 42), Ok);
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_unspecified_and_unknown_operators_are_rejected() {
        let unspecified = op(int_condition(proto::Operator::Unspecified, 42), Ok);
        assert!(matches!(
            unspecified,
            Err(query::OpError::UnsupportedOperation)
        ));

        let unknown = op(
            proto::Condition {
                op: 99,
                ..int_condition(proto::Operator::Eq, 42)
            },
            Ok,
        );
        assert!(matches!(unknown, Err(query::OpError::UnsupportedOperation)));
    }

    #[test]
    fn test_missing_operand_is_rejected() {
        let err = op(condition(proto::Operator::Eq, None), Ok);
        assert!(matches!(err, Err(query::OpError::WrongType)));

        // `ex`/`nex` take no operand.
        assert_eq!(
            op(condition(proto::Operator::Ex, None), Ok).unwrap(),
            query::Op::Ex
        );
    }

    #[test]
    fn test_text_field_rejects_non_text_operand() {
        let err = op(int_condition(proto::Operator::Eq, 42), text);
        assert!(matches!(err, Err(query::OpError::WrongType)));
    }

    #[test]
    fn test_empty_filter_converts_to_empty_domain_filter() {
        let filter = query_filter_from_proto(proto::Filter::default()).unwrap();
        assert!(filter.is_empty());
    }
    
    #[test]
    fn test_text_field_rejects_ordering_operator() {
        let filter = proto::TopicFilter {
            name: Some(text_condition(proto::Operator::Gt, "topic")),
            ..Default::default()
        };
        let err = topic_filter(filter).unwrap_err();
        assert!(matches!(
            err,
            query::Error::OpError { ref field, err: query::OpError::UnsupportedOperation } if field == "topic.name"
        ));
    }
}