use mosaicod_query as query;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Value {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

impl From<Value> for query::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(v) => query::Value::Integer(v),
            Value::Float(v) => query::Value::Float(v),
            Value::Text(v) => query::Value::Text(v),
            Value::Boolean(v) => query::Value::Boolean(v),
        }
    }
}

impl TryInto<query::Text> for Value {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Text, Self::Error> {
        match self {
            Value::Text(v) => Ok(v),
            _ => Err(query::OpError::WrongType),
        }
    }
}

impl TryInto<query::Float> for Value {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Float, Self::Error> {
        match self {
            Value::Float(v) => Ok(v),
            _ => Err(Self::Error::WrongType),
        }
    }
}

impl TryInto<query::Integer> for Value {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Integer, Self::Error> {
        match self {
            Value::Integer(v) => Ok(v),
            _ => Err(Self::Error::WrongType),
        }
    }
}

impl TryInto<query::Timestamp> for Value {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Timestamp, Self::Error> {
        match self {
            Value::Integer(v) => Ok(v.into()),
            _ => Err(Self::Error::WrongType),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum Op {
    #[serde(rename = "$eq")]
    Eq(Value),
    #[serde(rename = "$neq")]
    Neq(Value),
    #[serde(rename = "$leq")]
    Leq(Value),
    #[serde(rename = "$geq")]
    Geq(Value),
    #[serde(rename = "$lt")]
    Lt(Value),
    #[serde(rename = "$gt")]
    Gt(Value),
    #[serde(rename = "$ex")]
    Ex,
    #[serde(rename = "$nex")]
    Nex,
    #[serde(rename = "$between")]
    Between([Value; 2]),
    #[serde(rename = "$in")]
    In(Vec<Value>),
    #[serde(rename = "$match")]
    Match(Value),
}

impl TryInto<query::Op<query::Text>> for Op {
    type Error = query::OpError;

    fn try_into(self) -> Result<query::Op<query::Text>, Self::Error> {
        Ok(match self {
            Op::Eq(v) => query::Op::Eq(v.try_into()?),
            Op::Neq(v) => query::Op::Neq(v.try_into()?),
            Op::Leq(_) => return Err(query::OpError::UnsupportedOperation),
            Op::Geq(_) => return Err(query::OpError::UnsupportedOperation),
            Op::Lt(_) => return Err(query::OpError::UnsupportedOperation),
            Op::Gt(_) => return Err(query::OpError::UnsupportedOperation),
            Op::Between(_) => return Err(query::OpError::UnsupportedOperation),
            Op::Ex => query::Op::Ex,
            Op::Nex => query::Op::Nex,
            Op::In(vec) => query::Op::In(
                vec.into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<_, _>>()?,
            ),
            Op::Match(v) => query::Op::Match(v.try_into()?),
        })
    }
}

impl TryInto<query::Op<query::Timestamp>> for Op {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Op<query::Timestamp>, Self::Error> {
        Ok(match self {
            Op::Eq(v) => query::Op::Eq(v.try_into()?),
            Op::Leq(v) => query::Op::Leq(v.try_into()?),
            Op::Neq(v) => query::Op::Neq(v.try_into()?),
            Op::Geq(v) => query::Op::Geq(v.try_into()?),
            Op::Lt(v) => query::Op::Lt(v.try_into()?),
            Op::Gt(v) => query::Op::Gt(v.try_into()?),
            Op::Ex => query::Op::Ex,
            Op::Nex => query::Op::Nex,
            Op::Between([min, max]) => {
                query::Op::Between(query::Range::try_new(min.try_into()?, max.try_into()?)?)
            }
            Op::In(vec) => query::Op::In(
                vec.into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<_, _>>()?,
            ),
            Op::Match(_) => return Err(Self::Error::UnsupportedOperation),
        })
    }
}

impl TryInto<query::Op<query::Value>> for Op {
    type Error = query::OpError;
    fn try_into(self) -> Result<query::Op<query::Value>, Self::Error> {
        Ok(match self {
            Op::Eq(v) => query::Op::Eq(v.into()),
            Op::Leq(v) => query::Op::Leq(v.into()),
            Op::Neq(v) => query::Op::Neq(v.into()),
            Op::Geq(v) => query::Op::Geq(v.into()),
            Op::Lt(v) => query::Op::Lt(v.into()),
            Op::Gt(v) => query::Op::Gt(v.into()),
            Op::Ex => query::Op::Ex,
            Op::Nex => query::Op::Nex,
            Op::Between([min, max]) => {
                query::Op::Between(query::Range::try_new(min.into(), max.into())?)
            }
            Op::In(vec) => query::Op::In(vec.into_iter().map(Into::into).collect()),
            Op::Match(v) => query::Op::Match(v.into()),
        })
    }
}

#[derive(Debug, Deserialize)]
struct Query {
    sequence: Option<Sequence>,
    topic: Option<Topic>,
    ontology: Option<Ontology>,
}

impl TryInto<query::Filter> for Query {
    type Error = query::Error;
    fn try_into(self) -> Result<query::Filter, Self::Error> {
        Ok(query::Filter {
            sequence: self.sequence.map(|v| v.try_into()).transpose()?,
            topic: self.topic.map(|v| v.try_into()).transpose()?,
            ontology: self.ontology.map(|v| v.try_into()).transpose()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Ontology {
    #[serde(flatten)]
    filter: HashMap<String, Op>,
    include_timestamp_range: Option<bool>,
}

impl Ontology {
    pub fn len(&self) -> usize {
        self.filter.len()
    }
    pub fn is_empty(&self) -> bool {
        self.filter.is_empty()
    }
}

impl TryInto<query::OntologyFilter> for Ontology {
    type Error = query::Error;
    fn try_into(self) -> Result<query::OntologyFilter, Self::Error> {
        let ontology = self
            .filter
            .into_iter()
            .map(|(col, op)| {
                let op = op.try_into().map_err(|e| query::Error::OpError {
                    field: col.clone(),
                    err: e,
                })?;

                let col = query::OntologyField::try_new(col)?;

                Ok::<(query::OntologyField, query::Op<query::Value>), Self::Error>((col, op))
            })
            .collect::<Result<_, _>>()?;

        let include_timestamp_range = self.include_timestamp_range.unwrap_or_default();

        Ok(query::OntologyFilter::new_with_timestamp_range(
            ontology,
            include_timestamp_range,
        ))
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.contains("--")
        && !key.contains("***")
        && key.bytes().all(|b| {
            b.is_ascii_alphanumeric() || [b' ', b'_', b'-', b'*', b'[', b']', b'.'].contains(&b)
        })
}

/// Utility function to convert deserialized user metadata
fn convert_user_metadata(
    user_metadata: Option<HashMap<String, Op>>,
) -> Result<HashMap<String, query::Op<query::Value>>, query::Error> {
    user_metadata
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            if !valid_key(&k) {
                return Err(query::Error::DeserializationError(format!(
                    "Found invalid key in user metadata query filter: {k}"
                )));
            }

            let v: query::Op<query::Value> = v.try_into().map_err(|e| query::Error::OpError {
                field: k.clone(),
                err: e,
            })?;

            Ok::<(String, query::Op<query::Value>), query::Error>((k, v))
        })
        .collect::<Result<_, _>>()
}

#[derive(Debug, Deserialize)]
struct Sequence {
    locator: Option<Op>,
    created_at_ns: Option<Op>,
    user_metadata: Option<HashMap<String, Op>>,
}

impl TryInto<query::SequenceFilter> for Sequence {
    type Error = query::Error;
    fn try_into(self) -> Result<query::SequenceFilter, Self::Error> {
        Ok(query::SequenceFilter {
            name: self
                .locator
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "sequence.locator".to_owned(),
                    err: e,
                })?,
            created_at: self
                .created_at_ns
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "sequence.created_at".to_owned(),
                    err: e,
                })?,
            user_metadata: convert_user_metadata(self.user_metadata)?,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct Topic {
    locator: Option<Op>,
    created_at_ns: Option<Op>,
    ontology_tag: Option<Op>,
    serialization_format: Option<Op>,
    user_metadata: Option<HashMap<String, Op>>,
}

impl TryInto<query::TopicFilter> for Topic {
    type Error = query::Error;

    fn try_into(self) -> Result<query::TopicFilter, Self::Error> {
        Ok(query::TopicFilter {
            name: self
                .locator
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "topic.locator".to_owned(),
                    err: e,
                })?,

            created_at: self
                .created_at_ns
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "topic.created_at".to_owned(),
                    err: e,
                })?,

            ontology_tag: self
                .ontology_tag
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "topic.ontology_tag".to_owned(),
                    err: e,
                })?,

            serialization_format: self
                .serialization_format
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| Self::Error::OpError {
                    field: "topic.serialization_format".to_owned(),
                    err: e,
                })?,

            user_metadata: convert_user_metadata(self.user_metadata)?,
        })
    }
}

pub fn query_filter_from_string(s: &str) -> Result<query::Filter, super::Error> {
    let query: Query =
        serde_json::from_str(s).map_err(|e| super::Error::DeserializationError(e.to_string()))?;
    let query: query::Filter = query
        .try_into()
        .map_err(|e: query::Error| super::Error::DeserializationError(e.to_string()))?;
    Ok(query)
}

pub fn query_filter_from_serde_value(v: serde_json::Value) -> Result<query::Filter, super::Error> {
    let query: Query =
        serde_json::from_value(v).map_err(|e| super::Error::DeserializationError(e.to_string()))?;
    let query: query::Filter = query
        .try_into()
        .map_err(|e: query::Error| super::Error::DeserializationError(e.to_string()))?;
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_user_metadata_none_returns_empty_map() {
        let result = convert_user_metadata(None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_convert_user_metadata_empty_map_returns_empty_map() {
        let input = Some(HashMap::new());
        let result = convert_user_metadata(input);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_convert_user_metadata_valid_metadata_success() {
        let mut input_map = HashMap::new();
        input_map.insert("valid_key_123".to_string(), Op::Eq(Value::Integer(42)));
        input_map.insert(
            "another-valid key*".to_string(),
            Op::Eq(Value::Text("hello".to_string())),
        );
        input_map.insert(
            "another.*.key".to_string(),
            Op::Eq(Value::Text("hello".to_string())),
        );
        input_map.insert(
            "**.glob_key".to_string(),
            Op::Eq(Value::Text("hello".to_string())),
        );
        input_map.insert(
            "**[*].glob_array_key".to_string(),
            Op::Eq(Value::Text("hello".to_string())),
        );

        let result = convert_user_metadata(Some(input_map));
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
        input_map.insert("".to_string(), Op::Eq(Value::Integer(42)));

        let result = convert_user_metadata(Some(input_map));
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
        input_map.insert("invalid--key".to_string(), Op::Eq(Value::Integer(42)));

        let result = convert_user_metadata(Some(input_map));
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
            Op::Eq(Value::Integer(42)),
        );

        let result = convert_user_metadata(Some(input_map));
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
            input_map.insert(key.to_string(), Op::Eq(Value::Integer(100)));

            let result = convert_user_metadata(Some(input_map));
            assert!(
                result.is_err(),
                "Expected key '{}' to fail validation, but it succeeded.",
                key
            );
        }
    }
}
