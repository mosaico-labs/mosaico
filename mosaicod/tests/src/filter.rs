//! Builders for the protobuf query filters used in tests.

use mosaicod_proto::v1::query as proto_query;
use proto_query::{Condition, Operator, value::Kind};
use std::collections::HashMap;

/// Converts a rust value into a protobuf [`proto_query::Value`].
pub trait IntoValue {
    fn into_value(self) -> proto_query::Value;
}

fn value(kind: Kind) -> proto_query::Value {
    proto_query::Value { kind: Some(kind) }
}

impl IntoValue for i32 {
    fn into_value(self) -> proto_query::Value {
        value(Kind::Integer(self.into()))
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> proto_query::Value {
        value(Kind::Integer(self))
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> proto_query::Value {
        value(Kind::Float(self))
    }
}

impl IntoValue for bool {
    fn into_value(self) -> proto_query::Value {
        value(Kind::Boolean(self))
    }
}

impl IntoValue for &str {
    fn into_value(self) -> proto_query::Value {
        value(Kind::Text(self.to_owned()))
    }
}

impl IntoValue for Vec<i64> {
    fn into_value(self) -> proto_query::Value {
        value(Kind::IntegerArray(proto_query::IntegerArray {
            values: self,
        }))
    }
}

impl<const N: usize> IntoValue for [i32; N] {
    fn into_value(self) -> proto_query::Value {
        self.map(i64::from).to_vec().into_value()
    }
}

impl<const N: usize> IntoValue for [i64; N] {
    fn into_value(self) -> proto_query::Value {
        self.to_vec().into_value()
    }
}

impl<const N: usize> IntoValue for [f64; N] {
    fn into_value(self) -> proto_query::Value {
        value(Kind::FloatArray(proto_query::FloatArray {
            values: self.to_vec(),
        }))
    }
}

impl<const N: usize> IntoValue for [bool; N] {
    fn into_value(self) -> proto_query::Value {
        value(Kind::BooleanArray(proto_query::BooleanArray {
            values: self.to_vec(),
        }))
    }
}

impl<const N: usize> IntoValue for [&str; N] {
    fn into_value(self) -> proto_query::Value {
        value(Kind::TextArray(proto_query::TextArray {
            values: self.map(str::to_owned).to_vec(),
        }))
    }
}

/// Builds a condition with an arbitrary (possibly missing) value, useful to
/// test malformed requests.
pub fn cond(op: Operator, value: Option<proto_query::Value>) -> Condition {
    Condition {
        op: op as i32,
        value,
    }
}

fn with_value(op: Operator, v: impl IntoValue) -> Condition {
    cond(op, Some(v.into_value()))
}

pub fn eq(v: impl IntoValue) -> Condition {
    with_value(Operator::Eq, v)
}

pub fn ne(v: impl IntoValue) -> Condition {
    with_value(Operator::Ne, v)
}

pub fn lt(v: impl IntoValue) -> Condition {
    with_value(Operator::Lt, v)
}

pub fn le(v: impl IntoValue) -> Condition {
    with_value(Operator::Le, v)
}

pub fn gt(v: impl IntoValue) -> Condition {
    with_value(Operator::Gt, v)
}

pub fn ge(v: impl IntoValue) -> Condition {
    with_value(Operator::Ge, v)
}

pub fn between(v: impl IntoValue) -> Condition {
    with_value(Operator::Between, v)
}

pub fn outside(v: impl IntoValue) -> Condition {
    with_value(Operator::Outside, v)
}

pub fn is_in(v: impl IntoValue) -> Condition {
    with_value(Operator::In, v)
}

pub fn matches(v: impl IntoValue) -> Condition {
    with_value(Operator::Match, v)
}

pub fn ex() -> Condition {
    cond(Operator::Ex, None)
}

pub fn nex() -> Condition {
    cond(Operator::Nex, None)
}

/// Builds a `user_metadata` filter map.
pub fn metadata<const N: usize>(conds: [(&str, Condition); N]) -> HashMap<String, Condition> {
    conds.into_iter().map(|(k, c)| (k.to_owned(), c)).collect()
}

/// Builds an ontology filter.
pub fn ontology_filter<const N: usize>(preds: [(&str, Condition); N]) -> proto_query::OntologyFilter {
    proto_query::OntologyFilter {
        predicates: preds
            .into_iter()
            .map(|(field, condition)| proto_query::OntologyPredicate {
                field: field.to_owned(),
                aggregator: proto_query::Aggregator::Unspecified as i32,
                condition: Some(condition),
            })
            .collect(),
    }
}
