use crate::types;
use std::{borrow::Borrow, collections::HashMap};

/// Floating point value type alias
pub type Float = f64;
/// Integer value type alias
pub type Integer = i64;
/// Timestam type alias
pub type Timestamp = types::Timestamp;
/// Literal type alias
pub type Text = String;

#[derive(Debug, thiserror::Error)]
pub enum OpError {
    /// Occurs when a field expects a specific type (e.g., String) but receives another (e.g., Numeric).
    #[error("wrong type")]
    WrongType,

    /// Unsupported operation
    #[error("unsupported operation")]
    UnsupportedOperation,

    /// Occurs when constructing a [`Range`] where `min > max`.
    #[error("empty range")]
    EmptyRange,
}

/// A wrapper enum to allow heterogeneous values (Numbers and Strings)
/// to coexist in dynamic containers like [`Metadata`].
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Integer(Integer),
    Float(Float),
    Text(Text),
    Boolean(bool),
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Integer(n)
    }
}

impl From<Timestamp> for Value {
    fn from(n: Timestamp) -> Self {
        Value::Integer(n.into())
    }
}

/// A trait that indicates which combinations of [`Value`]s and [`Op`]s
/// are supported by an implementing type.
///
/// Each method corresponds to a capability check for a particular
/// operation. By default, all operations are unsupported (`false`).
/// Implementors should override the methods for the operations they
/// support.
///
/// These checks are performed at **runtime**.
pub trait IsSupportedOp {
    fn support_eq(&self) -> bool {
        false
    }
    fn support_ordering(&self) -> bool {
        false
    }
    fn support_in(&self) -> bool {
        false
    }
    fn support_match(&self) -> bool {
        false
    }
}

impl IsSupportedOp for Value {
    fn support_eq(&self) -> bool {
        true
    }

    fn support_ordering(&self) -> bool {
        match self {
            Self::Text(_) => false,
            Self::Boolean(_) => false,
            Self::Integer(_) => true,
            Self::Float(_) => true,
        }
    }

    fn support_in(&self) -> bool {
        matches!(self, Self::Boolean(_))
    }

    fn support_match(&self) -> bool {
        matches!(self, Self::Text(_))
    }
}

impl IsSupportedOp for bool {
    fn support_eq(&self) -> bool {
        true
    }
}

impl IsSupportedOp for i64 {
    fn support_eq(&self) -> bool {
        true
    }

    fn support_ordering(&self) -> bool {
        true
    }

    fn support_in(&self) -> bool {
        true
    }
}

impl IsSupportedOp for types::Timestamp {
    fn support_eq(&self) -> bool {
        true
    }

    fn support_ordering(&self) -> bool {
        true
    }
}

impl IsSupportedOp for Text {
    fn support_eq(&self) -> bool {
        true
    }

    fn support_in(&self) -> bool {
        true
    }

    fn support_match(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Range<T> {
    pub min: T,
    pub max: T,
}

impl<T> Range<T>
where
    T: PartialOrd,
{
    pub fn try_new(min: T, max: T) -> Result<Self, OpError> {
        if min > max {
            return Err(OpError::EmptyRange);
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, Clone)]
pub struct OntologyField {
    value: String,
    tag_offset: usize,
}

impl PartialEq for OntologyField {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialEq<str> for OntologyField {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

impl Borrow<str> for OntologyField {
    fn borrow(&self) -> &str {
        &self.value
    }
}

impl Eq for OntologyField {}

impl std::hash::Hash for OntologyField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl OntologyField {
    pub fn try_new(v: String) -> Result<Self, super::Error> {
        let ontology_tag = v.split(".").next().ok_or_else(|| super::Error::BadField {
            field: v.to_string(),
        })?;
        let len = ontology_tag.len();

        Ok(Self {
            value: v,
            tag_offset: len,
        })
    }

    pub fn ontology_tag(&self) -> &str {
        &self.value[..self.tag_offset]
    }

    pub fn field(&self) -> &str {
        // +1 to remove the dot
        &self.value[(self.tag_offset + 1)..]
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

/// A container for dynamic user-defined key-value pairs.
///
/// This is used for fields where the schema is not known at compile time.
/// Keys are field names, and values are [`Op`]operations containing [`Value`]s.
#[derive(Debug, Clone)]
pub struct OntologyFilter(HashMap<OntologyField, Op<Value>>);

impl OntologyFilter {
    /// Creates a new Metadata instance from a [`HashMap`].
    pub fn new(v: HashMap<OntologyField, Op<Value>>) -> Self {
        Self(v)
    }

    /// Creates an empty Metadata instance.
    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    /// Retrieves the operation associated with a specific metadata field.
    pub fn get_op(&self, field: &str) -> Option<&Op<Value>> {
        self.0.get(field)
    }

    /// Custom function that calls `into_iter()` on the inner type
    pub fn into_iterator(self) -> impl Iterator<Item = (OntologyField, Op<Value>)> {
        self.0.into_iter()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&OntologyField, &Op<Value>)> {
        self.0.iter()
    }
}

/// Represents the logical operator to apply to a field for filtering.
#[derive(Debug, Clone, PartialEq)]
pub enum Op<T> {
    /// Equal
    Eq(T),
    /// Not equal
    Neq(T),
    /// Less than or equal
    Leq(T),
    /// Greater then or equal
    Geq(T),
    /// Lower then
    Lt(T),
    /// Greater then
    Gt(T),
    /// Exists
    Ex,
    /// Not exists
    Nex,
    /// In between a two value range [a, b] with a >= b
    Between(Range<T>),
    /// Found in a set
    In(Vec<T>),
    /// Matches a certain expression
    Match(T),
}

impl<T> Op<T>
where
    T: IsSupportedOp,
{
    pub fn is_supported_op(&self) -> bool {
        match self {
            Self::Eq(v) => v.support_eq(),
            Op::Neq(v) => v.support_eq(),
            Op::Leq(v) => v.support_ordering(),
            Op::Geq(v) => v.support_ordering(),
            Op::Lt(v) => v.support_ordering(),
            Op::Gt(v) => v.support_ordering(),
            Op::Ex => true,
            Op::Nex => true,
            Op::Between(range) => range.min.support_ordering(),
            Op::In(items) => items[0].support_in(),
            Op::Match(v) => v.support_match(),
        }
    }
}

/// The root object representing a complete search query.
///
/// A query allows filtering across three distinct domains:
/// 1. The sequence, as [`SequenceFilter`]
/// 2. The topic, as [`TopicFilter`]
/// 3. The data catalog, represented as [`DataCatalogFilter`]
///
/// All fields are optional; [`None`] implies no filtering for that domain.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub sequence: Option<SequenceFilter>,
    pub topic: Option<TopicFilter>,
    pub ontology: Option<OntologyFilter>,
}

impl Filter {
    /// Returns true if there are no filters applied
    pub fn is_empty(&self) -> bool {
        self.sequence.is_none() && self.topic.is_none() && self.ontology.is_none()
    }

    pub fn into_parts(
        self,
    ) -> (
        Option<SequenceFilter>,
        Option<TopicFilter>,
        Option<OntologyFilter>,
    ) {
        (self.sequence, self.topic, self.ontology)
    }
}

#[derive(Debug, Clone)]
pub struct SequenceFilter {
    pub name: Option<Op<Text>>,
    pub creation: Option<Op<Timestamp>>,
    pub user_metadata: Option<OntologyFilter>,
}

impl SequenceFilter {
    pub fn is_empty(&self) -> bool {
        self.name.is_none() && self.creation.is_none() && self.user_metadata.is_none()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicFilter {
    pub name: Option<Op<Text>>,
    pub creation: Option<Op<Timestamp>>,
    pub ontology_tag: Option<Op<Text>>,
    pub serialization_format: Option<Op<Text>>,
    pub user_metadata: Option<OntologyFilter>,
}

impl TopicFilter {
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.creation.is_none()
            && self.user_metadata.is_none()
            && self.ontology_tag.is_none()
            && self.serialization_format.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ontology_field() {
        let oc = OntologyField::try_new("image.info.height".into()).expect("");

        assert_eq!(oc.field(), "info.height");
        assert_eq!(oc.ontology_tag(), "image");
        assert_eq!(oc.value(), "image.info.height");
    }
}
