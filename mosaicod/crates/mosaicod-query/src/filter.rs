//! # Search and Filtering Primitives
//!
//! This module provides the building blocks for constructing dynamic, type-safe search queries
//! and filters across sequences, topics, and ontology data.
//!
//! The filtering logic is built hierarchically using four core concepts:
//!
//! -   _Value_: the unit of data.
//!     A wrapper ([`Value`]) that allows heterogeneous types (Integers, Floats, Strings, Booleans)
//!     to be treated uniformly within dynamic containers.
//!
//! -   _Operation_ ([`Op`]): the logical predicate.
//!     An [`Op`] defines *how* to compare data. It represents specific conditions like equality
//!     (`Eq`), ranges (`Between`), set membership (`In`), or existence (`Ex`).
//!
//! -   _Expression_ ([`Expr`]): the single constraint.
//!     An expression is formed by binding a specific identifier (a field name or [`OntologyField`])
//!     to an [`Op`]. It asserts a rule for that specific field (e.g., *"temperature > 25.0"*).
//!
//! -   _Filter_: the composite query.
//!     A [`Filter`] is a collection of expressions grouped by domain (Sequence, Topic, Ontology,
//!     ...).
//!     It represents the complete set of criteria required to match a specific resource.
//!

use mosaicod_core::types;
use std::{collections::HashMap, collections::hash_map::Entry};

/// Floating point value type alias
pub type Float = f64;
/// Integer value type alias
pub type Integer = i64;
/// Timestam type alias
pub type Timestamp = types::Timestamp;
/// Text type alias
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

    /// Occurs when `Op::In` is constructed with an empty list of values.
    #[error("in operator requires at least one value")]
    EmptyIn,

    /// Occurs when `Op::Match` is constructed with an empty pattern string.
    #[error("match pattern cannot be empty")]
    EmptyPattern,

    /// Occurs when a plain list `eq`/`neq` literal exceeds the maximum unrollable size.
    #[error("list literal exceeds the maximum size of {max} for equality comparison")]
    ListTooLarge { max: usize },

    /// Occurs when `Op::Match` is constructed with a pattern exceeding the maximum length.
    #[error("match pattern too long")]
    PatternTooLong,

    /// Occurs when `Op::Match` is constructed with a malformed pattern.
    #[error("match pattern conversion to POSIX regex error: {0}")]
    MalformedPattern(String),
}

/// A wrapper enum to allow heterogeneous values (Numbers and Strings)
/// to coexist in dynamic containers like [`Metadata`].
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Integer(Integer),
    Float(Float),
    Text(Text),
    Boolean(bool),
    IntegerArray(Vec<Integer>),
    FloatArray(Vec<Float>),
    TextArray(Vec<Text>),
    BooleanArray(Vec<bool>),
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_owned())
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
            Self::Text(_) => true,
            Self::Boolean(_) => false,
            Self::Integer(_) => true,
            Self::Float(_) => true,
            Self::IntegerArray(_)
            | Self::FloatArray(_)
            | Self::TextArray(_)
            | Self::BooleanArray(_) => false,
        }
    }

    fn support_in(&self) -> bool {
        matches!(
            self,
            Self::Integer(_) | Self::Float(_) | Self::Text(_) | Self::Boolean(_)
        )
    }

    fn support_match(&self) -> bool {
        matches!(self, Self::Text(_))
    }
}

impl IsSupportedOp for bool {
    fn support_eq(&self) -> bool {
        true
    }

    fn support_in(&self) -> bool {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexSpecifier {
    /// Access the element at a specific position: [0], [42].
    At(usize),
    /// At least one element must satisfy the predicate: [?].
    Any,
    /// Every element must satisfy the predicate: [!].
    All,
}

impl std::fmt::Display for IndexSpecifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexSpecifier::At(n) => write!(f, "[{n}]"),
            IndexSpecifier::Any => write!(f, "[?]"),
            IndexSpecifier::All => write!(f, "[!]"),
        }
    }
}

/// The position and specifier of the list field within an [`OntologyFieldPath`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListAccess {
    /// Index into [`OntologyFieldPath::segments`] of the list field.
    pub segment_index: usize,
    pub specifier: IndexSpecifier,
}

/// The structured representation of an ontology field path after parsing.
///
/// At most one segment may carry a list specifier ([?], [!], [N]).
/// After the list field, only plain struct navigation is allowed.
///
/// Examples:
/// - "x"                     segments: ["x"],                      list_access: None
/// - "x[?]"                  segments: ["x"],                      list_access: Some { index: 0, Any }
/// - "x[30]"                 segments: ["x"],                      list_access: Some { index: 0, At(30) }
/// - "acceleration.x"        segments: ["acceleration","x"],       list_access: None
/// - "acceleration.x[!]"     segments: ["acceleration","x"],       list_access: Some { index: 1, All }
/// - "acceleration[?].x"     segments: ["acceleration","x"],       list_access: Some { index: 0, Any }
/// - "robot.pose[?].acc.x"   segments: ["robot","pose","acc","x"], list_access: Some { index: 1, Any }
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OntologyFieldPath {
    /// All field name segments in order, without specifiers.
    segments: Vec<String>,
    /// Which segment (if any) is the list field and its specifier.
    pub list_access: Option<ListAccess>,
}

impl OntologyFieldPath {
    /// Returns an iterator over all dot-path segment names in order.
    pub fn field_segments(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().map(|s| s.as_str())
    }

    /// Returns the specifier of the list field, if any.
    pub fn specifier(&self) -> Option<&IndexSpecifier> {
        self.list_access.as_ref().map(|la| &la.specifier)
    }
}

impl std::fmt::Display for OntologyFieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, seg) in self.segments.iter().enumerate() {
            if i > 0 {
                write!(f, ".")?;
            }
            write!(f, "{seg}")?;
            if let Some(la) = &self.list_access
                && la.segment_index == i
            {
                write!(f, "{}", la.specifier)?;
            }
        }
        Ok(())
    }
}

/// Parses a single dot-path segment, splitting the field name from its optional
/// index specifier (e.g. "x[?]" -> name "x", specifier Any).
fn parse_segment(s: &str) -> Result<(String, Option<IndexSpecifier>), ()> {
    match s.find('[') {
        None => Ok((s.to_owned(), None)),
        Some(pos) => {
            let name = s[..pos].to_owned();
            let content = s[pos..]
                .strip_prefix('[')
                .and_then(|r| r.strip_suffix(']'))
                .ok_or(())?;
            let specifier = match content {
                "?" => IndexSpecifier::Any,
                "!" => IndexSpecifier::All,
                n => IndexSpecifier::At(n.parse::<usize>().map_err(|_| ())?),
            };
            Ok((name, Some(specifier)))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OntologyField {
    tag: String,
    field_path: OntologyFieldPath,
}

impl OntologyField {
    pub fn try_new(v: String) -> Result<Self, super::Error> {
        let ontology_tag = v
            .split('.')
            .next()
            .ok_or_else(|| super::Error::bad_field(v.to_string()))?;
        let tag = ontology_tag.to_owned();
        let len = ontology_tag.len();

        let field_part = &v[(len + 1)..];
        let raw_segments: Vec<&str> = field_part.split('.').collect();

        let mut segments = Vec::with_capacity(raw_segments.len());
        let mut list_access: Option<ListAccess> = None;

        for (i, segment) in raw_segments.iter().enumerate() {
            let (name, specifier) =
                parse_segment(segment).map_err(|_| super::Error::bad_field(v.clone()))?;

            if let Some(specifier) = specifier {
                // A second specifier in the same path is not allowed.
                if list_access.is_some() {
                    return Err(super::Error::bad_field(v));
                }
                list_access = Some(ListAccess {
                    segment_index: i,
                    specifier,
                });
            }
            segments.push(name);
        }

        Ok(Self {
            tag,
            field_path: OntologyFieldPath {
                segments,
                list_access,
            },
        })
    }

    pub fn ontology_tag(&self) -> &str {
        &self.tag
    }

    pub fn field_path(&self) -> &OntologyFieldPath {
        &self.field_path
    }

    pub fn field(&self) -> String {
        self.field_path.to_string()
    }

    pub fn value(&self) -> String {
        self.to_string()
    }

    pub fn field_str(&self) -> String {
        let mut parts = vec![self.tag.as_str()];
        parts.extend(self.field_path.field_segments());
        parts.join(".")
    }
}

impl std::fmt::Display for OntologyField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.tag, self.field_path)
    }
}

/// A single constraint.
/// An expression is formed by binding a specific identifier (a field name or [`OntologyField`])
/// to an [`Op`]. It asserts a rule for that specific field (e.g., *"temperature > 25.0"*).
#[derive(Debug, Clone)]
pub struct OntologyExpr<T>(OntologyField, Op<T>);

impl<T> OntologyExpr<T> {
    pub fn ontology_field(&self) -> &OntologyField {
        &self.0
    }

    pub fn op(&self) -> &Op<T> {
        &self.1
    }

    pub fn into_parts(self) -> (OntologyField, Op<T>) {
        (self.0, self.1)
    }
}

impl<T> From<(OntologyField, Op<T>)> for OntologyExpr<T> {
    fn from(value: (OntologyField, Op<T>)) -> Self {
        Self(value.0, value.1)
    }
}

/// An expression group is defined as a series of ontology fields
/// with associated operations.
#[derive(Debug, Clone)]
pub struct OntologyExprGroup<T> {
    pub group: Vec<OntologyExpr<T>>,
}

impl<T> OntologyExprGroup<T> {
    pub fn new(group: Vec<OntologyExpr<T>>) -> Self {
        Self { group }
    }

    /// Exports filter data as several expression groupss grouped by ontology tag
    /// So if the
    pub fn split_by_ontology_tag(self) -> Vec<OntologyExprGroup<T>> {
        let mut map: HashMap<String, OntologyExprGroup<T>> = HashMap::new();
        for expr in self.group {
            let tag = expr.ontology_field().ontology_tag();
            match map.entry(tag.to_owned()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(Self::new(vec![expr]));
                }
                Entry::Occupied(mut occupied) => {
                    occupied.get_mut().group.push(expr);
                }
            }
        }

        map.into_values().collect()
    }
}

impl<T> Default for OntologyExprGroup<T> {
    fn default() -> Self {
        Self { group: Vec::new() }
    }
}

impl<T> IntoIterator for OntologyExprGroup<T> {
    type Item = OntologyExpr<T>;
    type IntoIter = std::vec::IntoIter<OntologyExpr<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.group.into_iter()
    }
}

/// A container for dynamic user-defined expressions mapping to ontology data models.
#[derive(Debug, Clone)]
pub struct OntologyFilter {
    ontology: HashMap<OntologyField, Op<Value>>,
}

impl OntologyFilter {
    /// Creates a new Metadata instance from a [`HashMap`].
    pub fn new(v: HashMap<OntologyField, Op<Value>>) -> Self {
        Self { ontology: v }
    }

    /// Creates an empty Metadata instance.
    pub fn empty() -> Self {
        Self {
            ontology: HashMap::new(),
        }
    }

    /// Retrieves the operation associated with a specific metadata field.
    pub fn get_op(&self, field: &OntologyField) -> Option<&Op<Value>> {
        self.ontology.get(field)
    }

    /// Returns an iterator over the ontology tags.
    pub fn ontology_tags(&self) -> impl Iterator<Item = &str> + '_ {
        self.ontology.keys().map(|f| f.ontology_tag())
    }

    /// Exports filter data as a unique expression group
    pub fn into_expr_group(self) -> OntologyExprGroup<Value> {
        OntologyExprGroup {
            group: self
                .ontology
                .into_iter()
                .map(|(o, v)| OntologyExpr(o, v))
                .collect(),
        }
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
    /// In between a two value range [a, b] with a <= b
    Between(Range<T>),
    /// Found in a set
    In(Vec<T>),
    /// Matches a certain expression
    Match(T),
    /// Outside a two value range [a, b], the strict complement of `Between`:
    /// v < a or v > b, with a <= b
    Outside(Range<T>),
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
            Op::Outside(range) => range.min.support_ordering(),
            // If no elements are provided the operation is unsupported
            // (cabba) TODO: check if there is a way to access these methods
            // directly from T
            Op::In(items) => !items.is_empty() && items[0].support_in(),
            Op::Match(v) => v.support_match(),
        }
    }
}

/// The root object representing a complete search query.
///
/// A query allows filtering across three distinct domains:
/// 1. The sequence, as [`SequenceFilter`]
/// 2. The topic, as [`TopicFilter`]
/// 3. The data catalog, represented as [`OntologyFilter`]
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
    pub created_at: Option<Op<Timestamp>>,
    pub user_metadata: HashMap<String, Op<Value>>,
}

impl SequenceFilter {
    pub fn is_empty(&self) -> bool {
        self.name.is_none() && self.created_at.is_none() && self.user_metadata.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicFilter {
    pub name: Option<Op<Text>>,
    pub created_at: Option<Op<Timestamp>>,
    pub ontology_tag: Option<Op<Text>>,
    pub serialization_format: Option<Op<Text>>,
    pub user_metadata: HashMap<String, Op<Value>>,
}

impl TopicFilter {
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.created_at.is_none()
            && self.user_metadata.is_empty()
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

    #[test]
    fn expr_grp_split() {
        let grp = OntologyExprGroup {
            group: vec![
                (
                    OntologyField::try_new("image.width".into()).unwrap(),
                    Op::Eq(Value::Integer(1200)),
                )
                    .into(),
                (
                    OntologyField::try_new("image.height".into()).unwrap(),
                    Op::Eq(Value::Integer(800)),
                )
                    .into(),
                (
                    OntologyField::try_new("imu.acceleration.x".into()).unwrap(),
                    Op::Geq(Value::Float(8.0)),
                )
                    .into(),
                (
                    OntologyField::try_new("imu.angular_velocity.x".into()).unwrap(),
                    Op::Leq(Value::Float(3.0)),
                )
                    .into(),
            ],
        };

        let splits = grp.split_by_ontology_tag();

        dbg!(&splits);

        for split in splits {
            assert_eq!(split.group.len(), 2);

            let ontology_tag = split.group[0].ontology_field().ontology_tag();
            assert!(ontology_tag == "image" || ontology_tag == "imu");
        }
    }
}
