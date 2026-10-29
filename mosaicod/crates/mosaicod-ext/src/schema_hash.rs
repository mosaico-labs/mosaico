//! A hash that tells ontology schema versions apart.
//!
//! [`SchemaHash`] is the SHA-256 of a canonical encoding of an Arrow [`Schema`]. Any difference in
//! meaning changes it, and so does field order: schema metadata, and each field's name,
//! nullability, `dict_is_ordered`, metadata and type, with the parameters that change what a type
//! means (widths, units, time zones, precision and scale, fixed sizes).
//!
//! Layouts don't change it:
//! - `Utf8`/`LargeUtf8`/`Utf8View`, `Binary`/`LargeBinary`/`BinaryView` and the four list types
//!   are encoded alike. mosaicod reads stored schemas through DataFusion, which returns a top-level
//!   `Utf8` or `LargeUtf8` as `Utf8View`, so a hash that kept layouts wouldn't match after a read.
//! - The children of lists, maps and run-end encoded arrays are encoded without their names, which
//!   vary by producer (`item`, `element`).
//!
//! Everything else keeps its own encoding, dictionary and run-end encoding and the decimal widths
//! included.
//!
//! The hash must stay the same across processes, toolchains and Arrow upgrades. That rules out the
//! IPC bytes, which Arrow doesn't promise to keep across releases, and [`std::hash::Hash`], which
//! for `DataType` depends on the order of its variants. Arrow additions can't slip in silently: the
//! matches on Arrow's enums have no wildcard arm, `Schema` is destructured without `..`, and a test
//! pins `Field`'s properties.
//!
//! Changing the encoding changes every stored hash, so do it on purpose, by bumping `DOMAIN`.

use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;

const DOMAIN: &str = "mosaico.schema.v1";
const STRUCT: u8 = 25;

/// SHA-256 of a schema's canonical encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    pub fn new(schema: &Schema) -> Self {
        let Schema { fields, metadata } = schema;

        let mut encoder = Encoder::default();
        encoder.str(DOMAIN);
        encoder.metadata(metadata);
        encoder.fields(fields);
        Self(Sha256::digest(encoder.bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn i8(&mut self, value: i8) {
        self.bytes.extend(value.to_le_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.bytes.extend(value.to_le_bytes());
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn count(&mut self, count: usize) {
        self.bytes.extend((count as u64).to_le_bytes());
    }

    fn str(&mut self, value: &str) {
        self.count(value.len());
        self.bytes.extend(value.as_bytes());
    }

    fn option<T>(&mut self, value: Option<T>, encode: impl FnOnce(&mut Self, T)) {
        self.bool(value.is_some());
        if let Some(value) = value {
            encode(self, value);
        }
    }

    fn metadata(&mut self, metadata: &HashMap<String, String>) {
        let mut pairs: Vec<_> = metadata.iter().collect();
        pairs.sort_unstable();
        self.count(pairs.len());
        for (key, value) in pairs {
            self.str(key);
            self.str(value);
        }
    }

    fn fields(&mut self, fields: &Fields) {
        self.count(fields.len());
        for field in fields {
            self.field(field);
        }
    }

    fn field(&mut self, field: &Field) {
        self.str(field.name());
        self.unnamed_field(field);
    }

    fn unnamed_field(&mut self, field: &Field) {
        self.field_properties(field);
        self.data_type(field.data_type());
    }

    fn field_properties(&mut self, field: &Field) {
        self.bool(field.is_nullable());
        self.option(field.dict_is_ordered(), Self::bool);
        self.metadata(field.metadata());
    }

    fn union_fields(&mut self, fields: &UnionFields) {
        self.count(fields.len());
        for (type_id, field) in fields.iter() {
            self.i8(type_id);
            self.field(field);
        }
    }

    fn map_entries(&mut self, entries: &Field) {
        let DataType::Struct(children) = entries.data_type() else {
            return self.unnamed_field(entries);
        };
        self.field_properties(entries);
        self.u8(STRUCT);
        self.count(children.len());
        for child in children {
            self.unnamed_field(child);
        }
    }

    fn decimal(&mut self, tag: u8, precision: u8, scale: i8) {
        self.u8(tag);
        self.u8(precision);
        self.i8(scale);
    }

    fn data_type(&mut self, data_type: &DataType) {
        match data_type {
            DataType::Null => self.u8(0),
            DataType::Boolean => self.u8(1),
            DataType::Int8 => self.u8(2),
            DataType::Int16 => self.u8(3),
            DataType::Int32 => self.u8(4),
            DataType::Int64 => self.u8(5),
            DataType::UInt8 => self.u8(6),
            DataType::UInt16 => self.u8(7),
            DataType::UInt32 => self.u8(8),
            DataType::UInt64 => self.u8(9),
            DataType::Float16 => self.u8(10),
            DataType::Float32 => self.u8(11),
            DataType::Float64 => self.u8(12),
            DataType::Timestamp(unit, tz) => {
                self.u8(13);
                self.u8(time_unit_tag(unit));
                self.option(tz.as_deref(), Self::str);
            }
            DataType::Date32 => self.u8(14),
            DataType::Date64 => self.u8(15),
            DataType::Time32(unit) => {
                self.u8(16);
                self.u8(time_unit_tag(unit));
            }
            DataType::Time64(unit) => {
                self.u8(17);
                self.u8(time_unit_tag(unit));
            }
            DataType::Duration(unit) => {
                self.u8(18);
                self.u8(time_unit_tag(unit));
            }
            DataType::Interval(unit) => {
                self.u8(19);
                self.u8(interval_unit_tag(unit));
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => self.u8(20),
            DataType::FixedSizeBinary(size) => {
                self.u8(21);
                self.i32(*size);
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => self.u8(22),
            DataType::List(child)
            | DataType::ListView(child)
            | DataType::LargeList(child)
            | DataType::LargeListView(child) => {
                self.u8(23);
                self.unnamed_field(child);
            }
            DataType::FixedSizeList(child, size) => {
                self.u8(24);
                self.unnamed_field(child);
                self.i32(*size);
            }
            DataType::Struct(fields) => {
                self.u8(STRUCT);
                self.fields(fields);
            }
            DataType::Union(fields, mode) => {
                self.u8(26);
                self.union_fields(fields);
                self.u8(union_mode_tag(mode));
            }
            DataType::Dictionary(key, value) => {
                self.u8(27);
                self.data_type(key);
                self.data_type(value);
            }
            DataType::Decimal32(precision, scale) => self.decimal(28, *precision, *scale),
            DataType::Decimal64(precision, scale) => self.decimal(29, *precision, *scale),
            DataType::Decimal128(precision, scale) => self.decimal(30, *precision, *scale),
            DataType::Decimal256(precision, scale) => self.decimal(31, *precision, *scale),
            DataType::Map(entries, keys_sorted) => {
                self.u8(32);
                self.map_entries(entries);
                self.bool(*keys_sorted);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.u8(33);
                self.unnamed_field(run_ends);
                self.unnamed_field(values);
            }
        }
    }
}

fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn interval_unit_tag(unit: &IntervalUnit) -> u8 {
    match unit {
        IntervalUnit::YearMonth => 0,
        IntervalUnit::DayTime => 1,
        IntervalUnit::MonthDayNano => 2,
    }
}

fn union_mode_tag(mode: &UnionMode) -> u8 {
    match mode {
        UnionMode::Sparse => 0,
        UnionMode::Dense => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn hash(fields: Vec<Field>) -> SchemaHash {
        SchemaHash::new(&Schema::new(fields))
    }

    fn hash_with_metadata(fields: Vec<Field>, metadata: &[(&str, &str)]) -> SchemaHash {
        let metadata = metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        SchemaHash::new(&Schema::new_with_metadata(fields, metadata))
    }

    fn field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    fn list(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", element, true)))
    }

    fn structure(fields: Vec<Field>) -> DataType {
        DataType::Struct(fields.into())
    }

    fn map(entries: &str, key: &str, value: &str, keys_sorted: bool) -> DataType {
        let children = vec![
            Field::new(key, DataType::Utf8, false),
            Field::new(value, DataType::Float64, true),
        ];
        DataType::Map(
            Arc::new(Field::new(entries, structure(children), false)),
            keys_sorted,
        )
    }

    fn union(type_ids: [i8; 2], mode: UnionMode) -> DataType {
        let fields = [field("a", DataType::Int32), field("b", DataType::Utf8)];
        DataType::Union(UnionFields::try_new(type_ids, fields).unwrap(), mode)
    }

    #[test]
    fn same_schema_same_hash() {
        let build = |pairs: &[(&str, &str)]| {
            let metadata: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let x = field("x", DataType::Float64).with_metadata(metadata);
            hash_with_metadata(vec![x], pairs)
        };

        assert_eq!(
            build(&[("a", "1"), ("b", "2"), ("c", "3")]),
            build(&[("c", "3"), ("a", "1"), ("b", "2")])
        );
    }

    #[test]
    fn any_difference_in_meaning_changes_the_hash() {
        let base = || field("x", DataType::Float64);
        let dictionary = |ordered| {
            let data_type =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            Field::new("x", data_type, false).with_dict_is_ordered(ordered)
        };
        let timestamp = |unit, tz: Option<&str>| DataType::Timestamp(unit, tz.map(Arc::from));
        let item = || Arc::new(Field::new("item", DataType::Float64, true));

        let pairs: Vec<(&str, SchemaHash, SchemaHash)> = vec![
            (
                "schema metadata",
                hash(vec![base()]),
                hash_with_metadata(vec![base()], &[("k", "v")]),
            ),
            (
                "field name",
                hash(vec![base()]),
                hash(vec![field("y", DataType::Float64)]),
            ),
            (
                "field order",
                hash(vec![base(), field("y", DataType::Utf8)]),
                hash(vec![field("y", DataType::Utf8), base()]),
            ),
            (
                "struct field order",
                hash(vec![field(
                    "s",
                    structure(vec![base(), field("y", DataType::Utf8)]),
                )]),
                hash(vec![field(
                    "s",
                    structure(vec![field("y", DataType::Utf8), base()]),
                )]),
            ),
            (
                "union member order",
                hash(vec![field("u", union([0, 1], UnionMode::Dense))]),
                hash(vec![field(
                    "u",
                    DataType::Union(
                        UnionFields::try_new(
                            [1, 0],
                            [field("b", DataType::Utf8), field("a", DataType::Int32)],
                        )
                        .unwrap(),
                        UnionMode::Dense,
                    ),
                )]),
            ),
            (
                "nullability",
                hash(vec![base()]),
                hash(vec![Field::new("x", DataType::Float64, true)]),
            ),
            (
                "field metadata added",
                hash(vec![base()]),
                hash(vec![
                    base().with_metadata(HashMap::from([("k".into(), "v".into())])),
                ]),
            ),
            (
                "field metadata changed",
                hash(vec![
                    base().with_metadata(HashMap::from([("k".into(), "v".into())])),
                ]),
                hash(vec![
                    base().with_metadata(HashMap::from([("k".into(), "w".into())])),
                ]),
            ),
            (
                "dict_is_ordered",
                hash(vec![dictionary(false)]),
                hash(vec![dictionary(true)]),
            ),
            (
                "integer width",
                hash(vec![field("x", DataType::Int32)]),
                hash(vec![field("x", DataType::Int64)]),
            ),
            (
                "float width",
                hash(vec![field("x", DataType::Float32)]),
                hash(vec![base()]),
            ),
            (
                "timestamp unit",
                hash(vec![field("x", timestamp(TimeUnit::Microsecond, None))]),
                hash(vec![field("x", timestamp(TimeUnit::Nanosecond, None))]),
            ),
            (
                "timestamp tz",
                hash(vec![field(
                    "x",
                    timestamp(TimeUnit::Microsecond, Some("UTC")),
                )]),
                hash(vec![field(
                    "x",
                    timestamp(TimeUnit::Microsecond, Some("+02:00")),
                )]),
            ),
            (
                "timestamp tz None vs empty",
                hash(vec![field("x", timestamp(TimeUnit::Microsecond, None))]),
                hash(vec![field("x", timestamp(TimeUnit::Microsecond, Some("")))]),
            ),
            (
                "decimal precision",
                hash(vec![field("x", DataType::Decimal128(10, 2))]),
                hash(vec![field("x", DataType::Decimal128(12, 2))]),
            ),
            (
                "decimal scale",
                hash(vec![field("x", DataType::Decimal128(10, 2))]),
                hash(vec![field("x", DataType::Decimal128(10, 3))]),
            ),
            (
                "fixed-size list length",
                hash(vec![field("x", DataType::FixedSizeList(item(), 3))]),
                hash(vec![field("x", DataType::FixedSizeList(item(), 9))]),
            ),
            (
                "list vs fixed-size list",
                hash(vec![field("x", DataType::List(item()))]),
                hash(vec![field("x", DataType::FixedSizeList(item(), 1))]),
            ),
            (
                "struct child name",
                hash(vec![field("s", structure(vec![base()]))]),
                hash(vec![field(
                    "s",
                    structure(vec![field("y", DataType::Float64)]),
                )]),
            ),
            (
                "struct child type",
                hash(vec![field("s", structure(vec![base()]))]),
                hash(vec![field(
                    "s",
                    structure(vec![field("x", DataType::Int64)]),
                )]),
            ),
            (
                "list element type",
                hash(vec![field("x", list(DataType::Float64))]),
                hash(vec![field("x", list(DataType::Int64))]),
            ),
            (
                "map keys_sorted",
                hash(vec![field("m", map("entries", "key", "value", false))]),
                hash(vec![field("m", map("entries", "key", "value", true))]),
            ),
            (
                "union type ids",
                hash(vec![field("u", union([0, 1], UnionMode::Dense))]),
                hash(vec![field("u", union([1, 0], UnionMode::Dense))]),
            ),
            (
                "union mode",
                hash(vec![field("u", union([0, 1], UnionMode::Dense))]),
                hash(vec![field("u", union([0, 1], UnionMode::Sparse))]),
            ),
        ];

        for (what, before, after) in pairs {
            assert_ne!(before, after, "{what} should change the hash");
        }
    }

    #[test]
    fn golden() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("timestamp_ns", DataType::Int64, false),
                Field::new(
                    "acceleration",
                    structure(vec![
                        Field::new("x", DataType::Float64, false),
                        Field::new("frame_id", DataType::Utf8, true),
                    ]),
                    true,
                )
                .with_metadata(HashMap::from([(
                    "description".into(),
                    "Acceleration.".into(),
                )])),
                Field::new(
                    "covariance",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float64, true)),
                        9,
                    ),
                    false,
                ),
                Field::new(
                    "stamp",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("gains", map("entries", "key", "value", false), true),
            ],
            HashMap::from([("source".into(), "test".into())]),
        );

        // If this changes, every stored hash changes with it: bump `DOMAIN` on purpose instead.
        assert_eq!(
            SchemaHash::new(&schema).to_string(),
            "de7e320bb39782e71e48c32033507f67bd9adde77386e55b38785188d95c4ff6"
        );
    }

    fn type_bytes(data_type: &DataType) -> Vec<u8> {
        let mut encoder = Encoder::default();
        encoder.data_type(data_type);
        encoder.bytes
    }

    fn count(n: u64) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }

    /// A field without metadata whose type is a single tag byte.
    fn simple_field(name: Option<&str>, nullable: bool, tag: u8) -> Vec<u8> {
        let name = name
            .map(|name| [count(name.len() as u64), name.as_bytes().to_vec()].concat())
            .unwrap_or_default();
        [name, vec![u8::from(nullable), 0], count(0), vec![tag]].concat()
    }

    #[test]
    fn type_encoding() {
        let item = || Arc::new(Field::new("item", DataType::Int32, true));
        let item_bytes = simple_field(None, true, 4);
        let union_bytes = |mode: u8| {
            [
                vec![26],
                count(2),
                vec![0],
                simple_field(Some("a"), false, 4),
                vec![1],
                simple_field(Some("b"), false, 22),
                vec![mode],
            ]
            .concat()
        };

        let data_types: Vec<(DataType, Vec<u8>)> = vec![
            (DataType::Null, vec![0]),
            (DataType::Boolean, vec![1]),
            (DataType::Int8, vec![2]),
            (DataType::Int16, vec![3]),
            (DataType::Int32, vec![4]),
            (DataType::Int64, vec![5]),
            (DataType::UInt8, vec![6]),
            (DataType::UInt16, vec![7]),
            (DataType::UInt32, vec![8]),
            (DataType::UInt64, vec![9]),
            (DataType::Float16, vec![10]),
            (DataType::Float32, vec![11]),
            (DataType::Float64, vec![12]),
            (DataType::Timestamp(TimeUnit::Second, None), vec![13, 0, 0]),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                [vec![13, 2, 1], count(3), b"UTC".to_vec()].concat(),
            ),
            (DataType::Date32, vec![14]),
            (DataType::Date64, vec![15]),
            (DataType::Time32(TimeUnit::Second), vec![16, 0]),
            (DataType::Time64(TimeUnit::Nanosecond), vec![17, 3]),
            (DataType::Duration(TimeUnit::Millisecond), vec![18, 1]),
            (DataType::Interval(IntervalUnit::YearMonth), vec![19, 0]),
            (DataType::Interval(IntervalUnit::DayTime), vec![19, 1]),
            (DataType::Interval(IntervalUnit::MonthDayNano), vec![19, 2]),
            (DataType::Binary, vec![20]),
            (DataType::LargeBinary, vec![20]),
            (DataType::BinaryView, vec![20]),
            (DataType::FixedSizeBinary(4), vec![21, 4, 0, 0, 0]),
            (DataType::Utf8, vec![22]),
            (DataType::LargeUtf8, vec![22]),
            (DataType::Utf8View, vec![22]),
            (
                DataType::List(item()),
                [vec![23], item_bytes.clone()].concat(),
            ),
            (
                DataType::ListView(item()),
                [vec![23], item_bytes.clone()].concat(),
            ),
            (
                DataType::LargeList(item()),
                [vec![23], item_bytes.clone()].concat(),
            ),
            (
                DataType::LargeListView(item()),
                [vec![23], item_bytes.clone()].concat(),
            ),
            (
                DataType::FixedSizeList(item(), 2),
                [vec![24], item_bytes.clone(), vec![2, 0, 0, 0]].concat(),
            ),
            (
                structure(vec![field("a", DataType::Int32)]),
                [vec![25], count(1), simple_field(Some("a"), false, 4)].concat(),
            ),
            (union([0, 1], UnionMode::Sparse), union_bytes(0)),
            (union([0, 1], UnionMode::Dense), union_bytes(1)),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                vec![27, 2, 22],
            ),
            (DataType::Decimal32(9, 2), vec![28, 9, 2]),
            (DataType::Decimal64(18, 2), vec![29, 18, 2]),
            (DataType::Decimal128(10, -2), vec![30, 10, 0xfe]),
            (DataType::Decimal256(76, 2), vec![31, 76, 2]),
            (
                map("entries", "key", "value", false),
                [
                    vec![32, 0, 0],
                    count(0),
                    vec![25],
                    count(2),
                    simple_field(None, false, 22),
                    simple_field(None, true, 12),
                    vec![0],
                ]
                .concat(),
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Utf8, true)),
                ),
                [
                    vec![33],
                    simple_field(None, false, 4),
                    simple_field(None, true, 22),
                ]
                .concat(),
            ),
        ];
        for (data_type, bytes) in data_types {
            assert_eq!(type_bytes(&data_type), bytes, "{data_type}");
        }
    }

    /// `Field`'s properties are private, so the encoder can't destructure it. If this fails, an
    /// Arrow release added or removed a property: decide whether `SchemaHash` encodes it, then
    /// update the list. (`dict_id` is deprecated and not hashed: when it goes, only this list
    /// changes.)
    #[test]
    fn field_properties() {
        let value = serde_json::to_value(field("x", DataType::Int64)).unwrap();
        let keys: BTreeSet<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();

        assert_eq!(
            keys,
            BTreeSet::from([
                "name",
                "data_type",
                "nullable",
                "dict_id",
                "dict_is_ordered",
                "metadata",
            ])
        );
    }
}
