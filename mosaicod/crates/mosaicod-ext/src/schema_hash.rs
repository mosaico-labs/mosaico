use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;

const DOMAIN: &str = "mosaico.schema.v1";

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
        Self(encoder.hasher.finalize().into())
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
    hasher: Sha256,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.hasher.update([value]);
    }

    fn i8(&mut self, value: i8) {
        self.hasher.update(value.to_le_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.hasher.update(value.to_le_bytes());
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn count(&mut self, count: usize) {
        self.hasher.update((count as u64).to_le_bytes());
    }

    fn str(&mut self, value: &str) {
        self.count(value.len());
        self.hasher.update(value.as_bytes());
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
        self.u8(data_type_tag(entries.data_type()));
        self.count(children.len());
        for child in children {
            self.unnamed_field(child);
        }
    }

    fn data_type(&mut self, data_type: &DataType) {
        self.u8(data_type_tag(data_type));
        match data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => {}
            DataType::Timestamp(unit, tz) => {
                self.u8(time_unit_tag(unit));
                self.option(tz.as_deref(), Self::str);
            }
            DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
                self.u8(time_unit_tag(unit));
            }
            DataType::Interval(unit) => self.u8(interval_unit_tag(unit)),
            DataType::FixedSizeBinary(size) => self.i32(*size),
            DataType::List(child)
            | DataType::ListView(child)
            | DataType::LargeList(child)
            | DataType::LargeListView(child) => self.unnamed_field(child),
            DataType::FixedSizeList(child, size) => {
                self.unnamed_field(child);
                self.i32(*size);
            }
            DataType::Struct(fields) => self.fields(fields),
            DataType::Union(fields, mode) => {
                self.union_fields(fields);
                self.u8(union_mode_tag(mode));
            }
            DataType::Dictionary(key, value) => {
                self.data_type(key);
                self.data_type(value);
            }
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => {
                self.u8(*precision);
                self.i8(*scale);
            }
            DataType::Map(entries, keys_sorted) => {
                self.map_entries(entries);
                self.bool(*keys_sorted);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.unnamed_field(run_ends);
                self.unnamed_field(values);
            }
        }
    }
}

fn data_type_tag(data_type: &DataType) -> u8 {
    match data_type {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float16 => 10,
        DataType::Float32 => 11,
        DataType::Float64 => 12,
        DataType::Timestamp(..) => 13,
        DataType::Date32 => 14,
        DataType::Date64 => 15,
        DataType::Time32(_) => 16,
        DataType::Time64(_) => 17,
        DataType::Duration(_) => 18,
        DataType::Interval(_) => 19,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => 20,
        DataType::FixedSizeBinary(_) => 21,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => 22,
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => 23,
        DataType::FixedSizeList(..) => 24,
        DataType::Struct(_) => 25,
        DataType::Union(..) => 26,
        DataType::Dictionary(..) => 27,
        DataType::Decimal32(..) => 28,
        DataType::Decimal64(..) => 29,
        DataType::Decimal128(..) => 30,
        DataType::Decimal256(..) => 31,
        DataType::Map(..) => 32,
        DataType::RunEndEncoded(..) => 33,
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
    fn layout_variants_are_ignored() {
        let groups = [
            vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
            vec![
                DataType::Binary,
                DataType::LargeBinary,
                DataType::BinaryView,
            ],
        ];
        for group in groups {
            let top_level: Vec<_> = group
                .iter()
                .map(|t| hash(vec![field("x", t.clone())]))
                .collect();
            let nested: Vec<_> = group
                .iter()
                .map(|t| {
                    hash(vec![field(
                        "s",
                        structure(vec![field("x", list(t.clone()))]),
                    )])
                })
                .collect();
            assert!(top_level.windows(2).all(|w| w[0] == w[1]), "{group:?}");
            assert!(nested.windows(2).all(|w| w[0] == w[1]), "{group:?}");
        }

        let item = || Arc::new(Field::new("item", DataType::Float64, true));
        let lists = [
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::ListView(item()),
            DataType::LargeListView(item()),
        ];
        let hashes: Vec<_> = lists
            .iter()
            .map(|t| hash(vec![field("x", t.clone())]))
            .collect();
        assert!(hashes.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn conventional_child_names_are_ignored() {
        let element = DataType::List(Arc::new(Field::new("element", DataType::Float64, true)));
        assert_eq!(
            hash(vec![field("x", list(DataType::Float64))]),
            hash(vec![field("x", element)])
        );

        assert_eq!(
            hash(vec![field("m", map("entries", "key", "value", false))]),
            hash(vec![field("m", map("key_value", "keys", "values", false))])
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

    #[test]
    fn tags() {
        let item = || Arc::new(Field::new("item", DataType::Int32, true));
        let data_types: Vec<(DataType, u8)> = vec![
            (DataType::Null, 0),
            (DataType::Boolean, 1),
            (DataType::Int8, 2),
            (DataType::Int16, 3),
            (DataType::Int32, 4),
            (DataType::Int64, 5),
            (DataType::UInt8, 6),
            (DataType::UInt16, 7),
            (DataType::UInt32, 8),
            (DataType::UInt64, 9),
            (DataType::Float16, 10),
            (DataType::Float32, 11),
            (DataType::Float64, 12),
            (DataType::Timestamp(TimeUnit::Second, None), 13),
            (DataType::Date32, 14),
            (DataType::Date64, 15),
            (DataType::Time32(TimeUnit::Second), 16),
            (DataType::Time64(TimeUnit::Nanosecond), 17),
            (DataType::Duration(TimeUnit::Second), 18),
            (DataType::Interval(IntervalUnit::YearMonth), 19),
            (DataType::Binary, 20),
            (DataType::LargeBinary, 20),
            (DataType::BinaryView, 20),
            (DataType::FixedSizeBinary(4), 21),
            (DataType::Utf8, 22),
            (DataType::LargeUtf8, 22),
            (DataType::Utf8View, 22),
            (DataType::List(item()), 23),
            (DataType::ListView(item()), 23),
            (DataType::LargeList(item()), 23),
            (DataType::LargeListView(item()), 23),
            (DataType::FixedSizeList(item(), 2), 24),
            (DataType::Struct(Fields::empty()), 25),
            (union([0, 1], UnionMode::Sparse), 26),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                27,
            ),
            (DataType::Decimal32(9, 2), 28),
            (DataType::Decimal64(18, 2), 29),
            (DataType::Decimal128(38, 2), 30),
            (DataType::Decimal256(76, 2), 31),
            (map("entries", "key", "value", false), 32),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Utf8, true)),
                ),
                33,
            ),
        ];
        for (data_type, tag) in data_types {
            assert_eq!(data_type_tag(&data_type), tag, "{data_type}");
        }

        let time_units = [
            (TimeUnit::Second, 0),
            (TimeUnit::Millisecond, 1),
            (TimeUnit::Microsecond, 2),
            (TimeUnit::Nanosecond, 3),
        ];
        for (unit, tag) in time_units {
            assert_eq!(time_unit_tag(&unit), tag, "{unit:?}");
        }

        let interval_units = [
            (IntervalUnit::YearMonth, 0),
            (IntervalUnit::DayTime, 1),
            (IntervalUnit::MonthDayNano, 2),
        ];
        for (unit, tag) in interval_units {
            assert_eq!(interval_unit_tag(&unit), tag, "{unit:?}");
        }

        assert_eq!(union_mode_tag(&UnionMode::Sparse), 0);
        assert_eq!(union_mode_tag(&UnionMode::Dense), 1);
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
