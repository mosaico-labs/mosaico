use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;

use crate::{params, traits::SquashedIterator, types};

#[derive(thiserror::Error, Debug)]
pub enum SchemaError {
    /// Returned when the required `timestamp` field is missing in the provided schema.
    #[error("missing timestamp field in schema")]
    MissingTimestampInSchema,
    #[error("wrong timestamp field type, expected int64")]
    WrongTimestampType,
}

/// Validates that the provided Arrow schema meets certain structural requirements.
///
/// This function performs a series of validation checks on an [`arrow::datatypes::SchemaRef`]
/// to ensure it conforms to the platform conventions.
///
/// # Errors
///
/// Returns [`SchemaError`] if the schema structural requirements are not met.
pub fn check_schema(schema: &SchemaRef) -> Result<(), SchemaError> {
    let field = schema.field_with_name(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);
    if let Ok(field) = field {
        if DataType::Int64 != *field.data_type() {
            return Err(SchemaError::WrongTimestampType);
        }
    } else {
        return Err(SchemaError::MissingTimestampInSchema);
    }
    Ok(())
}

/// Return a arrow empty schema
pub fn empty_schema_ref() -> Arc<Schema> {
    Arc::new(Schema::empty())
}

/// Checks if the given Arrow [`DataType`] is considered numeric
#[must_use]
pub fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean,
    )
}

/// Checks if the given Arrow [`DataType`] is considered literal
#[must_use]
pub fn is_literal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
    )
}

/// Converts an arrow [`Array`] to a literal type array (Utf8)
pub fn cast_array_to_literal(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    if is_literal(array.data_type()) {
        Ok(arrow_cast::cast(
            array.as_ref(),
            &arrow_schema::DataType::Utf8,
        )?)
    } else {
        Err(arrow::error::ArrowError::CastError(
            "unable to cast arrow array to literal type".to_owned(),
        ))
    }
}

/// Converts an arrow [`Array`] to a numberic type array (f64)
pub fn cast_array_to_numeric(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    if is_numeric(array.data_type()) {
        Ok(arrow_cast::cast(
            array.as_ref(),
            &arrow_schema::DataType::Float64,
        )?)
    } else {
        Err(arrow::error::ArrowError::CastError(
            "unable to cast arrow array to numeric type".to_owned(),
        ))
    }
}

/// Retrieves a nested array from a [`RecordBatch`] based on a flattened field name.
///
/// For example, given a flattened field name like "user.address.street",
/// this function will traverse the nested structure in the [`RecordBatch`] to
/// retrieve the corresponding [`ArrayRef`].
pub fn array_from_flat_field_name(
    flattened_field_name: &str,
    batch: &RecordBatch,
) -> Result<ArrayRef, ArrowError> {
    let subfields: Vec<&str> = flattened_field_name.split('.').collect();
    if subfields.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "empty field not supported".to_owned(),
        ));
    }

    let top_level_name = subfields[0];
    let mut current_array = batch.column_by_name(top_level_name).ok_or_else(|| {
        ArrowError::SchemaError(format!("can't find top level field `{0}`", top_level_name))
    })?;

    // Iterate and traverse the remaining nested path components
    //
    // *Note*: only structs fields are supported
    for subfield in &subfields[1..] {
        let struct_array = current_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                    "can't downcast to struct subfield `{0}` for top level field `{1}`",
                    subfield, top_level_name
                ))
            })?;

        current_array = struct_array.column_by_name(subfield).ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "can't find subfield `{0}` for top level field `{1}`",
                subfield, top_level_name
            ))
        })?;
    }

    Ok(Arc::clone(current_array))
}

pub struct SchemaFlattenerIter {
    // A queue to hold fields and their current name prefix.
    field_queue: VecDeque<(String, FieldRef)>,
}

impl SchemaFlattenerIter {
    fn new(schema: &SchemaRef) -> Self {
        let mut queue = VecDeque::new();

        // Traverse the schema to build a queue of fields
        for field in schema.fields() {
            queue.push_back((String::new(), field.clone()));
        }

        SchemaFlattenerIter { field_queue: queue }
    }
}

impl Iterator for SchemaFlattenerIter {
    type Item = (String, FieldRef);

    fn next(&mut self) -> Option<Self::Item> {
        // Continue looping until the queue is empty OR we find a simple field
        while let Some((prefix, field)) = self.field_queue.pop_front() {
            let current_name = if prefix.is_empty() {
                field.name().clone()
            } else {
                format!("{}.{}", prefix, field.name())
            };

            match field.data_type() {
                // 1) Struct - Encountered a nested type.
                DataType::Struct(children) => {
                    // Push children onto the front of the queue (DFS-like processing).
                    // We reverse the children before pushing to maintain order when popping from the front.
                    for child_field in children.iter().rev() {
                        self.field_queue
                            .push_front((current_name.clone(), child_field.clone()));
                    }
                    // Skip the struct itself and continue to process its children.
                }

                // 2) Primitive/Leaf Type - This is a field we want to yield.
                _ => {
                    return Some((current_name, field));
                }
            }
        }

        // The queue is empty, so iteration is complete.
        None
    }
}

impl SquashedIterator for SchemaRef {
    type Value = FieldRef;
    type Iter = SchemaFlattenerIter;

    fn squashed_iter(&self) -> Self::Iter {
        SchemaFlattenerIter::new(self)
    }
}

pub fn stats_from_arrow_field(field: &Field) -> types::Stats {
    use types::{NumericStats, Stats, TextStats};

    match field.data_type() {
        dt if is_numeric(dt) => Stats::Numeric(NumericStats::new()),
        dt if is_literal(dt) => Stats::Text(TextStats::new()),
        _ => Stats::Unsupported,
    }
}

/// Inspects an array and updates the provided statistics using SIMD-optimized Arrow compute kernels.
pub fn stats_inspect_array(stats: &mut types::Stats, array: &ArrayRef) -> Result<(), ArrowError> {
    use arrow::array::Array;
    use arrow::compute;
    use types::Stats;

    match stats {
        Stats::Numeric(stats) => {
            let narray = cast_array_to_numeric(array)?;
            let primitive = narray.as_primitive::<arrow::datatypes::Float64Type>();

            // Use SIMD-optimized min/max from Arrow compute
            let min_val = compute::min(primitive);
            let max_val = compute::max(primitive);

            // Check for nulls (O(1) - Arrow tracks this in metadata)
            let has_null = primitive.null_count() > 0;

            // Check for NaN values - Arrow's min/max excludes NaN, so we check separately
            // This is still efficient: single pass, branch-prediction friendly
            let has_nan = primitive.values().iter().any(|v| v.is_nan());

            stats.merge(min_val, max_val, has_null, has_nan);
        }
        Stats::Text(stats) => {
            let sarray = cast_array_to_literal(array)?;
            let string_array = sarray.as_string::<i32>();

            // Use SIMD-optimized min/max from Arrow compute
            let min_val = compute::min_string(string_array);
            let max_val = compute::max_string(string_array);

            // Check for nulls (O(1))
            let has_null = string_array.null_count() > 0;

            stats.merge(min_val, max_val, has_null);
        }
        Stats::Unsupported => { /* do nothing */ }
    }

    Ok(())
}

/// Inspects a [`RecordBatch`] and updates the columns statistics accordingly.
pub fn ontology_model_stats_inspect_record_batch(
    cstats: &mut types::OntologyModelStats,
    batch: &RecordBatch,
) -> Result<(), ArrowError> {
    for (col_name, stats) in &mut cstats.cols {
        let array = array_from_flat_field_name(col_name, batch)?;
        stats_inspect_array(stats, &array)?;
    }
    Ok(())
}

/// Creates an empty chunk that holds al schema fields.
///
/// The schema fields are flattened inside the chunk.
pub fn ontology_model_stats_from_schema(schema: &SchemaRef) -> types::OntologyModelStats {
    let mut cs = types::OntologyModelStats::empty();
    for (squashed_name, field) in schema.squashed_iter() {
        cs.cols.insert(
            squashed_name.clone(),
            stats_from_arrow_field(field.as_ref()),
        );
    }
    cs
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use super::*;

    // Helper function to create a schema
    fn create_schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    /// Schema with the required 'timestamp' field.
    #[test]
    fn valid_schema_with_timestamp() {
        let fields = vec![
            Field::new("timestamp_ns", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ];
        let schema = create_schema(fields);

        let result = check_schema(&schema);

        // Assert that the function returns Ok(()), indicating a valid schema.
        assert!(
            result.is_ok(),
            "Schema with 'timestamp_ns' should pass validation."
        );
    }

    /// Test case 2: Schema **without** the required 'timestamp' field.
    #[test]
    fn invalid_schema_missing_timestamp() {
        let fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("data", DataType::Utf8, true),
        ];
        let schema = create_schema(fields);

        let result = check_schema(&schema);

        // Assert that the function returns the expected error.
        assert!(result.is_err());
    }

    /// Test case 3: Schema with fields but 'timestamp' field is misspelled or has wrong case.
    #[test]
    fn invalid_schema_misspelled_timestamp() {
        let fields = vec![
            Field::new("TimeStAmP", DataType::Int64, false), // Wrong casing
            Field::new("value", DataType::Float64, true),
        ];
        let schema = create_schema(fields);

        let result = check_schema(&schema);

        // The check uses field_with_name, which is case-sensitive, so it should fail.
        assert!(result.is_err());
    }

    // Helper function to create a simplified schema reference
    fn create_schema_ref(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn flat_schema() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ];
        let schema_ref = create_schema_ref(fields);

        let flattened_names: Vec<String> =
            schema_ref.squashed_iter().map(|(name, _)| name).collect();

        assert_eq!(flattened_names, vec!["id".to_owned(), "name".to_owned()]);
    }

    #[test]
    fn simple_nested_schema() {
        let address_fields = vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("zip", DataType::Int32, false),
        ];

        let fields = vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("address", DataType::Struct(address_fields.into()), false),
            Field::new("is_active", DataType::Boolean, false),
        ];
        let schema_ref = create_schema_ref(fields);

        let flattened_names: Vec<String> =
            schema_ref.squashed_iter().map(|(name, _)| name).collect();

        assert_eq!(
            flattened_names,
            vec![
                "user_id".to_owned(),
                "address.street".to_owned(),
                "address.zip".to_owned(),
                "is_active".to_owned(),
            ]
        );
    }

    #[test]
    fn deeply_nested_schema() {
        // user.profile.location.city
        let location_fields = vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, false),
        ];

        let profile_fields = vec![
            Field::new("age", DataType::Int16, false),
            Field::new("location", DataType::Struct(location_fields.into()), false),
        ];

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("profile", DataType::Struct(profile_fields.into()), false),
        ];
        let schema_ref = create_schema_ref(fields);

        let flattened_names: Vec<String> =
            schema_ref.squashed_iter().map(|(name, _)| name).collect();

        assert_eq!(
            flattened_names,
            vec![
                "id".to_owned(),
                "profile.age".to_owned(),
                "profile.location.city".to_owned(),
                "profile.location.country".to_owned(),
            ]
        );
    }

    #[test]
    fn non_struct_nested_types_are_leafs() {
        // // Arrow List types are complex but are treated as leaf nodes in flattening
        // // unless you specifically wanted to flatten list elements, which is rare.
        // let list_field = Field::new(
        //     "items",
        //     DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        //     true,
        // );

        let fields = vec![
            Field::new(
                "list_of_ints",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                false,
            ),
            Field::new(
                "map_data",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int64, false),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ];

        let schema_ref = create_schema_ref(fields);

        let flattened_names: Vec<String> =
            schema_ref.squashed_iter().map(|(name, _)| name).collect();

        // Since the code only recurses on DataType::Struct, List and Map remain as leaf nodes.
        assert_eq!(
            flattened_names,
            vec!["list_of_ints".to_owned(), "map_data".to_owned(),]
        );
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;

    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;

    pub fn dummy_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                DataType::Int64,
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    10000, 10005, 10010, 10015, 10020, 10025, 10030,
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
            ],
        )
        .unwrap()
    }
}
