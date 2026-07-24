#![allow(unused_crate_dependencies)]
use arrow::array::{
    BooleanBuilder, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use mosaicod_db as db;
use serde_json::json;
use std::sync::Arc;
use tests::{actions, common};
use tonic::Code;

use arrow::array::ArrayRef;

/// Builds a [`RecordBatch`] with a list column (list of i64) used to test filters
/// (equal, at least on, all) against list-typed ontology columns.
fn int_list_batch(ts_start: i64, values: &[i64], list_test: &[Vec<i64>]) -> RecordBatch {
    assert_eq!(
        values.len(),
        list_test.len(),
        "values and test_list must have the same length"
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "list_test",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
            false,
        ),
    ]));

    let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| ts_start + i * 5).collect();

    // Each inner Vec becomes one list entry; values are appended element by element,
    // then append(true) closes the current list and moves to the next row.
    let mut list_builder = ListBuilder::new(Int64Builder::new()).with_field(Field::new(
        "item",
        DataType::Int64,
        false,
    ));

    for row in list_test {
        for &val in row {
            list_builder.values().append_value(val);
        }
        list_builder.append(true);
    }
    let list_array = list_builder.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap()
}

fn string_list_batch(ts_start: i64, values: &[i64], list_test: &[Vec<&str>]) -> RecordBatch {
    assert_eq!(values.len(), list_test.len());

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "list_test",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
    ]));

    let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| ts_start + i * 5).collect();

    let mut list_builder = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
        "item",
        DataType::Utf8,
        false,
    ));

    for row in list_test {
        for &val in row {
            list_builder.values().append_value(val);
        }
        list_builder.append(true);
    }
    let list_array = list_builder.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap()
}

fn struct_list_batch(ts_start: i64, readings: &[Vec<(f64, f64)>]) -> RecordBatch {
    use arrow::array::{ArrayBuilder, Float64Builder, StructBuilder};

    let struct_fields: arrow::datatypes::Fields = vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]
    .into();

    let mut list_builder = ListBuilder::new(StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
        ],
    ))
    .with_field(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields.clone()),
        false,
    )));

    for row in readings {
        for (x, y) in row {
            let sb = list_builder.values();
            sb.field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(*x);
            sb.field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(*y);
            sb.append(true);
        }
        list_builder.append(true);
    }

    let list_array = list_builder.finish();
    let n = readings.len();
    let timestamps: Vec<i64> = (0..n as i64).map(|i| ts_start + i * 5).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new(
            "readings",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(struct_fields),
                false,
            ))),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap()
}

fn bool_list_batch(ts_start: i64, values: &[i64], list_test: &[Vec<bool>]) -> RecordBatch {
    assert_eq!(
        values.len(),
        list_test.len(),
        "values and test_list must have the same length"
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "list_test",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
            false,
        ),
    ]));

    let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| ts_start + i * 5).collect();

    let mut list_builder = ListBuilder::new(BooleanBuilder::new()).with_field(Field::new(
        "item",
        DataType::Boolean,
        false,
    ));

    for row in list_test {
        for &val in row {
            list_builder.values().append_value(val);
        }
        list_builder.append(true);
    }
    let list_array = list_builder.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap()
}

fn struct_bool_list_batch(ts_start: i64, readings: &[Vec<bool>]) -> RecordBatch {
    use arrow::array::{ArrayBuilder, StructBuilder};

    let struct_fields: arrow::datatypes::Fields =
        vec![Field::new("active", DataType::Boolean, false)].into();

    let mut list_builder = ListBuilder::new(StructBuilder::new(
        struct_fields.clone(),
        vec![Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>],
    ))
    .with_field(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields.clone()),
        false,
    )));

    for row in readings {
        for active in row {
            let sb = list_builder.values();
            sb.field_builder::<BooleanBuilder>(0)
                .unwrap()
                .append_value(*active);
            sb.append(true);
        }
        list_builder.append(true);
    }

    let list_array = list_builder.finish();
    let n = readings.len();
    let timestamps: Vec<i64> = (0..n as i64).map(|i| ts_start + i * 5).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new(
            "readings",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(struct_fields),
                false,
            ))),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap()
}

fn int_batch(ts_start: i64, values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| ts_start + i * 5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn nullable_int_batch(ts_start: i64, values: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let n = values.len();
    let timestamps: Vec<i64> = (0..n as i64).map(|i| ts_start + i * 5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn string_batch(ts_start: i64, values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| ts_start + i * 5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn no_value_batch(ts_start: i64, count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("sensor_id", DataType::Int64, false),
    ]));
    let timestamps: Vec<i64> = (0..count as i64).map(|i| ts_start + i * 5).collect();
    let ids: Vec<i64> = (0..count as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(ids)),
        ],
    )
    .unwrap()
}

async fn setup_topics(client: &mut common::Client, seq: &str, topics: Vec<(&str, RecordBatch)>) {
    actions::sequence_create(client, seq, None).await.unwrap();
    let (_, session_uuid) = actions::session_create(client, seq).await.unwrap();
    for (suffix, batch) in topics {
        let topic_name = format!("{seq}/{suffix}");
        let uuid = actions::topic_create(client, &session_uuid, &topic_name, None)
            .await
            .unwrap();
        actions::do_put(client, &uuid, &topic_name, vec![batch], false)
            .await
            .unwrap();
    }
    actions::session_finalize(client, &session_uuid)
        .await
        .unwrap();
}

fn topic_locators(items: &[serde_json::Value]) -> Vec<String> {
    items
        .iter()
        .flat_map(|item| {
            item["topics"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .map(|t| t["locator"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_neq_excludes_range_containing_value(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_neq";
    // Topic A: value range [1, 7], Topic B: value range [100, 106]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_batch(10_000, &[1, 2, 3, 4, 5, 6, 7])),
            (
                "topic_b",
                int_batch(20_000, &[100, 101, 102, 103, 104, 105, 106]),
            ),
        ],
    )
    .await;

    // neq 5: value 5 is inside topic_a's range [1,7] -> excluded; 5 < 100 so topic_b matches
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 5 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be excluded (value 5 is in range [1,7])"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be included (min=100 > 5)"
    );

    // neq 50: both ranges entirely exclude 50 (max=7 < 50, min=100 > 50)
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 50 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (max=7 < 50)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be included (min=100 > 50)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_exist_returns_topic_with_column(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_ex";
    // topic_with: has a "value" column; topic_without: has "sensor_id" but no "value"
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_with", int_batch(10_000, &[1, 2, 3])),
            ("topic_without", no_value_batch(20_000, 3)),
        ],
    )
    .await;

    let items = actions::query(&mut client, json!({ "ontology": { "mock.value": "$ex" } }))
        .await
        .unwrap();
    let locators = topic_locators(&items);

    assert!(
        locators.contains(&format!("{seq}/topic_with")),
        "topic_with should be included (has mock.value)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_without")),
        "topic_without should be excluded (no mock.value column)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_exist_works_on_text_column(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_ex_text";
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_with", string_batch(10_000, &["alpha", "beta"])),
            ("topic_without", int_batch(20_000, &[1, 2])),
        ],
    )
    .await;

    let items = actions::query(&mut client, json!({ "ontology": { "mock.name": "$ex" } }))
        .await
        .unwrap();
    let locators = topic_locators(&items);

    assert!(
        locators.contains(&format!("{seq}/topic_with")),
        "topic_with should be included (has mock.name)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_without")),
        "topic_without should be excluded (no mock.name column)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_not_exist_returns_chunks_with_nulls(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_nex";
    // topic_nonull: "value" column with no nulls -> has_null=FALSE
    // topic_withnull: "value" column with some nulls -> has_null=TRUE
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_nonull", int_batch(10_000, &[1, 2, 3])),
            (
                "topic_withnull",
                nullable_int_batch(20_000, vec![Some(1), None, Some(3)]),
            ),
        ],
    )
    .await;

    let items = actions::query(&mut client, json!({ "ontology": { "mock.value": "$nex" } }))
        .await
        .unwrap();
    let locators = topic_locators(&items);

    assert!(
        !locators.contains(&format!("{seq}/topic_nonull")),
        "topic_nonull should be excluded (has_null=FALSE, column definitely has non-null values)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_withnull")),
        "topic_withnull should be included (has_null=TRUE)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_in_returns_chunks_overlapping_any_value(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in";
    // topic_a: value range [1, 7]; topic_b: value range [100, 106]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_batch(10_000, &[1, 2, 3, 4, 5, 6, 7])),
            (
                "topic_b",
                int_batch(20_000, &[100, 101, 102, 103, 104, 105, 106]),
            ),
        ],
    )
    .await;

    // [5, 103]: 5 is in [1,7] and 103 is in [100,106] -> both match
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$in": [5, 103] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should match (5 is in range [1,7])"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "topic_b should match (103 is in range [100,106])"
    );

    // [50]: not in [1,7] and not in [100,106] -> no match
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$in": [50] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "topic_a should not match (50 not in [1,7])"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should not match (50 not in [100,106])"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_in_single_value_acts_like_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_single";
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_batch(10_000, &[1, 2, 3])),
            ("topic_b", int_batch(20_000, &[10, 20, 30])),
        ],
    )
    .await;

    // [2]: in [1,3] but not in [10,30]
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$in": [2] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(locators.contains(&format!("{seq}/topic_a")));
    assert!(!locators.contains(&format!("{seq}/topic_b")));

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_match_filters_by_regex(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match";
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_truck",
                string_batch(10_000, &["truck_scania", "truck_volvo"]),
            ),
            ("topic_car", string_batch(20_000, &["ferrari", "porsche"])),
        ],
    )
    .await;

    // "truck*" should match only topic_truck
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$match": "truck*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_truck")),
        "topic_truck should match '^truck'"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_car")),
        "topic_car should not match '^truck'"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_match_excludes_topics_without_column(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match_nocol";
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_with_name", string_batch(10_000, &["truck_scania"])),
            ("topic_no_name", int_batch(20_000, &[1, 2, 3])),
        ],
    )
    .await;

    // "?*" is the pattern to match a non-empty name.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$match": "?*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);

    assert!(locators.contains(&format!("{seq}/topic_with_name")));
    assert!(!locators.contains(&format!("{seq}/topic_no_name")));

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_in_unnest_no_false_positives(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_unnest";
    // topic_a: [1,7],  topic_b: [100,106]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_batch(10_000, &[1, 2, 3, 4, 5, 6, 7])),
            (
                "topic_b",
                int_batch(20_000, &[100, 101, 102, 103, 104, 105, 106]),
            ),
        ],
    )
    .await;

    // [3, 200]: 3 hits topic_a's range, 200 is outside both ranges.
    // topic_b must NOT be included even though 100 <= 200 and 106 >= 3.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$in": [3, 200] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should match (3 is in [1,7])"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b must not match: neither 3 nor 200 falls in [100,106]"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_in_text_values(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_text";
    // topic_vehicles: ["car", "truck", "bus"], topic_animals: ["cat", "dog", "fox"]
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_vehicles",
                string_batch(10_000, &["bus", "car", "truck"]),
            ),
            (
                "topic_animals",
                string_batch(20_000, &["cat", "dog", "fox"]),
            ),
        ],
    )
    .await;

    // ["truck", "dog"]: each value hits exactly one topic
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$in": ["truck", "dog"] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_vehicles")),
        "topic_vehicles should match ('truck' in ['bus','car','truck'])"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_animals")),
        "topic_animals should match ('dog' in ['cat','dog','fox'])"
    );

    // ["zebra"]: no topic has this value
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$in": ["zebra"] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(locators.is_empty(), "no topic should match 'zebra'");

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_neq_boundary_values(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_neq_boundary";
    // topic_a: [5, 10]  both boundaries are candidates for $neq
    setup_topics(
        &mut client,
        seq,
        vec![("topic_a", int_batch(10_000, &[5, 6, 7, 8, 9, 10]))],
    )
    .await;

    // neq 5: is in [5,10] -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 5 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).is_empty(),
        "topic_a should be excluded ($neq min=5, p=5 is in range)"
    );

    // neq 10: is in [5,10] -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 10 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).is_empty(),
        "topic_a should be excluded ($neq max=10, p=10 is in range)"
    );

    // neq 4 : range [5,10] -> included
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 4 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).contains(&format!("{seq}/topic_a")),
        "topic_a should be included ($neq 4, range [5,10] is entirely above 4)"
    );

    // neq 11: range [5,10] -> included
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 11 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).contains(&format!("{seq}/topic_a")),
        "topic_a should be included ($neq 11, range [5,10] is entirely below 11)"
    );

    // neq 8: range [5,10] -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$neq": 8 } } }),
    )
    .await
    .unwrap();
    assert!(
        !topic_locators(&items).contains(&format!("{seq}/topic_a")),
        "topic_a should not be included ($neq 8, range [5,10])"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_combined_in_and_geq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_combined";
    // topic_low:  values [1, 2, 3]
    // topic_mid:  values [5, 6, 7]
    // topic_high: values [20, 21, 22]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_low", int_batch(10_000, &[1, 2, 3])),
            ("topic_mid", int_batch(20_000, &[5, 6, 7])),
            ("topic_high", int_batch(30_000, &[20, 21, 22])),
        ],
    )
    .await;

    // $in [2, 6, 21] AND $geq 5: $in selects all three, $geq 5 prunes topic_low (max=3 < 5)
    let items = actions::query(
        &mut client,
        json!({
            "ontology": {
                "mock.value": { "$in": [2, 6, 21] },
                "mock.value": { "$geq": 5 }
            }
        }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_low")),
        "topic_low should be excluded ($geq 5 prunes max=3)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_mid")),
        "topic_mid should be included (6 in $in list and max=7 >= 5)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_high")),
        "topic_high should be included (21 in $in list and min=20 >= 5)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_eq";

    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                int_list_batch(
                    10_000,
                    &[1, 2, 3],
                    &[vec![1, 2, 3], vec![3, 4, 5], vec![6, 7, 8]],
                ),
            ),
            ("topic_b", int_batch(20_000, &[100, 101, 102])),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": 5 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (value 5 appears in one of its lists)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded (no list_test column)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_neq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_neq";
    // topic_a: [5,5,5] — every element equals 5, so no element != 5
    // topic_b: [1,2,3] — all elements != 5 (max=3 < 5, so DB also passes it through)
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![5, 5, 5]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![1, 2, 3]])),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$neq": 5 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be excluded (all elements are 5, none != 5)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be included (elements [1,2,3] all satisfy != 5)"
    );

    // [5,5,5] has no element equal to 3 -> all elements satisfy != 3 -> at least one does
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$neq": 3 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (elements [5,5,5] all satisfy != 3)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_ordering(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_ord";
    // topic_a elements: [1, 2, 3] (min=1, max=3)
    // topic_b elements: [10, 20, 30] (min=10, max=30)
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    macro_rules! query_locs {
        ($client:expr, $op:literal, $val:expr) => {{
            let items = actions::query(
                $client,
                json!({ "ontology": { "mock.list_test[?]": { $op: $val } } }),
            )
            .await
            .unwrap();
            topic_locators(&items)
        }};
    }

    // $gt v: any element > v — DataFusion: array_max(arr) > v
    let locs = query_locs!(&mut client, "$gt", 25);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$gt 25: max(a)=3, excluded"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$gt 25: max(b)=30 > 25, included"
    );

    let locs = query_locs!(&mut client, "$gt", 30);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$gt 30: excluded"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$gt 30: max(b)=30 not > 30, excluded"
    );

    // $geq v: any element >= v — DataFusion: array_max(arr) >= v
    let locs = query_locs!(&mut client, "$geq", 30);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$geq 30: max(a)=3, excluded"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$geq 30: max(b)=30 >= 30, included"
    );

    let locs = query_locs!(&mut client, "$geq", 31);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$geq 31: excluded"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$geq 31: max(b)=30 < 31, excluded"
    );

    // $lt v: any element < v — DataFusion: array_min(arr) < v
    let locs = query_locs!(&mut client, "$lt", 2);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$lt 2: min(a)=1 < 2, included"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$lt 2: min(b)=10, excluded"
    );

    let locs = query_locs!(&mut client, "$lt", 1);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$lt 1: min(a)=1 not < 1, excluded"
    );
    assert!(!locs.contains(&format!("{seq}/topic_b")), "$lt 1: excluded");

    // $leq v: any element <= v — DataFusion: array_min(arr) <= v
    let locs = query_locs!(&mut client, "$leq", 1);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$leq 1: min(a)=1 <= 1, included"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$leq 1: min(b)=10, excluded"
    );

    let locs = query_locs!(&mut client, "$leq", 0);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$leq 0: excluded"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$leq 0: excluded"
    );

    // Sanity: both topics have elements > 0
    let locs = query_locs!(&mut client, "$gt", 0);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$gt 0: topic_a included"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$gt 0: topic_b included"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_between(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_between";
    // topic_a elements: [1, 2, 3], topic_b elements: [10, 20, 30]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    // [5, 9]: gap between both topics — neither has an element in [5, 9]
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$between": [5, 9] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$between [5,9]: topic_a excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$between [5,9]: topic_b excluded"
    );

    // [5, 15]: only topic_b has element 10 in range
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$between": [5, 15] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$between [5,15]: topic_a excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$between [5,15]: topic_b included (10 in [5,15])"
    );

    // [2, 12]: both have an element in range (2 for a, 10 for b)
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$between": [2, 12] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$between [2,12]: topic_a included (2 in range)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$between [2,12]: topic_b included (10 in range)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_in(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_in";
    // topic_a elements: [1, 2, 3], topic_b elements: [10, 20, 30]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    // Neither topic has 5 or 7
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$in": [5, 7] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$in [5,7]: topic_a excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$in [5,7]: topic_b excluded"
    );

    // topic_a has 2, topic_b has neither 2 nor 99
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$in": [2, 99] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$in [2,99]: topic_a has 2, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$in [2,99]: topic_b excluded"
    );

    // Both match: topic_a has 3, topic_b has 10
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$in": [3, 10] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$in [3,10]: topic_a has 3, included"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$in [3,10]: topic_b has 10, included"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_all_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_all_eq";
    // topic_a: [7,7,7] — all elements equal 7 (min=max=7)
    // topic_b: [1,2,3] — mixed elements
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![7, 7, 7]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![1, 2, 3]])),
        ],
    )
    .await;

    // $eq 7
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$eq": 7 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (all elements are 7)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded (elements [1,2,3] not all equal 7)"
    );

    // $eq 2: no list is uniformly 2
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$eq": 2 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$eq 2: topic_a excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$eq 2: topic_b excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_all_neq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_all_neq";
    // topic_a: [1,2,3], topic_b: [4,5,6]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![4, 5, 6]])),
        ],
    )
    .await;

    // $neq 9: neither topic contains 9
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$neq": 9 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$neq 9: topic_a included"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$neq 9: topic_b included"
    );

    // $neq 2: topic_a contains 2 -> excluded; topic_b has no 2 -> included
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$neq": 2 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$neq 2: topic_a has 2, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$neq 2: topic_b has no 2, included"
    );

    // $neq 5: topic_a has no 5 -> included; topic_b contains 5 -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$neq": 5 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$neq 5: topic_a has no 5, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$neq 5: topic_b has 5, excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_all_ordering(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_all_ord";
    // topic_a elements: [1, 2, 3] (min=1, max=3)
    // topic_b elements: [10, 20, 30] (min=10, max=30)
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    macro_rules! query_locs {
        ($client:expr, $op:literal, $val:expr) => {{
            let items = actions::query(
                $client,
                json!({ "ontology": { "mock.list_test[!]": { $op: $val } } }),
            )
            .await
            .unwrap();
            topic_locators(&items)
        }};
    }

    // $gt v: all elements > v — DataFusion: array_min(arr) > v
    let locs = query_locs!(&mut client, "$gt", 9);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$gt 9: min(a)=1, excluded"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$gt 9: min(b)=10 > 9, included"
    );

    let locs = query_locs!(&mut client, "$gt", 10);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$gt 10: excluded"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$gt 10: min(b)=10 not > 10, excluded"
    );

    // $geq v: all elements >= v — DataFusion: array_min(arr) >= v
    let locs = query_locs!(&mut client, "$geq", 10);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$geq 10: min(a)=1, excluded"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$geq 10: min(b)=10 >= 10, included"
    );

    let locs = query_locs!(&mut client, "$geq", 1);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$geq 1: min(a)=1 >= 1, included"
    );
    assert!(
        locs.contains(&format!("{seq}/topic_b")),
        "$geq 1: min(b)=10 >= 1, included"
    );

    // $lt v: all elements < v
    let locs = query_locs!(&mut client, "$lt", 5);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$lt 5: max(a)=3 < 5, included"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$lt 5: max(b)=30, excluded"
    );

    let locs = query_locs!(&mut client, "$lt", 3);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$lt 3: max(a)=3 not < 3, excluded"
    );
    assert!(!locs.contains(&format!("{seq}/topic_b")), "$lt 3: excluded");

    // $leq v: all elements <= v
    let locs = query_locs!(&mut client, "$leq", 3);
    assert!(
        locs.contains(&format!("{seq}/topic_a")),
        "$leq 3: max(a)=3 <= 3, included"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$leq 3: max(b)=30, excluded"
    );

    let locs = query_locs!(&mut client, "$leq", 0);
    assert!(
        !locs.contains(&format!("{seq}/topic_a")),
        "$leq 0: excluded"
    );
    assert!(
        !locs.contains(&format!("{seq}/topic_b")),
        "$leq 0: excluded"
    );

    // $between [a, b]: all elements in [a, b]
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$between": [1, 3] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$between [1,3]: topic_a [1,2,3] fully in range, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$between [1,3]: topic_b min=10 > 3, excluded"
    );

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$between": [5, 35] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$between [5,35]: topic_a min=1 < 5, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$between [5,35]: topic_b [10,20,30] fully in [5,35], included"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_at_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_at_eq";
    // topic_a: [10, 20, 30] — index 0->10, 1->20, 2->30
    // topic_b: [40, 50, 60] — index 0->40, 1->50, 2->60
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![10, 20, 30]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![40, 50, 60]])),
        ],
    )
    .await;

    // [0] $eq 10: first element — topic_a matches, topic_b excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[0]": { "$eq": 10 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[0]$eq 10: topic_a[0]=10, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[0]$eq 10: topic_b[0]=40, excluded"
    );

    // [1] $eq 50: second element — topic_b matches, topic_a excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[1]": { "$eq": 50 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "[1]$eq 50: topic_a[1]=20, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "[1]$eq 50: topic_b[1]=50, included"
    );

    // [2] $eq 30: third element — topic_a matches, topic_b excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[2]": { "$eq": 30 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[2]$eq 30: topic_a[2]=30, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[2]$eq 30: topic_b[2]=60, excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_at_ordering(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_at_ord";
    // topic_a: [10, 20, 30], topic_b: [40, 50, 60]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![10, 20, 30]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![40, 50, 60]])),
        ],
    )
    .await;

    // [1] $gt 45: topic_a[1]=20 not > 45; topic_b[1]=50 > 45
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[1]": { "$gt": 45 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "[1]$gt 45: topic_a[1]=20, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "[1]$gt 45: topic_b[1]=50, included"
    );

    // [0] $leq 10: topic_a[0]=10 <= 10; topic_b[0]=40 not <= 10
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[0]": { "$leq": 10 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[0]$leq 10: topic_a[0]=10, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[0]$leq 10: topic_b[0]=40, excluded"
    );

    // [2] $between [25, 35]: topic_a[2]=30 in [25,35]; topic_b[2]=60 not
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[2]": { "$between": [25, 35] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[2]$between [25,35]: topic_a[2]=30, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[2]$between [25,35]: topic_b[2]=60, excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_at_outside(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_at_outside";
    // topic_a: [10, 20, 30], topic_b: [40, 50, 60]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![10, 20, 30]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![40, 50, 60]])),
        ],
    )
    .await;

    // [2] $outside [25, 35]: strict complement of the [2] $between [25, 35] case.
    // topic_a[2]=30 is inside [25,35] -> excluded; topic_b[2]=60 > 35 -> included.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[2]": { "$outside": [25, 35] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "[2]$outside [25,35]: topic_a[2]=30 inside, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "[2]$outside [25,35]: topic_b[2]=60 > 35, included"
    );

    // [1] $outside [45, 55]: topic_a[1]=20 < 45 -> included; topic_b[1]=50 inside -> excluded.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[1]": { "$outside": [45, 55] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[1]$outside [45,55]: topic_a[1]=20 < 45, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[1]$outside [45,55]: topic_b[1]=50 inside, excluded"
    );

    // [0] $outside [5, 100]: both first elements are inside the wide range -> both excluded.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[0]": { "$outside": [5, 100] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "[0]$outside [5,100]: topic_a[0]=10 inside, excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "[0]$outside [5,100]: topic_b[0]=40 inside, excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_list_db_pruning(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_list_pruning";
    // Elements span [1, 30] — DB stores min_element=1, max_element=30
    setup_topics(
        &mut client,
        seq,
        vec![(
            "topic_a",
            int_list_batch(10_000, &[1, 2], &[vec![1, 15, 30], vec![5, 10, 20]]),
        )],
    )
    .await;

    // Values at the element range boundaries are found
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": 1 } } }),
    )
    .await
    .unwrap();
    assert!(
        !topic_locators(&items).is_empty(),
        "$eq 1: element 1 at boundary, should be found"
    );

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": 30 } } }),
    )
    .await
    .unwrap();
    assert!(
        !topic_locators(&items).is_empty(),
        "$eq 30: element 30 at boundary, should be found"
    );

    // Values outside element range [1, 30]
    // v=0: 1 <= 0 is false -> chunk excluded at DB level
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": 0 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).is_empty(),
        "$eq 0: below element range [1,30], chunk pruned"
    );

    // v=99: 30 >= 99 is false -> chunk excluded at DB level
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": 99 } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).is_empty(),
        "$eq 99: above element range [1,30], chunk pruned"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_match(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_match";
    // topic_a: strings starting with 'a' and 'b'
    // topic_b: strings with no 'a' prefix
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                string_list_batch(10_000, &[1], &[vec!["apple", "banana", "cherry"]]),
            ),
            (
                "topic_b",
                string_list_batch(20_000, &[2], &[vec!["dog", "cat", "fish"]]),
            ),
        ],
    )
    .await;

    // "a*": topic_a has "apple" -> included; topic_b has none starting with 'a' -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$match": "a*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        r#"$match "a*": topic_a has "apple", included"#
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        r#"$match "a*": topic_b has no element starting with 'a', excluded"#
    );

    // "d*": topic_b has "dog" -> included; topic_a has none starting with 'd' -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$match": "d*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        r#"$match "d*": topic_a excluded"#
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        r#"$match "d*": topic_b has "dog", included"#
    );

    // "*a*": both have an element containing 'a' ("banana"/"apple" for a, "cat" for b)
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$match": "*a*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        r#"$match "*a*": topic_a has "apple"/"banana", included"#
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        r#"$match "*a*": topic_b has "cat", included"#
    );

    // "z*": no element in either topic starts with 'z' -> both excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$match": "z*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        r#"$match "z*": topic_a excluded"#
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        r#"$match "z*": topic_b excluded"#
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_all_match(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_all_match";
    // topic_a: all elements start with 'a'
    // topic_b: mixed — not all start with 'a'
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                string_list_batch(10_000, &[1], &[vec!["ant", "arrow", "alpha"]]),
            ),
            (
                "topic_b",
                string_list_batch(20_000, &[2], &[vec!["cat", "dog", "ant"]]),
            ),
        ],
    )
    .await;

    // "a": topic_a all match; topic_b has "cat"/"dog" that don't -> excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$match": "a*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        r#"$match "a": topic_a all start with 'a', included"#
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        r#"$match "a": topic_b has "cat"/"dog", excluded"#
    );

    // "?*": matches any non-empty string -> all elements in both topics match
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$match": "?*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        r#"$match "?*": topic_a included"#
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        r#"$match "?*": topic_b included"#
    );

    // "z*": no element starts with 'z' -> NOT all elements match -> both excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$match": "z*" } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        r#"$match "z*": topic_a excluded"#
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        r#"$match "z*": topic_b excluded"#
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_list_of_struct_any(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_list_of_struct_any";

    // topic_a: readings = [{x: 1.0, y: 10.0}]
    // topic_b: readings = [{x: 5.0, y: 50.0}]
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                struct_list_batch(10_000, &[vec![(1.0_f64, 10.0_f64)]]),
            ),
            (
                "topic_b",
                struct_list_batch(20_000, &[vec![(5.0_f64, 50.0_f64)]]),
            ),
        ],
    )
    .await;

    // x > 3.0: topic_b (x=5.0) included, topic_a (x=1.0) excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.readings[?].x": { "$gt": 3.0 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be included (x=5.0 > 3.0)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be excluded (x=1.0 not > 3.0)"
    );

    // y < 20.0: topic_a (y=10.0) included, topic_b (y=50.0) excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.readings[?].y": { "$lt": 20.0 } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (y=10.0 < 20.0)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded (y=50.0 not < 20.0)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_duplicate_specifier_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({ "ontology": { "mock.a[?].b[!].c": { "$eq": 1 } } }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_invalid_specifier_syntax_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({ "ontology": { "mock.readings[abc]": { "$eq": 1 } } }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_nested_list_is_unsupported(pool: sqlx::Pool<db::DatabaseType>) {
    use arrow::array::Array;

    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let mut list_builder = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    for inner in [[1, 2], [3, 4]] {
        for v in inner {
            list_builder.values().values().append_value(v);
        }
        list_builder.values().append(true);
    }
    list_builder.append(true);
    let list_array = list_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("list_test", list_array.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10_000_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )
    .unwrap();

    setup_topics(&mut client, "seq_nested_list", vec![("topic_a", batch)]).await;

    let err = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$gt": 0 } } }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_plain_list_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_plain_eq";
    // topic_a: rows [1,2,3] and [3,4,5] -> included when queried with [3,4,5]
    // topic_b: [10,20,30] -> excluded
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                int_list_batch(10_000, &[1, 2], &[vec![1, 2, 3], vec![3, 4, 5]]),
            ),
            ("topic_b", int_list_batch(20_000, &[1], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$eq": [3, 4, 5] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (has row [3,4,5])"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded ([10,20,30] != [3,4,5])"
    );

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$eq": [99, 100, 200] } } }),
    )
    .await
    .unwrap();
    assert!(
        topic_locators(&items).is_empty(),
        "no topic matches $eq [99,100,200]"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_plain_list_neq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_plain_neq";
    // topic_a: [1,2,3] — not equal to [3,5,7] -> included
    // topic_b: [3,5,7] — equal to [3,5,7] -> excluded
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[1], &[vec![3, 5, 7]])),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$neq": [3, 5, 7] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included ([1,2,3] != [3,5,7])"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded ([3,5,7] == [3,5,7])"
    );

    // [99,100,200] matches neither list -> $neq includes all
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$neq": [99, 100, 200] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$neq [99,100,200]: topic_a included"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$neq [99,100,200]: topic_b included"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_plain_list_eq_over_max_size_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_plain_eq_over_max";
    setup_topics(
        &mut client,
        seq,
        vec![("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]]))],
    )
    .await;

    // The default `max_size_plain_list_eq` is 1024; a 1025-element literal exceeds it
    // and must be rejected instead of silently dropping the filter.
    let big_list: Vec<i64> = (0..1025).collect();
    let err = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$eq": big_list } } }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_plain_list_bool_eq(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_plain_bool_eq";
    // topic_a: [true, false, true] -> matches $eq [true, false, true]
    // topic_b: [false, false]      -> excluded (different content/length)
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                bool_list_batch(10_000, &[1], &[vec![true, false, true]]),
            ),
            (
                "topic_b",
                bool_list_batch(20_000, &[1], &[vec![false, false]]),
            ),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$eq": [true, false, true] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (list == [true,false,true])"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded ([false,false] != [true,false,true])"
    );

    // $neq inverts the result.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test": { "$neq": [true, false, true] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$neq: topic_a excluded (list == [true,false,true])"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$neq: topic_b included ([false,false] != [true,false,true])"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_list_bool_specifiers(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_list_bool_spec";
    // topic_a: [true, false] -> has a true; not all true
    // topic_b: [true, true]  -> has a true; all true
    // topic_c: [false, false]-> no true; not all true
    setup_topics(
        &mut client,
        seq,
        vec![
            (
                "topic_a",
                bool_list_batch(10_000, &[1], &[vec![true, false]]),
            ),
            (
                "topic_b",
                bool_list_batch(20_000, &[1], &[vec![true, true]]),
            ),
            (
                "topic_c",
                bool_list_batch(30_000, &[1], &[vec![false, false]]),
            ),
        ],
    )
    .await;

    // [?] $eq true -> at least one element is true: topic_a, topic_b
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$eq": true } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "[?] $eq true: topic_a included (has a true)"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "[?] $eq true: topic_b included (has a true)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_c")),
        "[?] $eq true: topic_c excluded (no true)"
    );

    // [!] $eq true -> every element is true: only topic_b
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$eq": true } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "[!] $eq true: topic_b included (all true)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "[!] $eq true: topic_a excluded (not all true)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_c")),
        "[!] $eq true: topic_c excluded (not all true)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_list_of_struct_bool(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_list_of_struct_bool";
    // topic_a: readings = [{active: true}]
    // topic_b: readings = [{active: false}]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", struct_bool_list_batch(10_000, &[vec![true]])),
            ("topic_b", struct_bool_list_batch(20_000, &[vec![false]])),
        ],
    )
    .await;

    // readings[?].active $eq true -> topic_a included, topic_b excluded
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.readings[?].active": { "$eq": true } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "topic_a should be included (active=true)"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "topic_b should be excluded (active=false)"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_scalar_outside(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_scalar_outside";
    // Topic A: value range [1, 7], Topic B: value range [100, 106]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_batch(10_000, &[1, 2, 3, 4, 5, 6, 7])),
            (
                "topic_b",
                int_batch(20_000, &[100, 101, 102, 103, 104, 105, 106]),
            ),
        ],
    )
    .await;

    // outside([1, 7]): matches v < 1 || v > 7.
    // topic_a has no value below 1 or above 7 -> excluded (also exercises the chunk
    // stats pre-filter: min=1, max=7 -> neither min<1 nor max>7).
    // topic_b: 100 > 7 -> included.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$outside": [1, 7] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$outside [1,7]: topic_a has no value <1 or >7, excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$outside [1,7]: topic_b min=100 > 7, included"
    );

    // outside([0, 200]): both ranges are fully inside [0, 200] -> both excluded.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$outside": [0, 200] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$outside [0,200]: topic_a fully inside, excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$outside [0,200]: topic_b fully inside, excluded"
    );

    // outside([50, 60]): topic_a all < 50, topic_b all > 60 -> both included.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.value": { "$outside": [50, 60] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$outside [50,60]: topic_a values < 50, included"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$outside [50,60]: topic_b values > 60, included"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_any_outside(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_any_outside";
    // topic_a elements: [1, 2, 3], topic_b elements: [10, 20, 30]
    setup_topics(
        &mut client,
        seq,
        vec![
            ("topic_a", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            ("topic_b", int_list_batch(20_000, &[2], &[vec![10, 20, 30]])),
        ],
    )
    .await;

    // [?] outside([a, b]): at least one element is < a || > b.

    // outside([0, 30]): no element below 0 or above 30 in either topic -> both excluded.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$outside": [0, 30] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$outside [0,30]: topic_a has no element outside, excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$outside [0,30]: topic_b has no element outside, excluded"
    );

    // outside([0, 25]): only topic_b has 30 > 25.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$outside": [0, 25] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        !locators.contains(&format!("{seq}/topic_a")),
        "$outside [0,25]: topic_a all in [0,25], excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_b")),
        "$outside [0,25]: topic_b has 30 > 25, included"
    );

    // outside([2, 30]): only topic_a has 1 < 2.
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[?]": { "$outside": [2, 30] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);
    assert!(
        locators.contains(&format!("{seq}/topic_a")),
        "$outside [2,30]: topic_a has 1 < 2, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_b")),
        "$outside [2,30]: topic_b all in [2,30], excluded"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_ontology_all_outside(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_all_outside";
    // [!] outside([5, 35]) means EVERY element is < 5 || > 35, i.e. no element inside [5, 35].
    setup_topics(
        &mut client,
        seq,
        vec![
            // all < 5 -> all outside -> included
            ("topic_low", int_list_batch(10_000, &[1], &[vec![1, 2, 3]])),
            // all in [5, 35] -> none outside -> excluded
            (
                "topic_mid",
                int_list_batch(20_000, &[2], &[vec![10, 20, 30]]),
            ),
            // 1 < 5 (outside) but 10 in [5, 35] (inside) -> NOT all outside -> excluded.
            (
                "topic_straddle",
                int_list_batch(30_000, &[3], &[vec![1, 10]]),
            ),
            // all > 35 -> all outside -> included
            ("topic_high", int_list_batch(40_000, &[4], &[vec![40, 50]])),
            // 1 < 5 and 40 > 35, both outside -> all outside -> included
            ("topic_split", int_list_batch(50_000, &[5], &[vec![1, 40]])),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.list_test[!]": { "$outside": [5, 35] } } }),
    )
    .await
    .unwrap();
    let locators = topic_locators(&items);

    assert!(
        locators.contains(&format!("{seq}/topic_low")),
        "$outside [5,35]: topic_low [1,2,3] all < 5, included"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_mid")),
        "$outside [5,35]: topic_mid [10,20,30] all inside, excluded"
    );
    assert!(
        !locators.contains(&format!("{seq}/topic_straddle")),
        "$outside [5,35]: topic_straddle [1,10] has 10 inside [5,35], excluded"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_high")),
        "$outside [5,35]: topic_high [40,50] all > 35, included"
    );
    assert!(
        locators.contains(&format!("{seq}/topic_split")),
        "$outside [5,35]: topic_split [1,40] both outside, included"
    );

    server.shutdown().await;
}
