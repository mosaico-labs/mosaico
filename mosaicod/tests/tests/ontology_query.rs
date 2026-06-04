#![allow(unused_crate_dependencies)]
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use mosaicod_db as db;
use serde_json::json;
use std::sync::Arc;
use tests::{actions, common};

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

    // "^truck" should match only topic_truck
    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$match": "^truck" } } }),
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

    let items = actions::query(
        &mut client,
        json!({ "ontology": { "mock.name": { "$match": ".*" } } }),
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
