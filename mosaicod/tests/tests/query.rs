#![allow(unused_crate_dependencies)]
use mosaicod_db as db;
use serde_json::json;
use tests::{actions, common};

async fn setup_topics_with_metadata(
    client: &mut common::Client,
    sequence_name: &str,
    topics: &[(&str, serde_json::Value)],
) {
    actions::sequence_create(client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(client, sequence_name)
        .await
        .unwrap();

    for (topic_suffix, metadata) in topics {
        let topic_name = format!("{sequence_name}/{topic_suffix}");
        let topic_uuid = actions::topic_create(
            client,
            &session_uuid,
            &topic_name,
            Some(&metadata.to_string()),
        )
        .await
        .unwrap();

        let batches = vec![mosaicod_ext::arrow::testing::dummy_batch()];
        actions::do_put(client, &topic_uuid, &topic_name, batches, false)
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
async fn test_query_user_metadata_in_integer(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "my_seq";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[
            ("topic_1", json!({"x": 1})),
            ("topic_6", json!({"x": 6})),
            ("topic_99", json!({"x": 99})),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({
            "topic": {
                "user_metadata": {
                    "x": { "$in": [1, 6, -1] }
                }
            }
        }),
    )
    .await
    .unwrap();

    let locators = topic_locators(&items);
    assert_eq!(
        locators.len(),
        2,
        "expected 2 matching topics, got: {locators:?}"
    );
    assert!(locators.contains(&format!("{seq}/topic_1")));
    assert!(locators.contains(&format!("{seq}/topic_6")));
    assert!(!locators.contains(&format!("{seq}/topic_99")));

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_user_metadata_match_string(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_umeta_match";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[
            ("topic_truck", json!({"vehicle": "truck_scania"})),
            ("topic_car", json!({"vehicle": "ferrari"})),
            ("topic_supertruck", json!({"vehicle": "supertruck_volvo"})),
        ],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({
            "topic": {
                "user_metadata": {
                    "vehicle": { "$match": "^truck" }
                }
            }
        }),
    )
    .await
    .unwrap();

    let locators = topic_locators(&items);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&format!("{seq}/topic_truck")));
    assert!(!locators.contains(&format!("{seq}/topic_car")));
    assert!(!locators.contains(&format!("{seq}/topic_supertruck")));

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_with_dict_body_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$in": {"key": 1} } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_with_nested_list_elements_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$in": [[1, 2], [3, 4]] } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_with_array_value_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$match": [1, 2, 3] } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_on_integer_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$match": 42 } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_on_boolean_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "flag": { "$match": true } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_with_booleans_is_allowed(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "my_seq";

    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_truck", json!({"is_on_the_way": "true"}))],
    )
    .await;

    let item = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "is_on_the_way": { "$in": [true, false] } } }
        }),
    )
    .await
    .unwrap();

    let locators = topic_locators(&item);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&format!("{seq}/topic_truck")));

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_with_empty_list_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_empty";
    setup_topics_with_metadata(&mut client, seq, &[("topic_a", json!({"x": 1}))]).await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$in": [] } } }
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "empty $in should not silently return results"
    );

    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_on_list_valued_field_errors_at_runtime(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_list_field";
    setup_topics_with_metadata(&mut client, seq, &[("topic_list", json!({"x": [1, 2, 3]}))]).await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$in": [1, 6] } } }
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "querying a list-valued field with $in must error, not silently match"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_in_on_dict_valued_field_errors_at_runtime(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_dict_field";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_dict", json!({"x": {"nested": 1}}))],
    )
    .await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "x": { "$in": [1, 6] } } }
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "querying a dict-valued field with $in must error, not silently match"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_with_dict_body_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let err = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "vehicle": { "$match": {"key": "val"} } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_invalid_regex_errors_at_runtime(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match_bad_regex";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_a", json!({"vehicle": "truck_scania"}))],
    )
    .await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "vehicle": { "$match": "((unclosed" } } }
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "invalid regex must produce a runtime error"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_on_list_valued_field_returns_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match_list_field";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_list", json!({"vehicle": ["truck", "car"]}))],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "vehicle": { "$match": "^truck" } } }
        }),
    )
    .await
    .unwrap();

    assert!(
        topic_locators(&items).is_empty(),
        "match on a list-valued field must return no results, not error"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_on_dict_valued_field_returns_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match_dict_field";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_dict", json!({"vehicle": {"brand": "scania"}}))],
    )
    .await;

    let items = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "vehicle": { "$match": "^scania" } } }
        }),
    )
    .await
    .unwrap();

    assert!(
        topic_locators(&items).is_empty(),
        "match on a dict-valued field must return no results, not error"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_empty_pattern_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_match_empty_pattern";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[
            ("topic_a", json!({"vehicle": "truck_scania"})),
            ("topic_b", json!({"vehicle": "ferrari"})),
            ("topic_no_field", json!({"other": "value"})),
        ],
    )
    .await;

    let res = actions::query(
        &mut client,
        json!({
            "topic": { "user_metadata": { "vehicle": { "$match": "" } } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(res.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_topic_name_match_percent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_topic_match";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_a", json!({})), ("topic_b", json!({}))],
    )
    .await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "locator": { "$match": "%" } }
        }),
    )
    .await
    .unwrap();

    assert_eq!(result.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_topic_name_match_all(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_topic_match_1";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_a", json!({})), ("topic_b", json!({}))],
    )
    .await;

    let seq = "seq_topic_match_2";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_c", json!({})), ("topic_d", json!({}))],
    )
    .await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "locator": { "$match": ".*" } }
        }),
    )
    .await
    .unwrap();

    assert_eq!(result.len(), 2);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_topic_name_match_empty_is_rejected(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_topic_match";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[("topic_a", json!({})), ("topic_b", json!({}))],
    )
    .await;

    let result = actions::query(
        &mut client,
        json!({
            "topic": { "locator": { "$match": "" } }
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(result.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}
