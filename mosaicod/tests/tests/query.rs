#![allow(unused_crate_dependencies)]
use mosaicod_db as db;
use mosaicod_marshal::responses;
use mosaicod_proto::v1::query as proto_query;
use serde_json::json;
use tests::{actions, common, filter};

async fn setup_topics_with_metadata(
    client: &mut common::Client,
    sequence_name: &str,
    topics: &[(&str, serde_json::Value)],
) {
    actions::sequence_create(client, sequence_name, "")
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(client, sequence_name)
        .await
        .unwrap();

    for (topic_suffix, metadata) in topics {
        let topic_name = format!("{sequence_name}/{topic_suffix}");
        let topic_uuid =
            actions::topic_create(client, &session_uuid, &topic_name, &metadata.to_string())
                .await
                .unwrap();

        let batches = vec![mosaicod_ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1)];
        actions::do_put(client, &topic_uuid, &topic_name, batches, false)
            .await
            .unwrap();
    }

    actions::session_finalize(client, &session_uuid)
        .await
        .unwrap();
}

fn topic_locator_and_ontology(items: &[responses::ResponseQueryItem]) -> Vec<(String, String)> {
    items
        .iter()
        .flat_map(|item| {
            item.topics
                .iter()
                .map(|t| (t.locator.clone(), t.ontology_tag.clone()))
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::is_in([1, 6, -1]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&items);
    assert_eq!(
        locators.len(),
        2,
        "expected 2 matching topics, got: {locators:?}"
    );
    assert!(locators.contains(&(format!("{seq}/topic_1"), "mock".to_owned())));
    assert!(locators.contains(&(format!("{seq}/topic_6"), "mock".to_owned())));
    assert!(!locators.contains(&(format!("{seq}/topic_99"), "mock".to_owned())));

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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("vehicle", filter::matches("truck*"))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&items);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&(format!("{seq}/topic_truck"), "mock".to_owned())));
    assert!(!locators.contains(&(format!("{seq}/topic_car"), "mock".to_owned())));
    assert!(!locators.contains(&(format!("{seq}/topic_supertruck"), "mock".to_owned())));

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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([(
                    "x",
                    filter::cond(proto_query::Operator::In, None),
                )]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([(
                    "x",
                    filter::cond(proto_query::Operator::In, None),
                )]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::matches([1, 2, 3]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::matches(42))]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("flag", filter::matches(true))]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        &[("topic_truck", json!({"is_on_the_way": true}))],
    )
    .await;

    let item = actions::query(
        &mut client,
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("is_on_the_way", filter::is_in([true, false]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&item);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&(format!("{seq}/topic_truck"), "mock".to_owned())));

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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::is_in(Vec::<i64>::new()))]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
async fn test_query_in_on_list_valued_field(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_in_list_field";
    setup_topics_with_metadata(&mut client, seq, &[("topic_list", json!({"x": [1, 2, 3]}))]).await;

    let result = actions::query(
        &mut client,
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::is_in([1, 6]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&result);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&(format!("{seq}/topic_list"), "mock".to_owned())));

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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::is_in([1, 6]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(result.is_empty());

    let result = actions::query(
        &mut client,
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x.nested", filter::is_in([1, 6]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&result);
    assert_eq!(
        locators.len(),
        1,
        "expected 1 matching topic, got: {locators:?}"
    );
    assert!(locators.contains(&(format!("{seq}/topic_dict"), "mock".to_owned())));

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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([(
                    "vehicle",
                    filter::cond(proto_query::Operator::Match, None),
                )]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("vehicle", filter::matches("((unclosed"))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;

    assert!(
        result.is_err(),
        "invalid regex must produce a runtime error"
    );

    server.shutdown().await;
}

// We expect a query match on a list to return a value because json path in LAX mode unwrap the list into its elements.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_match_on_list_valued_field_returns_value(pool: sqlx::Pool<db::DatabaseType>) {
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("vehicle", filter::matches("truck"))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(items.len(), 1);
    assert_eq!(
        topic_locator_and_ontology(&items)[0].0,
        "seq_match_list_field/topic_list"
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("vehicle", filter::matches("scania"))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        topic_locator_and_ontology(&items).is_empty(),
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("vehicle", filter::matches(""))]),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                name: Some(filter::matches("%")),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                name: Some(filter::matches("*")),
                ..Default::default()
            }),
            ..Default::default()
        },
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
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                name: Some(filter::matches("")),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();

    assert_eq!(result.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_invalid_keys(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_invalid_keys";
    setup_topics_with_metadata(
        &mut client,
        seq,
        &[
            ("topic_a", json!({"vehicle": "truck_scania"})),
            ("topic_b", json!({"vehicle": "ferrari"})),
        ],
    )
    .await;

    let invalid_patterns = [
        "#", "$", "%", "?", "^", "{", "\"", "\\", "/", "@", "+", "--", "***",
    ];

    for p in invalid_patterns {
        let key_path = format!("vehicle{p}");

        let res = actions::query(
            &mut client,
            proto_query::Filter {
                topic: Some(proto_query::TopicFilter {
                    user_metadata: filter::metadata([(key_path.as_str(), filter::eq("ferrari"))]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(res.code(), tonic::Code::InvalidArgument);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_query_user_metadata_outside_integer(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_umeta_outside";
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

    // outside([5, 50]): matches x < 5 || x > 50.
    let items = actions::query(
        &mut client,
        proto_query::Filter {
            topic: Some(proto_query::TopicFilter {
                user_metadata: filter::metadata([("x", filter::outside([5, 50]))]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let locators = topic_locator_and_ontology(&items);
    assert_eq!(
        locators.len(),
        2,
        "expected 2 matching topics, got: {locators:?}"
    );
    assert!(
        locators.contains(&(format!("{seq}/topic_1"), "mock".to_owned())),
        "topic_1 (x=1 < 5) included"
    );
    assert!(
        locators.contains(&(format!("{seq}/topic_99"), "mock".to_owned())),
        "topic_99 (x=99 > 50) included"
    );
    assert!(
        !locators.contains(&(format!("{seq}/topic_6"), "mock".to_owned())),
        "topic_6 (x=6 inside [5,50]) excluded"
    );

    server.shutdown().await;
}
