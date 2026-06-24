#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_ext::arrow::testing::clustering_test_batch;
use mosaicod_marshal::{self as marshal, Ontology, flight::FilterTimestampRange};
use serde_json::json;
use tests::{self, actions, common};
// ===========================================================================
// Topic tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid =
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
            .await
            .unwrap();
    assert!(topic_uuid.is_valid());

    // Passing a wrong session uuid should trigger a NotFound error.
    let err = actions::topic_create(
        &mut client,
        &topic_uuid, // wrong uuid
        "test_sequence/my_topic",
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Creating a topic with same name should trigger an ALreadyExists error.
    let err = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // Create topic with malformed metadata should give an InvalidArgument error.
    assert_eq!(
        actions::topic_create(
            &mut client,
            &session_uuid,
            "test_sequence/my_topic",
            Some("{")
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );

    // Trying to create a topic inside an already finalized session should return a FailedPrecondition error.
    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(
        &mut client,
        &topic_uuid,
        "test_sequence/my_topic",
        batches,
        false,
    )
    .await
    .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    assert_eq!(
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic2", None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_create_invalid_format(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let bad_names = [
        "no_slash",
        "wrong_sequence/topic",
        "test_sequence/",
        "/topic_only",
    ];

    for name in bad_names {
        let res = actions::topic_create(&mut client, &session_uuid, name, None).await;
        assert!(res.is_err(), "topic name {:?} should be rejected", name);
        let code = res.unwrap_err().code();
        assert!(
            matches!(
                code,
                tonic::Code::InvalidArgument
                    | tonic::Code::NotFound
                    | tonic::Code::PermissionDenied
            ),
            "name {:?} got unexpected code {:?}",
            name,
            code
        );
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    actions::topic_delete(&mut client, topic_name)
        .await
        .unwrap();

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_flight_info(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    // Check flight info for a locked topic without data.

    let topic_name = "test_sequence/my_empty_topic";

    let uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Metadata should be available even if topic is unlocked, but not all info are filled.
    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(!app_metadata.locked);
    assert_eq!(app_metadata.resource_locator, topic_name);
    assert!(app_metadata.info.is_none());
    assert_ne!(app_metadata.created_at_ns, 0);
    assert!(app_metadata.completed_at_ns.is_none());

    let batches = vec![ext::arrow::testing::dummy_empty_batch()];

    let response = actions::do_put(&mut client, &uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(app_metadata.locked);
    assert_ne!(app_metadata.created_at_ns, 0);
    assert_ne!(app_metadata.completed_at_ns.unwrap(), 0);
    assert_eq!(app_metadata.resource_locator, topic_name);

    let info = app_metadata.info.unwrap();
    assert_eq!(info.chunks_number, 0);
    assert_eq!(info.total_bytes, 0);
    assert!(info.timestamp.is_none());

    // Check flight info for a locked topic with data.

    let topic_name = "test_sequence/my_topic";

    let uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(app_metadata.locked);
    assert_ne!(app_metadata.created_at_ns, 0);
    assert_ne!(app_metadata.completed_at_ns.unwrap(), 0);
    assert_eq!(app_metadata.resource_locator, topic_name);

    let info = app_metadata.info.unwrap();
    assert_eq!(info.chunks_number, 1);
    assert_eq!(info.total_bytes, 895);
    let ts_range: types::TimestampRange = info.timestamp.unwrap().into();
    assert_eq!(ts_range.start.as_i64(), 10000);
    assert_eq!(ts_range.end.as_i64(), 10030);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_create(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let error_msg = format!("Error in {}", topic_name);
    actions::topic_notification_create(
        &mut client,
        topic_name,
        types::NotificationType::Error.to_string(),
        error_msg,
    )
    .await
    .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_list(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);
    let notification_type = types::NotificationType::Error.to_string();
    let notifications_size = 5;

    actions::setup_topic_with_notifications(
        &mut client,
        sequence_name,
        topic_name,
        notification_type.clone(),
        notifications_size,
    )
    .await
    .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), notifications_size);

    for (i, notification) in notifications.iter().enumerate() {
        let error_msg = format!("Error {}_{}", topic_name, i + 1);
        assert_eq!(notification["notification_type"], notification_type);
        assert_eq!(notification["name"].as_str().unwrap(), topic_name);
        assert_eq!(notification["msg"], error_msg);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);
    let notification_type = types::NotificationType::Error.to_string();
    let notifications_size = 5;

    actions::setup_topic_with_notifications(
        &mut client,
        sequence_name,
        topic_name,
        notification_type.clone(),
        notifications_size,
    )
    .await
    .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 5);

    actions::topic_notification_purge(&mut client, topic_name)
        .await
        .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_create_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::topic_notification_create(
        &mut client,
        "test_sequence/never_existed",
        types::NotificationType::Error.to_string(),
        "msg".to_string(),
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_list_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::topic_delete(&mut client, "test_sequence/never_existed").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::topic_delete(&mut client, "ghost_sequence/ghost_topic").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete_unlocked(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unlocked_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert!(res.is_ok());

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

/// Setup: sequence + session + topic + batch upload + finalize.
async fn setup_topic_with_batches(
    client: &mut common::Client,
    sequence_name: &str,
    topic_name: &str,
    batches: Vec<arrow::array::RecordBatch>,
) {
    actions::sequence_create(client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    actions::do_put(client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(client, &session_uuid)
        .await
        .unwrap();
}

async fn setup_topic_with_batches_in_existing_seq(
    client: &mut common::Client,
    sequence_name: &str,
    topic_name: &str,
    batches: Vec<arrow::array::RecordBatch>,
) {
    let (_, session_uuid) = actions::session_create(client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    actions::do_put(client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(client, &session_uuid)
        .await
        .unwrap();
}

fn ontology_value_gt_5() -> Ontology {
    serde_json::from_value(json!({
        "mock.value": { "$gt": 5 }
    }))
    .unwrap()
}

fn ontology_value_lt_3() -> Ontology {
    serde_json::from_value(json!({
        "mock.value": { "$lt": 3 }
    }))
    .unwrap()
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_three_clusters(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_three";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 110, 120, 200, 210, 220, 300, 310, 320];
    let val: Vec<i64> = vec![10, 10, 10, 10, 10, 10, 10, 10, 10];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 3, "expected 3 clusters, got: {clusters:?}");

    let expected = [(100u64, 120u64), (200, 220), (300, 320)];
    for (i, (exp_start, exp_end)) in expected.iter().enumerate() {
        let start = clusters[i]["ts"]["start_ns"].as_u64().unwrap();
        let end = clusters[i]["ts"]["end_ns"].as_u64().unwrap();
        let id = clusters[i]["id"].as_u64().unwrap();
        assert_eq!(start, *exp_start, "cluster {i} start");
        assert_eq!(end, *exp_end, "cluster {i} end");
        assert_eq!(id, i as u64, "cluster {i} id");
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_single_cluster_via_gap(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_single";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![1_000, 1_010, 1_020, 1_030, 1_040, 1_050];
    let val: Vec<i64> = vec![10; 6];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters = actions::topic_filter_clusterize(
        &mut client,
        topic_name,
        100, // dt_ns >> max gap → un cluster
        ontology_value_gt_5(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 1_000);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 1_050);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_dt_zero_returns_full_range(
    pool: sqlx::Pool<db::DatabaseType>,
) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_dt_zero";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 500, 1_000, 5_000, 10_000];
    let val: Vec<i64> = vec![10; 5];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 0, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 1, "dt_ns=0 must yield exactly one cluster");
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 100);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 10_000);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_ontology_actually_filters(
    pool: sqlx::Pool<db::DatabaseType>,
) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_filter";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 110, 120, 130, 140, 500, 510, 520];
    let val: Vec<i64> = vec![10, 1, 10, 1, 10, 10, 1, 10];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 2, "got: {clusters:?}");
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 100);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 140);
    assert_eq!(clusters[1]["ts"]["start_ns"].as_u64().unwrap(), 500);
    assert_eq!(clusters[1]["ts"]["end_ns"].as_u64().unwrap(), 520);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_empty_result(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_empty";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 200, 300];
    let val: Vec<i64> = vec![1, 2, 3];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert!(
        clusters.is_empty(),
        "expected 0 clusters, got: {clusters:?}"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_with_time_range(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_range";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 200, 300, 1_000, 1_100, 2_000, 2_100];
    let val: Vec<i64> = vec![10; 7];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let ts_range: FilterTimestampRange = serde_json::from_value(json!({
        "start_ns": 500u64, "end_ns": 1_500u64
    }))
    .unwrap();

    let clusters = actions::topic_filter_clusterize(
        &mut client,
        topic_name,
        100,
        ontology_value_gt_5(),
        Some(ts_range),
    )
    .await
    .unwrap();

    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 1_000);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 1_100);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_single_row(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_one_row";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 200, 300];
    let val: Vec<i64> = vec![1, 10, 1];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 200);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 200);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_across_batches(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_multi";
    let topic_name = &format!("{sequence_name}/topic");

    let b1 = clustering_test_batch(&[100, 110, 120], &[10; 3]);
    let b2 = clustering_test_batch(&[130, 140], &[10; 2]);
    let b3 = clustering_test_batch(&[1_000, 1_010], &[10; 2]);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![b1, b2, b3]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 2, "got: {clusters:?}");
    assert_eq!(clusters[0]["ts"]["start_ns"].as_u64().unwrap(), 100);
    assert_eq!(clusters[0]["ts"]["end_ns"].as_u64().unwrap(), 140);
    assert_eq!(clusters[1]["ts"]["start_ns"].as_u64().unwrap(), 1_000);
    assert_eq!(clusters[1]["ts"]["end_ns"].as_u64().unwrap(), 1_010);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_gap_equals_dt(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "seq_gap_eq";
    let topic_name = &format!("{sequence_name}/topic");

    let ts: Vec<i64> = vec![100, 150, 200, 250];
    let val: Vec<i64> = vec![10; 4];
    let batch = clustering_test_batch(&ts, &val);

    setup_topic_with_batches(&mut client, sequence_name, topic_name, vec![batch]).await;

    let clusters =
        actions::topic_filter_clusterize(&mut client, topic_name, 50, ontology_value_gt_5(), None)
            .await
            .unwrap();

    assert_eq!(clusters.len(), 1, "got: {clusters:?}");

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_wrong_timestamp(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/test_topic", sequence_name);
    let clustering_dt_ns: u64 = 10;
    let ontology: Ontology = serde_json::from_value(json!({
        "imu.acceleration.x": { "$gt": 5 }
    }))
    .unwrap();

    let timestamp: FilterTimestampRange = serde_json::from_value(json!({
        "start_ns": 10000, "end_ns": 3000
    }))
    .unwrap();

    let res = actions::topic_filter_clusterize(
        &mut client,
        topic_name,
        clustering_dt_ns,
        ontology,
        Some(timestamp),
    )
    .await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_clusterize_more_ontology(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/test_topic", sequence_name);
    let clustering_dt_ns: u64 = 10;
    let ontology: Ontology = serde_json::from_value(json!({
        "imu.acceleration.x": { "$gt": 5 }, "imu.acceleration.y": { "$gt": 1 }
    }))
    .unwrap();

    let timestamp: FilterTimestampRange = serde_json::from_value(json!({
        "start_ns": 10000, "end_ns": 3000000
    }))
    .unwrap();

    let res = actions::topic_filter_clusterize(
        &mut client,
        topic_name,
        clustering_dt_ns,
        ontology,
        Some(timestamp),
    )
    .await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_intersect_no_intersection(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_no_intersect";

    // Topic 1: cluster [100, 150]
    let t1 = &format!("{seq}/topic_1");
    setup_topic_with_batches(
        &mut client,
        seq,
        t1,
        vec![clustering_test_batch(&[100, 150], &[6, 7])],
    )
    .await;

    // Topic 2: cluster [300, 400]
    let t2 = &format!("{seq}/topic_2");
    setup_topic_with_batches_in_existing_seq(
        &mut client,
        seq,
        t2,
        vec![clustering_test_batch(&[300, 400], &[1, 1])],
    )
    .await;

    let topics = vec![
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t1.to_owned(),
            clustering_dt_ns: 100,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t2.to_owned(),
            clustering_dt_ns: 100,
            ontology: ontology_value_lt_3(),
            timestamp_range: None,
        },
    ];

    let items = actions::topic_filter_intersect(&mut client, topics, 10)
        .await
        .unwrap();

    assert_eq!(items.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_intersect_multiple(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_multi";

    // Topic 1: clusters [100,110] and [500,510]
    let t1 = &format!("{seq}/topic_1");
    setup_topic_with_batches(
        &mut client,
        seq,
        t1,
        vec![clustering_test_batch(&[100, 110, 500, 510], &[6, 7, 8, 9])],
    )
    .await;

    // Topic 2: clusters [105,115] and [495,505]
    let t2 = &format!("{seq}/topic_2");
    setup_topic_with_batches_in_existing_seq(
        &mut client,
        seq,
        t2,
        vec![clustering_test_batch(&[105, 115, 495, 505], &[1, 2, 1, 2])],
    )
    .await;

    let topics = vec![
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t1.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t2.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_lt_3(),
            timestamp_range: None,
        },
    ];

    let items = actions::topic_filter_intersect(&mut client, topics, 0)
        .await
        .unwrap();

    assert_eq!(items.len(), 2, "got: {items:?}");

    assert_eq!(items[0]["ts"]["start_ns"].as_u64().unwrap(), 105);
    assert_eq!(items[0]["ts"]["end_ns"].as_u64().unwrap(), 110);

    assert_eq!(items[1]["ts"]["start_ns"].as_u64().unwrap(), 500);
    assert_eq!(items[1]["ts"]["end_ns"].as_u64().unwrap(), 505);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_intersect_three_topics(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_three";

    // Topic 1: cluster [100, 200]
    let t1 = &format!("{seq}/topic_1");
    setup_topic_with_batches(
        &mut client,
        seq,
        t1,
        vec![clustering_test_batch(&[100, 200], &[6, 7])],
    )
    .await;

    // Topic 2: cluster [120, 180]
    let t2 = &format!("{seq}/topic_2");
    setup_topic_with_batches_in_existing_seq(
        &mut client,
        seq,
        t2,
        vec![clustering_test_batch(&[120, 180], &[1, 2])],
    )
    .await;

    // Topic 3: cluster [130, 170]
    let t3 = &format!("{seq}/topic_3");
    setup_topic_with_batches_in_existing_seq(
        &mut client,
        seq,
        t3,
        vec![clustering_test_batch(&[130, 170], &[6, 7])],
    )
    .await;

    let topics = vec![
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t1.to_owned(),
            clustering_dt_ns: 150,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t2.to_owned(),
            clustering_dt_ns: 100,
            ontology: ontology_value_lt_3(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t3.to_owned(),
            clustering_dt_ns: 100,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
    ];

    let items = actions::topic_filter_intersect(&mut client, topics, 0)
        .await
        .unwrap();

    assert_eq!(items.len(), 1, "got: {items:?}");
    assert_eq!(items[0]["ts"]["start_ns"].as_u64().unwrap(), 130);
    assert_eq!(items[0]["ts"]["end_ns"].as_u64().unwrap(), 170);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_intersect_within_tolerance(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let seq = "seq_tolerance";

    // Topic 1: cluster [100, 140]
    let t1 = &format!("{seq}/topic_1");
    setup_topic_with_batches(
        &mut client,
        seq,
        t1,
        vec![clustering_test_batch(&[100, 140], &[6, 7])],
    )
    .await;

    // Topic 2: cluster [155, 200]
    let t2 = &format!("{seq}/topic_2");
    setup_topic_with_batches_in_existing_seq(
        &mut client,
        seq,
        t2,
        vec![clustering_test_batch(&[155, 200], &[1, 2])],
    )
    .await;

    let topics = vec![
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t1.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t2.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_lt_3(),
            timestamp_range: None,
        },
    ];

    // intersect_dt_ns=20: gap 15 < 20, within tolerance.
    // Tolerance interval: [155 - 20/2, 140 + 20/2] = [145, 150]
    let items = actions::topic_filter_intersect(&mut client, topics, 20)
        .await
        .unwrap();

    assert_eq!(items.len(), 1, "got: {items:?}");
    assert_eq!(items[0]["ts"]["start_ns"].as_u64().unwrap(), 145);
    assert_eq!(items[0]["ts"]["end_ns"].as_u64().unwrap(), 150);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_filter_intersect_different_sequences(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let t1 = "my_seq_1/topic";
    setup_topic_with_batches(
        &mut client,
        "my_seq_1",
        t1,
        vec![clustering_test_batch(&[100], &[6])],
    )
    .await;

    let t2 = "my_seq_2/topic";
    setup_topic_with_batches(
        &mut client,
        "my_seq_2",
        t2,
        vec![clustering_test_batch(&[100], &[1])],
    )
    .await;

    let topics = vec![
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t1.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_gt_5(),
            timestamp_range: None,
        },
        mosaicod_marshal::requests::TopicClusterizeParams {
            locator: t2.to_owned(),
            clustering_dt_ns: 50,
            ontology: ontology_value_lt_3(),
            timestamp_range: None,
        },
    ];

    let res = actions::topic_filter_intersect(&mut client, topics, 0).await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}
