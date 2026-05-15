#![allow(unused_crate_dependencies)]
use mosaicod_core::types::Uuid;
use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

// ===========================================================================
// Do put
// ===========================================================================
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put(pool: sqlx::Pool<db::DatabaseType>) {
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

    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    let mut response_reader = response.into_inner();
    if response_reader.message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    // Check do_put() without descriptor.
    let batches = vec![ext::arrow::testing::dummy_batch()];
    assert_eq!(
        actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, true)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_nonexistent_topic_uuid(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let fake_uuid = Uuid::new();
    let batches = vec![ext::arrow::testing::dummy_batch()];

    let res = actions::do_put(
        &mut client,
        &fake_uuid,
        "test_sequence/ghost",
        batches,
        false,
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_on_locked_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/locked", sequence_name);

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

    let batches = vec![ext::arrow::testing::dummy_batch()];
    let res = actions::do_put(&mut client, &topic_uuid, topic_name, batches, false).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::FailedPrecondition);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_descriptor_mismatch(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_a = &format!("{}/topic_a", sequence_name);
    let topic_b = &format!("{}/topic_b", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let uuid_a = actions::topic_create(&mut client, &session_uuid, topic_a, None)
        .await
        .unwrap();
    let _uuid_b = actions::topic_create(&mut client, &session_uuid, topic_b, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    let res = actions::do_put(&mut client, &uuid_a, topic_b, batches, false).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_no_schema_empty_batches(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/no_batches", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let res = actions::do_put(&mut client, &topic_uuid, topic_name, vec![], false).await;
    assert!(res.is_err(), "do_put with no batches should error");

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_schema_with_empty_batches(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/no_batches", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let record_batch_vec = vec![ext::arrow::testing::dummy_empty_batch()];
    let res = actions::do_put(
        &mut client,
        &topic_uuid,
        topic_name,
        record_batch_vec,
        false,
    )
    .await;
    assert!(
        res.is_ok(),
        "do_put with schema but empty batches should not fail."
    );

    server.shutdown().await;
}
