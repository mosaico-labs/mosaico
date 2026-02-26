#![allow(unused_crate_dependencies)]

use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    actions::sequence_create(&mut client, "test_sequence", None)
        .await
        .unwrap();

    // Check that sequences with same name are not allowed.
    assert_eq!(
        actions::sequence_create(&mut client, "test_sequence", None)
            .await
            .unwrap_err()
            .message(),
        "sequence `test_sequence` already exists"
    );

    // Check malformed metadata json.
    assert_eq!(
        actions::sequence_create(&mut client, "test_malformed_sequence", Some("{"))
            .await
            .unwrap_err()
            .message(),
        "action error"
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn topic_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Create topic with malformed metadata.
    assert_eq!(
        actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", Some("{"))
            .await
            .unwrap_err()
            .message(),
        "action error"
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn do_put(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None)
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
            .message(),
        "missing descriptor in request"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
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

    actions::session_finalize(&mut client, session_uuid).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_abort(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    actions::session_abort(&mut client, session_uuid).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, session_uuid).await;

    actions::sequence_delete(&mut client, "test_sequence").await;

    server.shutdown().await;
}
