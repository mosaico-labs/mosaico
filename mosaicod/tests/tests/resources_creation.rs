#![allow(unused_crate_dependencies)]

use mosaicod_ext as ext;
use mosaicod_repo as repo;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn sequence_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;

    let mut client = common::Client::new(common::HOST, port).await;

    actions::sequence_create(&mut client, "test_sequence", None).await;

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn session_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;
    let mut client = common::Client::new(common::HOST, port).await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None).await;
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn topic_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;
    let mut client = common::Client::new(common::HOST, port).await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None).await;
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None).await;
    assert!(uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn do_put(pool: sqlx::Pool<repo::Database>) {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;
    let mut client = common::Client::new(common::HOST, port).await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None).await;
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None).await;
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches).await;

    let mut response_reader = response.into_inner();
    while let Some(_) = response_reader.message().await.unwrap() {
        panic!("Received a not-empty response!");
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn session_finalize(pool: sqlx::Pool<repo::Database>) {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;
    let mut client = common::Client::new(common::HOST, port).await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None).await;
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());
    let uuid =
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None).await;
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches).await;

    let mut response_reader = response.into_inner();
    while let Some(_) = response_reader.message().await.unwrap() {
        panic!("Received a not-empty response!");
    }

    actions::session_finalize(&mut client, session_uuid).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
async fn session_abort(pool: sqlx::Pool<repo::Database>) {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;
    let mut client = common::Client::new(common::HOST, port).await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None).await;
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());
    let uuid =
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None).await;
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches).await;

    let mut response_reader = response.into_inner();
    while let Some(_) = response_reader.message().await.unwrap() {
        panic!("Received a not-empty response!");
    }

    actions::session_abort(&mut client, session_uuid).await;

    server.shutdown().await;
}
