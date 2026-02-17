#![allow(unused_crate_dependencies)]

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