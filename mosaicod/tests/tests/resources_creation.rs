use mosaicod_repo as repo;
use tests::{actions, common};

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
fn sequence_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::Server::new(common::HOST, port, pool).await;

    let mut client = common::Client::new(common::HOST, port).await;

    actions::sequence_create(&mut client, "test_sequence", None).await;

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
fn topic_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
    let port = common::random_port();
    let server = common::Server::new(common::HOST, port, pool).await;

    let mut client = common::Client::new(common::HOST, port).await;

    actions::sequence_create(&mut client, "test_sequence", None).await;

    // let key = actions::topic_create(&mut client, &key, "test_sequence/my_topic", None).await;
    // assert!(valid_key(&key));
    //
    server.shutdown().await;
    Ok(())
}
