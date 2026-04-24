#![allow(unused_crate_dependencies)]

use mosaicod_db as db;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_tls_with_valid_cert(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let res = server.is_shutdown().await;
    assert!(!res);

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls() // enable tls also in the client
        .build()
        .await;

    // make a dummy sequence create to see if the connection works
    actions::sequence_create(&mut client, "test_sequence", None)
        .await
        .unwrap();

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_tls_server_with_invalid_cert(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls_with("./data/wrong_cert.pem", "./data/wrong_key.pem")
        .build()
        .await;

    let res = server.is_shutdown().await;
    assert!(res);
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_tls_server_with_no_cert(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls_with("", "")
        .build()
        .await;

    let res = server.is_shutdown().await;
    assert!(res);
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_tls_client_with_no_cert(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .build()
        .await;

    let res = common::ClientBuilder::new(common::HOST, port).enable_tls_with("");

    assert!(res.is_err());
    server.shutdown().await;
    Ok(())
}

#[should_panic]
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_tls_client_with_invalid_cert(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let res = server.is_shutdown().await;
    assert!(!res);

    let _client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls_with("./data/cert.pem")
        .unwrap()
        .build()
        .await;

    server.shutdown().await;
}
