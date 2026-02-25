#![allow(unused_crate_dependencies)]

use mosaicod_db as db;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls() // enable tls also in the client
        .build()
        .await;

    // make a dummy sequence create to see if the connection works
    actions::sequence_create(&mut client, "test_sequence", None).await;

    server.shutdown().await;
    Ok(())
}
