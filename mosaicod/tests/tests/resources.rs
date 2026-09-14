#![allow(unused_crate_dependencies)]
use mosaicod_db as db;
use tests::{self, actions, common};

// ===========================================================================
// Get server info tests
// ===========================================================================
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_server_info(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    actions::server_info(&mut client).await.unwrap();

    server.shutdown().await;
}
