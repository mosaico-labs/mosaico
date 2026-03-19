#![allow(unused_crate_dependencies)]

use mosaicod_core::types;
use mosaicod_db as db;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_create(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls() // enable tls also in the client
        .build()
        .await;

    // Create an api key with lifetime duration.
    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permissions::READ,
        "api key description".to_string(),
        None,
    )
    .await
    .unwrap();

    assert!(!api_key_token.payload().is_empty());
    assert!(!api_key_token.fingerprint().is_empty());

    // Create an api key with duration.
    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permissions::READ
            | types::auth::Permissions::MANAGE
            | types::auth::Permissions::WRITE
            | types::auth::Permissions::DELETE,
        "api key description".to_string(),
        Some(types::Timestamp::now() + std::time::Duration::new(1000, 0)),
    )
    .await
    .unwrap();

    assert!(!api_key_token.payload().is_empty());
    assert!(!api_key_token.fingerprint().is_empty());

    // Create an api key with empty permissions.
    assert!(
        actions::api_key_create(
            &mut client,
            types::auth::Permissions::default(),
            "api key description".to_string(),
            Some(types::Timestamp::now() + std::time::Duration::new(1000, 0)),
        )
        .await
        .is_err()
    );

    server.shutdown().await;
}
