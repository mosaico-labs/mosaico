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

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_status(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls() // enable tls also in the client
        .build()
        .await;

    // Read status of a not-existing api key
    assert!(
        actions::api_key_status(&mut client, "wrong fingerprint")
            .await
            .is_err()
    );

    // Create an api key with lifetime duration and read its status.
    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permissions::READ,
        "api key description".to_string(),
        None,
    )
    .await
    .unwrap();

    let api_key_status = actions::api_key_status(&mut client, api_key_token.fingerprint())
        .await
        .unwrap();

    assert_eq!(api_key_status.0, api_key_token.fingerprint().to_string());
    assert_eq!(api_key_status.1, "api key description");
    assert_ne!(api_key_status.2, 0);
    assert!(api_key_status.3.is_none());

    // Create an api key with expiration time and read its status.
    let expiration_time = types::Timestamp::now() + std::time::Duration::from_hours(24);

    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permissions::READ | types::auth::Permissions::WRITE,
        "api key description".to_string(),
        Some(expiration_time),
    )
    .await
    .unwrap();

    let api_key_status = actions::api_key_status(&mut client, api_key_token.fingerprint())
        .await
        .unwrap();

    assert_eq!(api_key_status.0, api_key_token.fingerprint().to_string());
    assert_eq!(api_key_status.1, "api key description");
    assert_ne!(api_key_status.2, 0);
    assert_eq!(api_key_status.3.unwrap(), expiration_time.as_i64());

    server.shutdown().await;
}
