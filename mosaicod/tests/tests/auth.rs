#![allow(unused_crate_dependencies)]

use mosaicod_core::types::{self, auth::Permission};
use mosaicod_db as db;
use serde_json::json;
use tests::{self, actions, common};

async fn make_client(key: &types::auth::Token, port: u16) -> common::Client {
    common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key(key.to_string())
        .build()
        .await
}

/// A read action (query) that is rejected before execution when the key lacks
/// read permission. Returns the resulting status code (if any).
async fn try_read(client: &mut common::Client) -> Option<tonic::Code> {
    actions::query(client, json!({}))
        .await
        .err()
        .map(|e| e.code())
}

/// A read-only key can read but neither write nor delete.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_read_only(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key = server.create_api_key(Permission::Read.into(), None).await;

    assert!(api_key.permission.can_read());
    assert!(!api_key.permission.can_write());
    assert!(!api_key.permission.can_delete());

    let mut client = make_client(&api_key.key, server.port()).await;

    // Read is allowed: the request must not be rejected by the permission gate.
    assert_ne!(
        try_read(&mut client).await,
        Some(tonic::Code::PermissionDenied)
    );

    // Write is denied.
    let res = actions::sequence_create(&mut client, "read_only_seq", None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    // Delete is denied.
    let res = actions::sequence_delete(&mut client, "read_only_seq").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

/// A write-only key can write but neither read nor delete
/// (no implicit read is inherited any more).
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_write_only(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key = server.create_api_key(Permission::Write.into(), None).await;

    assert!(!api_key.permission.can_read());
    assert!(api_key.permission.can_write());
    assert!(!api_key.permission.can_delete());

    let mut client = make_client(&api_key.key, server.port()).await;

    // Write is allowed.
    let res = actions::sequence_create(&mut client, "write_only_seq", None).await;
    assert!(res.is_ok());

    // Read is denied.
    assert_eq!(
        try_read(&mut client).await,
        Some(tonic::Code::PermissionDenied)
    );

    // Delete is denied.
    let res = actions::sequence_delete(&mut client, "write_only_seq").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

/// A delete-only key can delete but neither read nor write
/// (a delete key can no longer create data).
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_delete_only(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let writer = server.create_api_key(Permission::Write.into(), None).await;
    let deleter = server.create_api_key(Permission::Delete.into(), None).await;

    assert!(!deleter.permission.can_read());
    assert!(!deleter.permission.can_write());
    assert!(deleter.permission.can_delete());

    let port = server.port();
    let mut client_writer = make_client(&writer.key, port).await;
    let mut client_deleter = make_client(&deleter.key, port).await;

    let seq_name = "delete_only_seq";

    // The delete-only key cannot create the sequence.
    let res = actions::sequence_create(&mut client_deleter, seq_name, None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    // A write key sets up the sequence, then the delete key removes it.
    actions::sequence_create(&mut client_writer, seq_name, None)
        .await
        .unwrap();

    let res = actions::sequence_delete(&mut client_deleter, seq_name).await;
    assert!(res.is_ok());

    // Read is denied for the delete-only key.
    assert_eq!(
        try_read(&mut client_deleter).await,
        Some(tonic::Code::PermissionDenied)
    );

    server.shutdown().await;
}

/// Permissions can be combined explicitly. A `write|delete` key can create and
/// delete, and a `read|write|delete` key can do everything.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_combined_permissions(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let write_delete = server
        .create_api_key("write|delete".parse().unwrap(), None)
        .await;
    let full = server
        .create_api_key("read|write|delete".parse().unwrap(), None)
        .await;

    assert!(!write_delete.permission.can_read());
    assert!(write_delete.permission.can_write());
    assert!(write_delete.permission.can_delete());

    assert!(full.permission.can_read());
    assert!(full.permission.can_write());
    assert!(full.permission.can_delete());

    let port = server.port();
    let mut client_wd = make_client(&write_delete.key, port).await;
    let mut client_full = make_client(&full.key, port).await;

    // write|delete: can create and delete, cannot read.
    actions::sequence_create(&mut client_wd, "wd_seq", None)
        .await
        .unwrap();
    assert_eq!(
        try_read(&mut client_wd).await,
        Some(tonic::Code::PermissionDenied)
    );
    actions::sequence_delete(&mut client_wd, "wd_seq")
        .await
        .unwrap();

    // read|write|delete: full access.
    actions::sequence_create(&mut client_full, "full_seq", None)
        .await
        .unwrap();
    assert_ne!(
        try_read(&mut client_full).await,
        Some(tonic::Code::PermissionDenied)
    );
    actions::sequence_delete(&mut client_full, "full_seq")
        .await
        .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_invalid_token(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let port = server.port();
    let mut client_1 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key("invalid_key".to_string())
        .build()
        .await;

    let mut client_2 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key("".to_string())
        .build()
        .await;

    let mut client_3 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key("xyzw_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".to_string())
        .build()
        .await;

    let mut client_4 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key("msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_00000000".to_string())
        .build()
        .await;

    let mut client_5 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key("msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530_extra".to_string())
        .build()
        .await;

    let fake_key = types::auth::Token::new().to_string();
    let mut client_6 = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key(fake_key)
        .build()
        .await;

    let res = actions::sequence_create(&mut client_1, "test_1", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    let res = actions::sequence_create(&mut client_2, "test_2", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    let res = actions::sequence_create(&mut client_3, "test_3", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    let res = actions::sequence_create(&mut client_4, "test_4", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    let res = actions::sequence_create(&mut client_5, "test_5", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    let res = actions::sequence_create(&mut client_6, "test_6", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_expiration(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let expiring_key = server
        .create_api_key(
            Permission::Write.into(),
            Some(types::Timestamp::now() + std::time::Duration::from_millis(200)),
        )
        .await;

    let stable_key = server.create_api_key(Permission::Write.into(), None).await;

    let port = server.port();
    let mut client_expiring = make_client(&expiring_key.key, port).await;
    let mut client_stable = make_client(&stable_key.key, port).await;

    let res = actions::sequence_create(&mut client_expiring, "test_before_expiry", None).await;
    assert!(res.is_ok());

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let res = actions::sequence_create(&mut client_expiring, "test_after_expiry", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    // A non-expiring key keeps working.
    let res = actions::sequence_create(&mut client_stable, "test_stable", None).await;
    assert!(res.is_ok());

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_concurrent_same_sequence(pool: sqlx::Pool<db::DatabaseType>) {
    let mut server = common::ServerBuilder::new(common::HOST, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_w1 = server.create_api_key(Permission::Write.into(), None).await;
    let api_key_w2 = server.create_api_key(Permission::Write.into(), None).await;
    let api_key_d1 = server.create_api_key(Permission::Delete.into(), None).await;
    let api_key_d2 = server.create_api_key(Permission::Delete.into(), None).await;

    let port = server.port();
    let mut client_w1 = make_client(&api_key_w1.key, port).await;
    let mut client_w2 = make_client(&api_key_w2.key, port).await;
    let mut client_d1 = make_client(&api_key_d1.key, port).await;
    let mut client_d2 = make_client(&api_key_d2.key, port).await;

    let seq_name = "test_concurrent_create";
    let (r1, r2) = tokio::join!(
        actions::sequence_create(&mut client_w1, seq_name, None),
        actions::sequence_create(&mut client_w2, seq_name, None),
    );
    dbg!(&r1, &r2);

    let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
    assert_eq!(oks, 1);

    let err = if r1.is_err() {
        r1.unwrap_err()
    } else {
        r2.unwrap_err()
    };
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    let (r1, r2) = tokio::join!(
        actions::sequence_delete(&mut client_d1, seq_name),
        actions::sequence_delete(&mut client_d2, seq_name),
    );
    dbg!(&r1, &r2);

    let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
    assert_eq!(oks, 1);

    let err = if r1.is_err() {
        r1.unwrap_err()
    } else {
        r2.unwrap_err()
    };
    assert_eq!(err.code(), tonic::Code::NotFound);

    let res = actions::sequence_create(&mut client_w1, seq_name, None).await;
    assert!(res.is_ok());

    server.shutdown().await;
}
