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
        types::auth::Permission::Read,
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
        types::auth::Permission::Manage,
        "api key description".to_string(),
        Some(types::Timestamp::now() + std::time::Duration::new(1000, 0)),
    )
    .await
    .unwrap();

    assert!(!api_key_token.payload().is_empty());
    assert!(!api_key_token.fingerprint().is_empty());

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
        types::auth::Permission::Read,
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
        types::auth::Permission::Write,
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

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_revoke(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls() // enable tls in the server
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls() // enable tls also in the client
        .build()
        .await;

    // Revoking a non-existing api key should return an error.
    assert!(
        actions::api_key_revoke(&mut client, "wrong fingerprint")
            .await
            .is_err()
    );

    // Create an api key with lifetime duration and revoke it.
    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permission::Read,
        "api key description".to_string(),
        None,
    )
    .await
    .unwrap();

    assert!(!api_key_token.payload().is_empty());
    assert!(!api_key_token.fingerprint().is_empty());

    assert!(
        actions::api_key_revoke(&mut client, api_key_token.fingerprint())
            .await
            .is_ok()
    );

    assert!(
        actions::api_key_status(&mut client, api_key_token.fingerprint())
            .await
            .is_err()
    );

    // Create an api key with duration and revoke it.
    let api_key_token = actions::api_key_create(
        &mut client,
        types::auth::Permission::Manage,
        "api key description".to_string(),
        Some(types::Timestamp::now() + std::time::Duration::new(1000, 0)),
    )
    .await
    .unwrap();

    assert!(!api_key_token.payload().is_empty());
    assert!(!api_key_token.fingerprint().is_empty());

    assert!(
        actions::api_key_revoke(&mut client, api_key_token.fingerprint())
            .await
            .is_ok()
    );

    assert!(
        actions::api_key_status(&mut client, api_key_token.fingerprint())
            .await
            .is_err()
    );

    server.shutdown().await;
}

async fn make_client(key: &types::auth::Token, port: u16) -> common::Client {
    common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key(key.to_string())
        .build()
        .await
}

async fn invalid_revoke_helper(client: &mut common::Client, fingerprint: &str) {
    let res = actions::api_key_revoke(client, fingerprint).await;
    let err_code = res.unwrap_err().code();
    assert_eq!(err_code, tonic::Code::PermissionDenied);
}

async fn valid_delete_helper(client: &mut common::Client, sequence_name: &str) {
    let res = actions::sequence_create(client, sequence_name, None).await;
    assert!(res.is_ok());

    let res = actions::sequence_delete(client, sequence_name).await;
    dbg!(&res);
    assert!(res.is_ok());
}

async fn valid_write_helper(client: &mut common::Client, sequence_name: &str) {
    let res = actions::sequence_create(client, sequence_name, None).await;
    assert!(res.is_ok());
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_invalid_write(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key = server
        .create_api_key(types::auth::Permission::Read, None)
        .await;

    let mut client = make_client(&api_key.key, port).await;

    assert!(!api_key.permission.can_write());

    let sequence_name = "test_api_key_invalid_write";

    let res = actions::sequence_create(&mut client, sequence_name, None).await;
    let err_code = res.unwrap_err().code();
    assert_eq!(err_code, tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_valid_write(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_1 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_2 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_3 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_write = make_client(&api_key_1.key, port).await;
    let mut client_delete = make_client(&api_key_2.key, port).await;
    let mut client_manage = make_client(&api_key_3.key, port).await;

    let sequence_name_write = "test_api_key_valid_write_1";
    let sequence_name_delete = "test_api_key_valid_write_2";
    let sequence_name_manage = "test_api_key_valid_write_3";

    valid_write_helper(&mut client_write, sequence_name_write).await;
    valid_write_helper(&mut client_delete, sequence_name_delete).await;
    valid_write_helper(&mut client_manage, sequence_name_manage).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_valid_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_1 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_2 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_delete = make_client(&api_key_1.key, port).await;
    let mut client_manage = make_client(&api_key_2.key, port).await;

    let sequence_name_delete = "test_api_key_valid_delete_1";
    let sequence_name_manage = "test_api_key_valid_delete_2";

    valid_delete_helper(&mut client_delete, sequence_name_delete).await;
    valid_delete_helper(&mut client_manage, sequence_name_manage).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_invalid_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_1 = server
        .create_api_key(types::auth::Permission::Read, None)
        .await;
    let api_key_2 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;

    let mut client_read = make_client(&api_key_1.key, port).await;
    let mut client_write = make_client(&api_key_2.key, port).await;

    assert!(!api_key_1.permission.can_delete());
    assert!(!api_key_2.permission.can_delete());

    let sequence_name = "test_api_key_invalid_delete";

    let res = actions::sequence_create(&mut client_write, sequence_name, None).await;
    assert!(res.is_ok());

    let res = actions::sequence_delete(&mut client_write, sequence_name).await;
    let err_code = res.unwrap_err().code();
    assert_eq!(err_code, tonic::Code::PermissionDenied);

    let res = actions::sequence_delete(&mut client_read, sequence_name).await;
    let err_code = res.unwrap_err().code();
    assert_eq!(err_code, tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_valid_manage(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_1 = server
        .create_api_key(types::auth::Permission::Read, None)
        .await;
    let api_key_2 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_3 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_4 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port)
        .enable_tls()
        .with_api_key(api_key_4.key.to_string())
        .build()
        .await;

    assert!(api_key_4.permission.can_manage());

    let sequence_name = "test_api_key_valid_manage";

    let res = actions::sequence_create(&mut client, sequence_name, None).await;
    assert!(res.is_ok());

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert!(res.is_ok());

    let fingerprints = [
        api_key_1.key.fingerprint(),
        api_key_2.key.fingerprint(),
        api_key_3.key.fingerprint(),
        api_key_4.key.fingerprint(),
    ];

    for fingerprint in &fingerprints {
        let res = actions::api_key_revoke(&mut client, fingerprint).await;
        assert!(res.is_ok());
    }

    let res = actions::sequence_create(&mut client, sequence_name, None).await;
    dbg!(&res);
    let err_code = res.unwrap_err().code();
    assert_eq!(err_code, tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_invalid_manage(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_1 = server
        .create_api_key(types::auth::Permission::Read, None)
        .await;
    let api_key_2 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_3 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_4 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_read = make_client(&api_key_1.key, port).await;
    let mut client_write = make_client(&api_key_2.key, port).await;
    let mut client_delete = make_client(&api_key_3.key, port).await;
    let mut client_manage = make_client(&api_key_4.key, port).await;

    let fingerprints = [
        api_key_1.key.fingerprint(),
        api_key_2.key.fingerprint(),
        api_key_3.key.fingerprint(),
        api_key_4.key.fingerprint(),
    ];

    for fingerprint in &fingerprints {
        invalid_revoke_helper(&mut client_read, fingerprint).await;
        invalid_revoke_helper(&mut client_write, fingerprint).await;
        invalid_revoke_helper(&mut client_delete, fingerprint).await;
    }

    let res = actions::api_key_revoke(&mut client_manage, api_key_4.key.fingerprint()).await;
    assert!(res.is_ok());
    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_invalid_token(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

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
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let expiring_key = server
        .create_api_key(
            types::auth::Permission::Write,
            Some(types::Timestamp::now() + std::time::Duration::from_millis(200)),
        )
        .await;

    let manage_key = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_expiring = make_client(&expiring_key.key, port).await;
    let mut client_manage = make_client(&manage_key.key, port).await;

    let res = actions::sequence_create(&mut client_expiring, "test_before_expiry", None).await;
    assert!(res.is_ok());

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let res = actions::sequence_create(&mut client_expiring, "test_after_expiry", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    let res = actions::sequence_create(&mut client_manage, "test_manage", None).await;
    assert!(res.is_ok());

    let res = actions::api_key_revoke(&mut client_manage, expiring_key.key.fingerprint()).await;
    assert!(res.is_ok(),);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_lifecycle_cross_revoke(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_write = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_delete = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_manage1 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;
    let api_key_manage2 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_write = make_client(&api_key_write.key, port).await;
    let mut client_delete = make_client(&api_key_delete.key, port).await;
    let mut client_manage1 = make_client(&api_key_manage1.key, port).await;
    let mut client_manage2 = make_client(&api_key_manage2.key, port).await;

    // write, delete cancella, clean state
    actions::sequence_create(&mut client_write, "seq_a", None)
        .await
        .unwrap();
    actions::sequence_delete(&mut client_delete, "seq_a")
        .await
        .unwrap();

    // write seq_b, manage1 revokes write
    actions::sequence_create(&mut client_write, "seq_b", None)
        .await
        .unwrap();
    actions::api_key_revoke(&mut client_manage1, api_key_write.key.fingerprint())
        .await
        .unwrap();

    // invalid write
    let err = actions::sequence_create(&mut client_write, "seq_c", None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // seq_b is still available
    actions::sequence_delete(&mut client_delete, "seq_b")
        .await
        .unwrap();

    // revoke manager
    actions::api_key_revoke(&mut client_manage2, api_key_manage1.key.fingerprint())
        .await
        .unwrap();
    let err = actions::api_key_revoke(&mut client_manage1, api_key_delete.key.fingerprint())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // manage2 still alive
    actions::sequence_create(&mut client_manage2, "seq_final", None)
        .await
        .unwrap();
    actions::sequence_delete(&mut client_manage2, "seq_final")
        .await
        .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_manage_self_revoke(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let manage_key_1 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;
    let manage_key_2 = server
        .create_api_key(types::auth::Permission::Manage, None)
        .await;

    let mut client_1 = make_client(&manage_key_1.key, port).await;
    let mut client_2 = make_client(&manage_key_2.key, port).await;

    let res = actions::sequence_create(&mut client_1, "seq_pre_self_revoke", None).await;
    assert!(res.is_ok());

    let res = actions::api_key_revoke(&mut client_1, manage_key_1.key.fingerprint()).await;
    dbg!(&res);
    assert!(res.is_ok());

    let res = actions::sequence_create(&mut client_1, "seq_after_self_revoke", None).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    let res = actions::sequence_delete(&mut client_1, "seq_pre_self_revoke").await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    let res = actions::api_key_revoke(&mut client_1, manage_key_2.key.fingerprint()).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    let res = actions::sequence_delete(&mut client_2, "seq_pre_self_revoke").await;
    assert!(res.is_ok());

    let res = actions::api_key_revoke(&mut client_2, manage_key_1.key.fingerprint()).await;
    dbg!(&res);
    assert!(res.unwrap_err().code() == tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_api_key_concurrent_same_sequence(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let mut server = common::ServerBuilder::new(common::HOST, port, pool)
        .enable_tls()
        .enable_api_key()
        .build()
        .await;

    let api_key_w1 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_w2 = server
        .create_api_key(types::auth::Permission::Write, None)
        .await;
    let api_key_d1 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;
    let api_key_d2 = server
        .create_api_key(types::auth::Permission::Delete, None)
        .await;

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
