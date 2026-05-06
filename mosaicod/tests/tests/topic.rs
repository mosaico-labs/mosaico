#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

// ===========================================================================
// Topic tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid =
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
            .await
            .unwrap();
    assert!(topic_uuid.is_valid());

    // Passing a wrong session uuid should trigger a NotFound error.
    let err = actions::topic_create(
        &mut client,
        &topic_uuid, // wrong uuid
        "test_sequence/my_topic",
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Creating a topic with same name should trigger an ALreadyExists error.
    let err = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // Create topic with malformed metadata should give an InvalidArgument error.
    assert_eq!(
        actions::topic_create(
            &mut client,
            &session_uuid,
            "test_sequence/my_topic",
            Some("{")
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );

    // Trying to create a topic inside an already finalized session should return a FailedPrecondition error.
    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(
        &mut client,
        &topic_uuid,
        "test_sequence/my_topic",
        batches,
        false,
    )
    .await
    .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    assert_eq!(
        actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic2", None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_create_invalid_format(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let bad_names = [
        "no_slash",
        "wrong_sequence/topic",
        "test_sequence/",
        "/topic_only",
    ];

    for name in bad_names {
        let res = actions::topic_create(&mut client, &session_uuid, name, None).await;
        assert!(res.is_err(), "topic name {:?} should be rejected", name);
        let code = res.unwrap_err().code();
        assert!(
            matches!(
                code,
                tonic::Code::InvalidArgument
                    | tonic::Code::NotFound
                    | tonic::Code::PermissionDenied
            ),
            "name {:?} got unexpected code {:?}",
            name,
            code
        );
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    actions::topic_delete(&mut client, topic_name)
        .await
        .unwrap();

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_flight_info(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    // Check flight info for a locked topic without data.

    let topic_name = "test_sequence/my_empty_topic";

    let uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Metadata should be available even if topic is unlocked, but not all info are filled.
    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(!app_metadata.locked);
    assert_eq!(app_metadata.resource_locator, topic_name);
    assert!(app_metadata.info.is_none());
    assert_ne!(app_metadata.created_at_ns, 0);
    assert!(app_metadata.completed_at_ns.is_none());

    let batches = vec![ext::arrow::testing::dummy_empty_batch()];

    let response = actions::do_put(&mut client, &uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(app_metadata.locked);
    assert_ne!(app_metadata.created_at_ns, 0);
    assert_ne!(app_metadata.completed_at_ns.unwrap(), 0);
    assert_eq!(app_metadata.resource_locator, topic_name);

    let info = app_metadata.info.unwrap();
    assert_eq!(info.chunks_number, 0);
    assert_eq!(info.total_bytes, 0);
    assert!(info.timestamp.is_none());

    // Check flight info for a locked topic with data.

    let topic_name = "test_sequence/my_topic";

    let uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(!info.endpoint.first().unwrap().app_metadata.is_empty());

    let app_metadata: marshal::flight::TopicAppMetadata = info
        .endpoint
        .first()
        .unwrap()
        .clone()
        .app_metadata
        .try_into()
        .unwrap();

    assert!(app_metadata.locked);
    assert_ne!(app_metadata.created_at_ns, 0);
    assert_ne!(app_metadata.completed_at_ns.unwrap(), 0);
    assert_eq!(app_metadata.resource_locator, topic_name);

    let info = app_metadata.info.unwrap();
    assert_eq!(info.chunks_number, 1);
    assert_eq!(info.total_bytes, 895);
    let ts_range: types::TimestampRange = info.timestamp.unwrap().into();
    assert_eq!(ts_range.start.as_i64(), 10000);
    assert_eq!(ts_range.end.as_i64(), 10030);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_create(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let error_msg = format!("Error in {}", topic_name);
    actions::topic_notification_create(
        &mut client,
        topic_name,
        types::NotificationType::Error.to_string(),
        error_msg,
    )
    .await
    .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_list(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);
    let notification_type = types::NotificationType::Error.to_string();
    let notifications_size = 5;

    actions::setup_topic_with_notifications(
        &mut client,
        sequence_name,
        topic_name,
        notification_type.clone(),
        notifications_size,
    )
    .await
    .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), notifications_size);

    for (i, notification) in notifications.iter().enumerate() {
        let error_msg = format!("Error {}_{}", topic_name, i + 1);
        assert_eq!(notification["notification_type"], notification_type);
        assert_eq!(notification["name"].as_str().unwrap(), topic_name);
        assert_eq!(notification["msg"], error_msg);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    let sequence_name = "test_sequence_topic_notification_create";
    let topic_name = &format!("{}/my_topic", sequence_name);
    let notification_type = types::NotificationType::Error.to_string();
    let notifications_size = 5;

    actions::setup_topic_with_notifications(
        &mut client,
        sequence_name,
        topic_name,
        notification_type.clone(),
        notifications_size,
    )
    .await
    .unwrap();

    actions::topic_notification_purge(&mut client, topic_name)
        .await
        .unwrap();
    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_create_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::topic_notification_create(
        &mut client,
        "test_sequence/never_existed",
        types::NotificationType::Error.to_string(),
        "msg".to_string(),
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_notification_list_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let r = actions::topic_notification_list(&mut client, topic_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::topic_delete(&mut client, "test_sequence/never_existed").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::topic_delete(&mut client, "ghost_sequence/ghost_topic").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_delete_unlocked(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unlocked_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert!(res.is_ok());

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}
