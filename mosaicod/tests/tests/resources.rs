#![allow(unused_crate_dependencies)]
use arrow_flight::Ticket;
use mosaicod_core::types::{self, Uuid};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

// ===========================================================================
// Sequence tests
// ===========================================================================
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    actions::sequence_create(&mut client, "test_sequence", None)
        .await
        .unwrap();

    // Check that sequences with same name are not allowed.
    assert!(
        actions::sequence_create(&mut client, "test_sequence", None)
            .await
            .is_err()
    );

    // Check malformed metadata json.
    assert_eq!(
        actions::sequence_create(&mut client, "test_malformed_sequence", Some("{"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_flight_info(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    // The manifest for a sequence without sessions should be empty.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_metadata: types::SequenceMetadata<marshal::JsonMetadataBlob> =
        app_metadata.try_into().unwrap();

    assert!(sequence_metadata.sessions.is_empty());
    assert_eq!(
        sequence_metadata.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_metadata.created_at.as_i64(), 0);

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    // Check the manifest for a sequence with a still running session and no topic yet injected.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_manifest: types::SequenceMetadata<marshal::JsonMetadataBlob> =
        app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_at.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].locator, session_locator);
    assert_ne!(sequence_manifest.sessions[0].created_at.as_i64(), 0);
    assert!(sequence_manifest.sessions[0].completed_at.is_none());
    assert!(sequence_manifest.sessions[0].topics.is_empty());

    let topic_name = "test_sequence/my_topic";

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

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

    // Check the manifest for a sequence with a still running session and a topic injected.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_manifest: types::SequenceMetadata<marshal::JsonMetadataBlob> =
        app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_at.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].locator, session_locator);
    assert_ne!(sequence_manifest.sessions[0].created_at.as_i64(), 0);
    assert!(sequence_manifest.sessions[0].completed_at.is_none());
    assert_eq!(sequence_manifest.sessions[0].topics.len(), 1);
    assert_eq!(
        sequence_manifest.sessions[0].topics[0].to_string(),
        topic_name
    );

    let _ = actions::session_finalize(&mut client, &session_uuid).await;

    // Check the manifest for a sequence with a finalized session and a topic injected.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_manifest: types::SequenceMetadata<marshal::JsonMetadataBlob> =
        app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_at.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    let sm = &sequence_manifest.sessions[0];
    assert_eq!(sm.locator, session_locator);
    assert_ne!(sm.created_at.as_i64(), 0);
    assert_ne!(sm.completed_at.unwrap().as_i64(), 0);
    assert_eq!(sm.topics.len(), 1);
    assert_eq!(sm.topics[0].to_string(), topic_name);

    assert_eq!(info.endpoint.len(), 1);
    let ep_metadata: marshal::flight::TopicAppMetadata =
        info.endpoint[0].clone().app_metadata.try_into().unwrap();
    assert!(ep_metadata.locked);
    assert_ne!(ep_metadata.created_at_ns, 0);
    assert_ne!(ep_metadata.completed_at_ns.unwrap(), 0);
    assert_eq!(ep_metadata.resource_locator, topic_name);

    let ep_metadata_info = ep_metadata.info.unwrap();
    assert_eq!(ep_metadata_info.chunks_number, 1);
    assert_eq!(ep_metadata_info.total_bytes, 895);
    let ts_range: types::TimestampRange = ep_metadata_info.timestamp.unwrap().into();
    assert_eq!(ts_range.start.as_i64(), 10000);
    assert_eq!(ts_range.end.as_i64(), 10030);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());
    assert_eq!(session_locator.sequence, sequence_name);
    assert_eq!(session_locator.to_string().split(':').count(), 2);
    let session_locator_str = session_locator.to_string();
    let mut split = session_locator_str.split(':');
    split.next();
    assert!(split.next().unwrap().parse::<ulid::Ulid>().is_ok());

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

    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Make sure that delete command did not actually remove any file from Store.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_create(pool: sqlx::Pool<db::DatabaseType>) {
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence_notification_create";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    actions::sequence_notification_create(
        &mut client,
        sequence_name,
        types::NotificationType::Error.to_string(),
        "Error test_sequence_notification_create".to_string(),
    )
    .await
    .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_list(pool: sqlx::Pool<db::DatabaseType>) {
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence_notification_list";
    let notifications_size = 5;
    let notification_type = types::NotificationType::Error.to_string();
    actions::setup_sequence_with_notifications(
        &mut client,
        sequence_name,
        notification_type.clone(),
        notifications_size,
    )
    .await
    .unwrap();

    let r = actions::sequence_notification_list(&mut client, sequence_name)
        .await
        .unwrap();

    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), notifications_size);

    for (i, notification) in notifications.iter().enumerate() {
        let error_msg = format!("Error {}_{}", sequence_name, i + 1);
        assert_eq!(notification["notification_type"], notification_type);
        assert_eq!(notification["name"], sequence_name);
        assert_eq!(notification["msg"], error_msg);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence_notification_purge";
    let notification_type = types::NotificationType::Error.to_string();

    let notifications_size = 10;
    actions::setup_sequence_with_notifications(
        &mut client,
        sequence_name,
        notification_type,
        notifications_size,
    )
    .await
    .unwrap();

    actions::sequence_notification_purge(&mut client, sequence_name)
        .await
        .unwrap();

    let r = actions::sequence_notification_list(&mut client, sequence_name)
        .await
        .unwrap();

    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_delete_with_active_session(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert!(res.is_ok());

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_delete_cascades(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
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

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    let res = actions::get_flight_info(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::topic_delete(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::session_delete(&mut client, &session_locator).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_create_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let res = actions::sequence_notification_create(
        &mut client,
        "ghost_sequence",
        types::NotificationType::Error.to_string(),
        "msg".to_string(),
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_list_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let r = actions::sequence_notification_list(&mut client, sequence_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_list_nonexistent(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let res = actions::sequence_notification_list(&mut client, "ghost_sequence").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_invalid_type(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::sequence_notification_create(
        &mut client,
        sequence_name,
        "this_is_not_a_valid_type".to_string(),
        "msg".to_string(),
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_purge_empty(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    actions::sequence_notification_purge(&mut client, sequence_name)
        .await
        .unwrap();

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create_empty_name(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let res = actions::sequence_create(&mut client, "", None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create_invalid_chars(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let bad_names = [
        "with space",
        "with/slash",
        "with\nnewline",
        "with\ttab",
        "with\"quote",
        "with!bang",
        "with'apostrophe",
        "with*asterisk",
        "with£pound",
        "with$dollar",
        "with%percent",
        "with&amp",
        "with.dot",
        "with_emoji_🚀",
        "caffè",
        "モザイク",
    ];

    for name in bad_names {
        let res = actions::sequence_create(&mut client, name, None).await;
        assert_eq!(
            res.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "name {:?} should be rejected",
            name
        );
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create_very_long_name(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let long_name = "a".repeat(10_000);
    let res = actions::sequence_create(&mut client, &long_name, None).await;
    if let Err(status) = res {
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "long name should give InvalidArgument or be accepted, not {:?}",
            status.code()
        );
    }

    server.shutdown().await;
}

// ===========================================================================
// Topic tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;
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
    let port: u16 = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;
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
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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

// ===========================================================================
// Session tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Calling finalize with unlocked topics should fail.
    assert_eq!(
        actions::session_finalize(&mut client, &session_uuid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    let mut response_reader = response.into_inner();
    if response_reader.message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let ctx = server.context();
    let mut cx = ctx.db.connection();
    let db_session = db::session_find_by_locator(&mut cx, &session_locator)
        .await
        .unwrap();
    assert!(db_session.completion_timestamp().unwrap().as_i64() > 0);

    // Finalize on an empty session should fail.
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());
    assert_eq!(
        actions::session_finalize(&mut client, &session_uuid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    // Delete must work on both open and finalized sessions.
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    //// Fake session locator and UUID test
    let fake_session_locator = types::SessionLocator::new(sequence_name.parse().unwrap());
    let res = actions::session_delete(&mut client, &fake_session_locator).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let fake_session_uuid = types::Uuid::new();
    let res = actions::topic_create(
        &mut client,
        &fake_session_uuid,
        "test_sequence/topic2",
        None,
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_idempotent(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    // NotFound
    let res = actions::session_delete(&mut client, &session_locator).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_unlocked_with_data(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unfinalized_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let res = actions::get_flight_info(&mut client, topic_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_cascades_to_topics(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name_a = &format!("{}/topic_a", sequence_name);
    let topic_name_b = &format!("{}/topic_b", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_uuid_a = actions::topic_create(&mut client, &session_uuid, topic_name_a, None)
        .await
        .unwrap();
    let topic_uuid_b = actions::topic_create(&mut client, &session_uuid, topic_name_b, None)
        .await
        .unwrap();

    let batches_a = vec![ext::arrow::testing::dummy_batch()];
    let batches_b = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid_a, topic_name_a, batches_a, false)
        .await
        .unwrap();
    actions::do_put(&mut client, &topic_uuid_b, topic_name_b, batches_b, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let res = actions::get_flight_info(&mut client, topic_name_a).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::get_flight_info(&mut client, topic_name_b).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::topic_delete(&mut client, topic_name_a).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_preserves_sequence(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let info = actions::get_flight_info(&mut client, sequence_name).await;
    assert!(info.is_ok());

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_basic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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

    let original_batch = ext::arrow::testing::dummy_batch();
    let batches = vec![original_batch.clone()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let received_batches = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();
    assert_eq!(received_batches.len(), 1);
    assert_eq!(received_batches[0].num_rows(), original_batch.num_rows());
    assert_eq!(
        received_batches[0].num_columns(),
        original_batch.num_columns()
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_unlocked_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unlocked", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let _ = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let res = actions::do_get_with_ticket(&mut client, ticket).await;
    assert_eq!(
        res.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "reading from an unlocked topic should fail"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_nonexistent_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let fake_locator = "ghost_sequence/ghost_topic".parse().unwrap();
    let ticket_payload = types::flight::TicketTopic {
        locator: fake_locator,
        timestamp_range: None,
    };

    let fake_ticket = Ticket {
        ticket: marshal::flight::ticket_topic_to_binary(ticket_payload)
            .unwrap()
            .into(),
    };

    let res = actions::do_get_with_ticket(&mut client, fake_ticket).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);
    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_empty_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/empty_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let _ = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    assert_eq!(
        actions::do_get_with_ticket(&mut client, ticket)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    server.shutdown().await;
}

// ===========================================================================
// Do put
// ===========================================================================
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    let mut response_reader = response.into_inner();
    if response_reader.message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    // Check do_put() without descriptor.
    let batches = vec![ext::arrow::testing::dummy_batch()];
    assert_eq!(
        actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, true)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_nonexistent_topic_uuid(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let fake_uuid = Uuid::new();
    let batches = vec![ext::arrow::testing::dummy_batch()];

    let res = actions::do_put(
        &mut client,
        &fake_uuid,
        "test_sequence/ghost",
        batches,
        false,
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_on_locked_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/locked", sequence_name);

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

    let batches = vec![ext::arrow::testing::dummy_batch()];
    let res = actions::do_put(&mut client, &topic_uuid, topic_name, batches, false).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::FailedPrecondition);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_descriptor_mismatch(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_a = &format!("{}/topic_a", sequence_name);
    let topic_b = &format!("{}/topic_b", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let uuid_a = actions::topic_create(&mut client, &session_uuid, topic_a, None)
        .await
        .unwrap();
    let _uuid_b = actions::topic_create(&mut client, &session_uuid, topic_b, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    let res = actions::do_put(&mut client, &uuid_a, topic_b, batches, false).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_put_empty_batches(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/no_batches", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let res = actions::do_put(&mut client, &topic_uuid, topic_name, vec![], false).await;
    assert!(res.is_err(), "do_put with no batches should error");

    server.shutdown().await;
}

// ===========================================================================
// Get server version  tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_server_version(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    actions::server_version(&mut client).await.unwrap();

    server.shutdown().await;
}

// ===========================================================================
// Concurrent tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_sequence_create(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let sequence_name = "concurrent_seq";

    let mut client1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut client2 = common::ClientBuilder::new(common::HOST, port).build().await;

    let h1 = tokio::spawn(async move {
        actions::sequence_create(&mut client1, "concurrent_seq", None).await
    });
    let h2 = tokio::spawn(async move {
        actions::sequence_create(&mut client2, "concurrent_seq", None).await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let (success_count, already_exists_count) =
        [&r1, &r2]
            .iter()
            .fold((0usize, 0usize), |(succ, ae), r| match r {
                Ok(_) => (succ + 1, ae),
                Err(e) if e.code() == tonic::Code::AlreadyExists => (succ, ae + 1),
                Err(e) => panic!("unexpected error: {:?}", e),
            });

    assert_eq!(success_count, 1);
    assert_eq!(already_exists_count, 1);

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;
    let info = actions::get_flight_info(&mut client, sequence_name).await;
    assert!(info.is_ok());

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

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

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;
    let s1 = session_uuid.clone();
    let s2 = session_uuid.clone();

    let h1 = tokio::spawn(async move { actions::session_finalize(&mut c1, &s1).await });
    let h2 = tokio::spawn(async move { actions::session_finalize(&mut c2, &s2).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let success_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(success_count, 1);

    for r in [&r1, &r2] {
        if let Err(e) = r {
            assert!(
                matches!(e.code(), tonic::Code::FailedPrecondition),
                "unexpected error code: {:?}",
                e.code()
            );
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_do_put_same_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/concurrent_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;

    let t1 = topic_uuid.clone();
    let t2 = topic_uuid.clone();
    let n1 = topic_name.clone();
    let n2 = topic_name.clone();

    let h1 = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut c1, &t1, &n1, batches, false).await
    });
    let h2 = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut c2, &t2, &n2, batches, false).await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let success_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert!(success_count >= 1, "at least one writer must succeed");

    for r in [&r1, &r2] {
        if let Err(e) = r {
            assert!(
                matches!(
                    e.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::Aborted
                ),
                "unexpected error code: {:?}",
                e.code()
            );
        }
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let app_metadata: marshal::flight::TopicAppMetadata =
        info.endpoint[0].clone().app_metadata.try_into().unwrap();
    let chunks = app_metadata.info.unwrap().chunks_number;

    assert_eq!(chunks as usize, success_count);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_topic_create_during_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let existing_topic = &format!("{}/existing", sequence_name);
    let new_topic = &format!("{}/new", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, existing_topic, None)
        .await
        .unwrap();
    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, existing_topic, batches, false)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;
    let s1 = session_uuid.clone();
    let s2 = session_uuid.clone();
    let new_topic_owned = new_topic.clone();

    let h_finalize = tokio::spawn(async move { actions::session_finalize(&mut c1, &s1).await });
    let h_create =
        tokio::spawn(
            async move { actions::topic_create(&mut c2, &s2, &new_topic_owned, None).await },
        );

    let r_fin = h_finalize.await.unwrap();
    let r_create = h_create.await.unwrap();

    // Expected: one of two coherent outcomes
    //  A) finalize wins -> create gets FailedPrecondition
    //  B) create wins -> finalize succeeds afterwards
    match (r_fin.is_ok(), r_create.is_ok()) {
        (true, false) => {
            assert_eq!(
                r_create.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
        (true, true) => {}
        (false, true) => {
            assert_eq!(r_fin.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
        (false, false) => {
            panic!("not possible");
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_read_during_write(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/read_during_write", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let mut writer = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut reader = common::ClientBuilder::new(common::HOST, port).build().await;

    let t = topic_uuid.clone();
    let n = topic_name.clone();
    let writer_task = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut writer, &t, &n, batches, false).await
    });

    let n_read = topic_name.clone();
    let reader_task = tokio::spawn(async move {
        let mut results = Vec::new();
        for _ in 0..10 {
            let info = actions::get_flight_info(&mut reader, &n_read).await;
            results.push(info);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        results
    });

    let _ = writer_task.await.unwrap().unwrap();
    let read_results = reader_task.await.unwrap();

    for info in read_results {
        let info = info.unwrap();
        let app_metadata: marshal::flight::TopicAppMetadata =
            info.endpoint[0].clone().app_metadata.try_into().unwrap();

        if app_metadata.locked {
            assert!(
                app_metadata.completed_at_ns.is_some(),
                "locked topic must have completed_at_ns"
            );
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_notification_create(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let n_notifications = 20;
    let mut handles = Vec::with_capacity(n_notifications);

    for i in 0..n_notifications {
        let mut c = common::ClientBuilder::new(common::HOST, port).build().await;
        let name = sequence_name.to_string();
        handles.push(tokio::spawn(async move {
            actions::sequence_notification_create(
                &mut c,
                &name,
                types::NotificationType::Error.to_string(),
                format!("concurrent msg {}", i),
            )
            .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    let r = actions::sequence_notification_list(&mut client, sequence_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(
        notifications.len(),
        n_notifications,
        "no notifications must be lost"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_sequence_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;
    let mut setup = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "race_seq";
    actions::sequence_create(&mut setup, sequence_name, None)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;

    // c1 deletes, c2 tries to create a session in it.
    let h_del = tokio::spawn(async move { actions::sequence_delete(&mut c1, "race_seq").await });
    let h_session = tokio::spawn(async move { actions::session_create(&mut c2, "race_seq").await });

    let r_del = h_del.await.unwrap();
    let r_session = h_session.await.unwrap();

    r_del.unwrap();

    if let Err(e) = r_session {
        assert_eq!(e.code(), tonic::Code::NotFound);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_stress_many_sequences_in_parallel(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();
    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let n_sequences = 50;
    let mut handles = Vec::with_capacity(n_sequences);

    for i in 0..n_sequences {
        let mut c = common::ClientBuilder::new(common::HOST, port).build().await;
        handles.push(tokio::spawn(async move {
            let name = format!("stress_seq_{}", i);
            actions::sequence_create(&mut c, &name, None).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    server.shutdown().await;
}
