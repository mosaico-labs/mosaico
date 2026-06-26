#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

// ===========================================================================
// Sequence tests
// ===========================================================================
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert!(res.is_ok());

    let res = actions::sequence_delete(&mut client, sequence_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_delete_cascades(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let r = actions::sequence_notification_list(&mut client, "ghost_sequence")
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(notifications.len(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_notification_invalid_type(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let res = actions::sequence_create(&mut client, "", None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_sequence_create_invalid_chars(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

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
