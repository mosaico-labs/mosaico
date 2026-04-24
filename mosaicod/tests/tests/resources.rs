#![allow(unused_crate_dependencies)]

use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

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

    let session_uuid = actions::session_create(&mut client, sequence_name)
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
    assert_eq!(sequence_manifest.sessions[0].uuid, session_uuid);
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
    assert_eq!(sequence_manifest.sessions[0].uuid, session_uuid);
    assert_ne!(sequence_manifest.sessions[0].created_at.as_i64(), 0);
    assert!(sequence_manifest.sessions[0].completed_at.is_none());
    assert_eq!(sequence_manifest.sessions[0].topics.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].topics[0], topic_name);

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
    assert_eq!(sm.uuid, session_uuid);
    assert_ne!(sm.created_at.as_i64(), 0);
    assert_ne!(sm.completed_at.unwrap().as_i64(), 0);
    assert_eq!(sm.topics.len(), 1);
    assert_eq!(sm.topics[0].clone(), topic_name);

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
    let uuid = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

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

    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    let uuid = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None)
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

    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    // Finalize on an empty session should fail.
    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let session_uuid = actions::session_create(&mut client, sequence_name)
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

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    // Delete must work on both unlocked and locked sessions.
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_uuid)
        .await
        .unwrap();

    let session_uuid = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_uuid)
        .await
        .unwrap();

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

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let session_uuid = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    actions::sequence_delete(&mut client, "test_sequence")
        .await
        .unwrap();

    // Make sure that delete command did not actually remove any file from Store.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    server.shutdown().await;
}

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
    let session_uuid = actions::session_create(&mut client, sequence_name)
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
