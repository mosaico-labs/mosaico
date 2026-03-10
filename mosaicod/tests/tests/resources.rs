#![allow(unused_crate_dependencies)]

use mosaicod_core as core;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    actions::sequence_create(&mut client, "test_sequence", None)
        .await
        .unwrap();

    // Check that sequences with same name are not allowed.
    assert_eq!(
        actions::sequence_create(&mut client, "test_sequence", None)
            .await
            .unwrap_err()
            .message(),
        "sequence `test_sequence` already exists"
    );

    // Check malformed metadata json.
    assert_eq!(
        actions::sequence_create(&mut client, "test_malformed_sequence", Some("{"))
            .await
            .unwrap_err()
            .message(),
        "action error"
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_flight_info(pool: sqlx::Pool<db::DatabaseType>) {
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
    let sequence_manifest: core::types::SequenceManifest = app_metadata.try_into().unwrap();

    assert!(sequence_manifest.sessions.is_empty());
    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_timestamp.as_i64(), 0);

    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());

    // Check the manifest for a sequence with a still running session and no topic yet injected.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_manifest: core::types::SequenceManifest = app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_timestamp.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].0, session_uuid);
    assert!(sequence_manifest.sessions[0].1.is_none());

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
    let sequence_manifest: core::types::SequenceManifest = app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_timestamp.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].0, session_uuid);
    assert!(sequence_manifest.sessions[0].1.is_none());

    actions::session_finalize(&mut client, &session_uuid).await;

    // Check the manifest for a sequence with a finalized session and a topic injected.
    let info = actions::get_flight_info(&mut client, sequence_name)
        .await
        .unwrap();

    let app_metadata: marshal::flight::SequenceAppMetadata = info.app_metadata.try_into().unwrap();
    let sequence_manifest: core::types::SequenceManifest = app_metadata.try_into().unwrap();

    assert_eq!(
        sequence_manifest.resource_locator.to_string(),
        sequence_name
    );
    assert_ne!(sequence_manifest.created_timestamp.as_i64(), 0);
    assert_eq!(sequence_manifest.sessions.len(), 1);
    assert_eq!(sequence_manifest.sessions[0].0, session_uuid);
    let sm = sequence_manifest.sessions[0].1.clone().unwrap();
    assert_eq!(sm.uuid, session_uuid);
    assert_ne!(sm.created_timestamp.as_i64(), 0);
    assert_ne!(sm.completed_timestamp.as_i64(), 0);
    assert_eq!(sm.topics.len(), 1);
    assert_eq!(sm.topics[0].clone().into_parts().0, topic_name);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn topic_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Create topic with malformed metadata.
    assert_eq!(
        actions::topic_create(&mut client, &uuid, "test_sequence/my_topic", Some("{"))
            .await
            .unwrap_err()
            .message(),
        "action error"
    );

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn topic_flight_info(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(uuid.is_valid());

    let topic_name = "test_sequence/my_topic";

    let uuid = actions::topic_create(&mut client, &uuid, topic_name, None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Metadata (topic manifest) shouldn't be available if topic is unlocked.
    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    assert!(info.endpoint.first().unwrap().app_metadata.is_empty());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
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

    let topic_manifest: core::types::TopicManifest = app_metadata.into();

    assert!(topic_manifest.info.is_locked);
    assert_eq!(topic_manifest.info.chunks_number, 1);
    assert_eq!(topic_manifest.info.total_size_bytes, 895);
    assert_ne!(topic_manifest.info.created_timestamp.as_i64(), 0);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn do_put(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let uuid = actions::session_create(&mut client, sequence_name).await;
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
            .message(),
        "missing descriptor in request"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
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

    actions::session_finalize(&mut client, &session_uuid).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_abort(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let session_uuid = actions::session_create(&mut client, sequence_name).await;
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

    actions::session_abort(&mut client, &session_uuid)
        .await
        .unwrap();

    // Abort on locked sessions must fail.
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    actions::session_finalize(&mut client, &session_uuid).await;
    assert_eq!(
        actions::session_abort(&mut client, &session_uuid)
            .await
            .unwrap_err()
            .message(),
        "facade error"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn session_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let session_uuid = actions::session_create(&mut client, sequence_name).await;
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
    actions::session_finalize(&mut client, &session_uuid).await;
    actions::session_delete(&mut client, &session_uuid).await;

    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    actions::session_delete(&mut client, &session_uuid).await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn sequence_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let session_uuid = actions::session_create(&mut client, sequence_name).await;
    assert!(session_uuid.is_valid());
    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid).await;

    actions::sequence_delete(&mut client, "test_sequence").await;

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn get_server_version(pool: sqlx::Pool<db::DatabaseType>) {
    let port = common::random_port();

    let server = common::ServerBuilder::new(common::HOST, port, pool)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    actions::server_version(&mut client).await;

    server.shutdown().await;
}
