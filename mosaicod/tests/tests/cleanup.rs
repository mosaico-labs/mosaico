#![allow(unused_crate_dependencies)]

use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_store as store;
use mosaicod_task::Duration;
use tests::{self, actions, common};

// ===========================================================================
// Cleanup routine single server tests
// ===========================================================================

/// Tests the cleanup in a scenario with 1 sequence. The sequence is deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_1(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and add the TO_DELETE marker file in the sequence folder.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 2);

    // Wait for the cleanup to run again and remove the folder from the store.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert!(server.store.list("", None).await.unwrap().is_empty());

    server.shutdown().await;
}

/// Tests the cleanup in a scenario with 1 sequence and 1 topic. Only the topic is deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_2(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_name = "test_sequence/test_topic";

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    // A topic folder in the store is created when the do_put is called.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::topic_delete(&mut client, topic_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and add the TO_DELETE marker file in the topic folder.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 4);

    // Wait for the cleanup to run again and remove the folder from the store (the sequence folder must be left untouched).
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    server.shutdown().await;
}

/// Tests the cleanup in a scenario with 1 sequence and 1 topic. The sequence is deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_3(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_name = "test_sequence/test_topic";

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    // A topic folder in the store is created when the do_put is called.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    // Deleting the sequence must delete all its topics as well.
    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and add the TO_DELETE marker file in the topic folder.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 5);

    // Wait for the cleanup to run again and remove the sequence and topic folders from the store.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert!(server.store.list("", None).await.unwrap().is_empty());

    server.shutdown().await;
}

/// Tests the cleanup in a scenario with 2 sequences and 2 topics, one for each sequence.
/// One sequence and one topic are deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_4(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence1_name = "test_sequence1";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence1_name, Some(metadata))
        .await
        .unwrap();

    let sequence2_name = "test_sequence2";

    actions::sequence_create(&mut client, sequence2_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 2);

    let (_, session1_uuid) = actions::session_create(&mut client, sequence1_name)
        .await
        .unwrap();

    let (_, session2_uuid) = actions::session_create(&mut client, sequence2_name)
        .await
        .unwrap();

    let topic1_name = "test_sequence1/test_topic1";

    let topic1_uuid = actions::topic_create(&mut client, &session1_uuid, topic1_name, None)
        .await
        .unwrap();

    let topic2_name = "test_sequence2/test_topic2";

    let topic2_uuid = actions::topic_create(&mut client, &session2_uuid, topic2_name, None)
        .await
        .unwrap();

    // A topic folder in the store is created when the do_put is called.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 2);

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic1_uuid, topic1_name, batches, false)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic2_uuid, topic2_name, batches, false)
        .await
        .unwrap();

    // In the store we should have 1 file for each sequence + 2 files for each topic.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 6);

    actions::sequence_delete(&mut client, sequence1_name)
        .await
        .unwrap();

    actions::topic_delete(&mut client, topic2_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and add the TO_DELETE marker file for 1 sequence and the 2 topics.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 9);

    // Wait for the cleanup to run again and remove the sequence and topic folders from the store.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    server.shutdown().await;
}

/// Tests the cleanup in a scenario with 1 sequence and retention duration = 0. The sequence is deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_5(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(2);
    let cleanup_retention_duration = Duration::seconds(0);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence_6";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and delete the sequence folder.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let res = server.store.list("", None).await.unwrap();
    dbg!(res);
    assert!(server.store.list("", None).await.unwrap().is_empty());

    server.shutdown().await;
}

/// Tests that re-creating a sequence with the same name as a deleted one works correctly:
/// the old folder must be cleaned up while the new one stays intact.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_6(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build()
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait for cleanup to mark (but not yet delete) the old sequence folder.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server.store.list("", None).await.unwrap().len(), 2);

    // Re-create a sequence with the same name. It lives in a different folder on the store
    // (path_in_store is generated fresh on creation).
    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    // Old folder (metadata + TO_DELETE marker) + new folder (metadata) = 3 files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    // Wait for cleanup to physically remove the old folder.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Only the new sequence's metadata remains.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 1);

    // The new sequence is still fully usable.
    let (_, _session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    server.shutdown().await;
}

/// Tests that a TO_DELETE marker created during a previous server lifecycle is honored
/// after a restart: the new server must complete the deletion once retention has elapsed.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_7(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    // Same store across both server lifecycles.
    let store = store::testing::Store::new_random_on_tmp().unwrap();

    // server marks the orphan folder, then shuts down before deletion
    let server1 = common::ServerBuilder::new(common::HOST, pool.clone())
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build_with_store(store.clone())
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server1.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait long enough for the marker to be created but NOT for the physical delete.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server1.store.list("", None).await.unwrap().len(), 2);

    server1.shutdown().await;

    // fresh server on the same store must complete the deletion
    let server2 = common::ServerBuilder::new(common::HOST, pool.clone())
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build_with_store(store.clone())
        .await;

    // The marker is already past retention; the very next cleanup cycle must wipe the folder.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    assert!(server2.store.list("", None).await.unwrap().is_empty());

    server2.shutdown().await;
}

// ===========================================================================
// Cleanup routine multi-server tests
// ===========================================================================

/// Tests the cleanup in a scenario with 2 active servers and 1 sequence. The sequence is deleted.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_cleanup_multi_1(pool: sqlx::Pool<db::DatabaseType>) {
    let cleanup_time_interval = Duration::seconds(1);
    let cleanup_retention_duration = Duration::seconds(3);

    let store = store::testing::Store::new_random_on_tmp().unwrap();

    let server1 = common::ServerBuilder::new(common::HOST, pool.clone())
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build_with_store(store.clone())
        .await;
    let server2 = common::ServerBuilder::new(common::HOST, pool.clone())
        .with_cleanup(cleanup_time_interval, cleanup_retention_duration)
        .build_with_store(store.clone())
        .await;

    let mut client = common::ClientBuilder::new(common::HOST, server1.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    assert_eq!(server1.store.list("", None).await.unwrap().len(), 1);
    assert_eq!(server2.store.list("", None).await.unwrap().len(), 1);

    actions::sequence_delete(&mut client, sequence_name)
        .await
        .unwrap();

    // Wait for the cleanup to run and add the TO_DELETE marker file in the sequence folder.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(server1.store.list("", None).await.unwrap().len(), 2);
    assert_eq!(server2.store.list("", None).await.unwrap().len(), 2);

    // Wait for the cleanup to run again and remove the sequence folder from the store.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert!(server1.store.list("", None).await.unwrap().is_empty());
    assert!(server2.store.list("", None).await.unwrap().is_empty());

    // Only one server at a time can perform a cleanup. We expect to see logs in the history spaced one second apart.
    let history = db::cleanup_log_history(&mut server1.db.connection(), u16::MAX)
        .await
        .unwrap();

    assert!(
        history
            .iter()
            .zip(history.iter().skip(1))
            .all(|(lhs, rhs)| {
                lhs.start_datetime() >= rhs.start_datetime() + chrono::Duration::seconds(1) // cleanup time interval.
            })
    );

    assert!(
        history
            .iter()
            .skip(1)
            .all(|log| log.end_datetime().is_some())
    );

    server1.shutdown().await;
    server2.shutdown().await;
}
