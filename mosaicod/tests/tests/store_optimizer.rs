#![allow(unused_crate_dependencies)]

use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

// ===========================================================================
// Store optimization routine. Single server tests
// ===========================================================================

/// Tests the optimization in a scenario with 1 sequence and one small record batch.
/// The optimization should produce in output a single file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_1(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(1);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval)
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

    let session = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_name = "test_sequence/topic";
    let topic_uuid = actions::topic_create(&mut client, &session.1, topic_name, None)
        .await
        .unwrap();

    // Trying to create a topic inside an already finalized session should return a FailedPrecondition error.
    let batches = vec![ext::arrow::testing::dummy_batch()];

    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files, so the store grows by one object.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 4);

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence and many record batches.
/// The optimization should produce a single output file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_2(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(1);

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval)
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

    let session = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_name = "test_sequence/topic";
    let topic_uuid = actions::topic_create(&mut client, &session.1, topic_name, None)
        .await
        .unwrap();

    // Trying to create a topic inside an already finalized session should return a FailedPrecondition error.
    let batches = vec![
        ext::arrow::testing::dummy_batch(),
        ext::arrow::testing::dummy_batch(),
        ext::arrow::testing::dummy_batch(),
        ext::arrow::testing::dummy_batch(),
        ext::arrow::testing::dummy_batch(),
    ];

    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 7);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files, so the store grows by one object.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 8);

    server.shutdown().await;
}
