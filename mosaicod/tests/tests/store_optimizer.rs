#![allow(unused_crate_dependencies)]

use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_rw::ToProperties;
use mosaicod_store as store;
use tests::{self, actions, common};

// ===========================================================================
// Store optimization routine. Single server tests
// ===========================================================================

/// Tests the optimization in a scenario with 1 sequence, 1 topic and one small record batch.
/// The optimization should produce a single output file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_1(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(1);
    let max_file_size = 256 * 1024 * 1024;

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval, max_file_size)
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

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    // Trying to create a topic inside an already finalized session should return a FailedPrecondition error.
    let batches = vec![ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1)];

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 5);

    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 1 topic and many record batches.
/// The optimization should produce a single output file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_2(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(1);
    let max_file_size = 2_000;

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval, max_file_size)
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

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    let batches = vec![
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
    ];

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 7);

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 9);

    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 1 topic and many record batches with many rows.
/// The optimization should produce a single output file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_3(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(5);
    let max_file_size = 3_000_000;

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval, max_file_size)
        .build()
        .await;

    // WORKAROUND: wait for the optimizer to run the first time.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

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

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    let batches = vec![
        ext::arrow::testing::dummy_batch(10_000, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(1_000, 100000, 5, 10, 1000),
        ext::arrow::testing::dummy_batch(20_000, 200000, 5, 23723323, 898),
        ext::arrow::testing::dummy_batch(100_000, 400000, 5, 321, 999),
        ext::arrow::testing::dummy_batch(10, 1000000, 5, 1000, 1000),
        ext::arrow::testing::dummy_batch(1_000_000, 2000000, 5, 2, 30),
    ];

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 8);

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 11);

    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 2 topics and many record batches filled with random byte arrays.
/// The optimization should produce 2 output files per topic.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_4(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(5);
    let max_file_size = 50_000_000; // 50 MB

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval, max_file_size)
        .build()
        .await;

    // WORKAROUND: wait for the optimizer to run the first time.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

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

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    let batches = vec![
        ext::arrow::testing::dummy_binary_batch(1_000, 0, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 10_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 20_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 30_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 40_000, 5, 16_000, 16_000),
    ];

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    let topic_locator2 = "test_sequence/topic2"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator2.to_string(), None)
            .await
            .unwrap();

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator2.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    let topic_record2 = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator2)
        .await
        .unwrap();

    assert!(topic_record2.optimization_end_timestamp().is_none());

    assert_eq!(server.store.list("", None).await.unwrap().len(), 13);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 19);

    // Check first topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    // Check second topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator2)
            .await
            .unwrap();

    assert_ne!(
        topic_record2.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 1 topic and many record batches.
/// The optimization max file size is set to a value smaller than the minimum parquet file size (headers, row groups, etc...)
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_5(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(1);
    let max_file_size = 100;

    let server = common::ServerBuilder::new(common::HOST, pool)
        .with_cleanup(types::Duration::seconds(0), types::Duration::seconds(0))
        .with_store_optimizer(optimization_time_interval, max_file_size)
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

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    let batches = vec![
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
        ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1),
    ];

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 7);

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 10);

    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    server.shutdown().await;
}

// ===========================================================================
// Store optimizer routine multi-server tests
// ===========================================================================

/// Tests the optimization in a scenario with 1 sequence, 3 topics and many record batches filled with random byte arrays.
/// The optimization should produce 2 output files per topic.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_multi_1(pool: sqlx::Pool<db::DatabaseType>) {
    let optimization_time_interval = types::Duration::seconds(5);
    let max_file_size = 50_000_000; // 50 MB

    let store = store::testing::Store::new_random_on_tmp().unwrap();

    let server1 = common::ServerBuilder::new(common::HOST, pool.clone())
        .with_store_optimizer(optimization_time_interval, max_file_size)
        .build_with_store(store.clone())
        .await;

    let server2 = common::ServerBuilder::new(common::HOST, pool)
        .with_store_optimizer(optimization_time_interval, max_file_size)
        .build_with_store(store)
        .await;

    // WORKAROUND: wait for the optimizer to run the first time.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut client = common::ClientBuilder::new(common::HOST, server1.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let metadata = r#"{"meta": "test"}"#;

    actions::sequence_create(&mut client, sequence_name, Some(metadata))
        .await
        .unwrap();

    let batches = vec![
        ext::arrow::testing::dummy_binary_batch(1_000, 0, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 10_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 20_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 30_000, 5, 16_000, 16_000),
        ext::arrow::testing::dummy_binary_batch(1_000, 40_000, 5, 16_000, 16_000),
    ];

    let session = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_locator = "test_sequence/topic"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator.to_string(), None)
            .await
            .unwrap();

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator.to_string(),
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    let topic_locator2 = "test_sequence/topic2"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator2.to_string(), None)
            .await
            .unwrap();

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator2.to_string(),
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    let topic_record2 = db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator2)
        .await
        .unwrap();

    assert!(topic_record2.optimization_end_timestamp().is_none());

    let topic_locator3 = "test_sequence/topic3"
        .parse::<types::TopicLocator>()
        .unwrap();
    let topic_uuid =
        actions::topic_create(&mut client, &session.1, &topic_locator3.to_string(), None)
            .await
            .unwrap();

    actions::do_put(
        &mut client,
        &topic_uuid,
        &topic_locator3.to_string(),
        batches,
        false,
    )
    .await
    .unwrap();

    let topic_record3 = db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator3)
        .await
        .unwrap();

    assert!(topic_record3.optimization_end_timestamp().is_none());

    assert_eq!(server1.store.list("", None).await.unwrap().len(), 19);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server1.store.list("", None).await.unwrap().len(), 28);

    // Check first topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator)
            .await
            .unwrap();

    assert_ne!(
        topic_record.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server1.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server1.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server1
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server1
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    // Check second topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator2)
            .await
            .unwrap();

    assert_ne!(
        topic_record2.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server1.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server1.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server1
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server1
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    // Check third topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server1.db.connection(), &topic_locator2)
            .await
            .unwrap();

    assert_ne!(
        topic_record3.path_in_store().unwrap().to_string(),
        optimized_topic_record.path_in_store().unwrap().to_string()
    );

    assert!(
        optimized_topic_record
            .optimization_end_timestamp()
            .is_some()
    );

    assert!(
        db::topic_next_to_be_optimized(&mut server1.db.connection())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        db::topic_optimization_count(&mut server1.db.connection())
            .await
            .unwrap(),
        0
    );

    assert!(
        server1
            .store
            .exists(
                optimized_topic_record
                    .path_in_store()
                    .unwrap()
                    .path_metadata(),
            )
            .await
            .unwrap()
    );

    // Expecting 2 parquet files.
    for i in 0..2 {
        let chunk_metadata = server1
            .store
            .meta(
                optimized_topic_record.path_in_store().unwrap().path_data(
                    i,
                    optimized_topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(chunk_metadata.size > 0);
    }

    server1.shutdown().await;
    server2.shutdown().await;
}
