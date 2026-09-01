#![allow(unused_crate_dependencies)]

// use arrow::util::pretty::print_batches;
use arrow::{array::RecordBatch, compute::BatchCoalescer};
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_rw::ToProperties;
use mosaicod_store as store;
use tests::{self, actions, cleanup, common, store_optimizer};

/// Does not take into account metadata.s
fn record_batches_equal(b1: &RecordBatch, b2: &RecordBatch) -> bool {
    // print_batches(&[b1.clone()]).unwrap();
    // print_batches(&[b2.clone()]).unwrap();

    b1.schema().fields == b2.schema().fields
        && b1.num_rows() == b2.num_rows()
        && b1.num_columns() == b2.num_columns()
        && b1
            .columns()
            .iter()
            .zip(b2.columns())
            .all(|(c1, c2)| c1 == c2)
}

// ===========================================================================
// Store optimization routine. Single server tests
// ===========================================================================

/// Tests the optimization in a scenario with 1 sequence, 1 topic and one small record batch.
/// The optimization should produce a single output file. After, cleanup should remove old data.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_1(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool.clone())
        .build()
        .await;

    let cleanup_handle = cleanup::Builder::new(pool.clone())
        .with_time_interval(types::Duration::seconds(2))
        .with_retention_period(types::Duration::seconds(0))
        .build_with_store(&server.store)
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
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    assert!(topic_record.optimization_end_timestamp().is_none());

    assert_eq!(server.store.list("", None).await.unwrap().len(), 3);

    // Run the store optimizer one-shot.
    let max_file_size = 256 * 1024 * 1024;
    let store_optimizer_handle = store_optimizer::Builder::new(pool)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    // Wait for the optimizer to run.
    store_optimizer_handle.wait_until_finished().await;

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

    // Old topic folder is still present until cleanup runs.
    assert!(
        server
            .store
            .exists(topic_record.path_in_store().unwrap().path_metadata())
            .await
            .unwrap()
    );

    assert!(
        server
            .store
            .exists(
                topic_record.path_in_store().unwrap().path_data(
                    0,
                    topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref()
                )
            )
            .await
            .unwrap()
    );

    // Wait for the cleanup to run.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    assert!(
        !server
            .store
            .exists(topic_record.path_in_store().unwrap().path_metadata())
            .await
            .unwrap()
    );

    assert!(
        !server
            .store
            .exists(
                topic_record.path_in_store().unwrap().path_data(
                    0,
                    topic_record
                        .serialization_format()
                        .unwrap()
                        .to_properties()
                        .as_ref()
                )
            )
            .await
            .unwrap()
    );

    // Retrieve back data with do_get to check it remained unchanged.
    let info = actions::get_flight_info(&mut client, &topic_locator.to_string(), None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let (_, received_batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    assert_eq!(received_batches.len(), 1);
    assert!(record_batches_equal(&received_batches[0], &batches[0]));

    server.shutdown().await;
    cleanup_handle.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 1 topic and many record batches.
/// The optimization should produce a single output file.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_2(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool.clone())
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
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 7);

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    // Run the store optimizer one-shot.
    let max_file_size = 2_000;
    let store_optimizer_handle = store_optimizer::Builder::new(pool)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    // Wait for the optimizer to run.
    store_optimizer_handle.wait_until_finished().await;

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

    // Retrieve back data with do_get.
    let info = actions::get_flight_info(&mut client, &topic_locator.to_string(), None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let (_, received_batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    assert_eq!(received_batches.len(), 1);

    let recv_batch = &received_batches[0];

    assert_eq!(recv_batch.num_rows(), 35);
    assert_eq!(recv_batch.num_columns(), 2);

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 1 topic and many record batches with many rows.
/// The optimization should produce 2 output files due to 3MB file limit.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_3(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool.clone())
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
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    assert_eq!(server.store.list("", None).await.unwrap().len(), 8);

    let topic_record = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator)
        .await
        .unwrap();

    let max_file_size = 3_000_000;
    let store_optimizer_handle = store_optimizer::Builder::new(pool)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    // Wait for the optimizer to run.
    store_optimizer_handle.wait_until_finished().await;

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

    // Retrieve back data with do_get to check data order remained unchanged.
    let info = actions::get_flight_info(&mut client, &topic_locator.to_string(), None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let (_, received_batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    let target_rows = batches.iter().map(|x| x.num_rows()).sum();

    let mut bc = BatchCoalescer::new(batches[0].schema(), target_rows);

    batches
        .into_iter()
        .for_each(|batch| bc.push_batch(batch).unwrap());

    let merged_batch = bc.next_completed_batch().unwrap();

    // Consider that once sent record batches have a default limit of 8192 rows.
    assert_eq!(received_batches.len(), 139);

    let mut offset = 0;
    received_batches.iter().for_each(|batch| {
        let slice = merged_batch.slice(offset, batch.num_rows());
        assert!(record_batches_equal(&slice, batch));
        offset += batch.num_rows();
    });

    server.shutdown().await;
}

/// Tests the optimization in a scenario with 1 sequence, 2 topics and many record batches filled with random byte arrays.
/// The optimization should produce 2 output files per topic.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_store_optimization_4(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool.clone())
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

    let max_file_size = 50_000_000; // 50 MB
    let store_optimizer_handle = store_optimizer::Builder::new(pool)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    // Wait for the optimizer to run.
    store_optimizer_handle.wait_until_finished().await;

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
    let server = common::ServerBuilder::new(common::HOST, pool.clone())
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

    let max_file_size = 100;
    let store_optimizer_handle = store_optimizer::Builder::new(pool)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    // Wait for the optimizer to run.
    store_optimizer_handle.wait_until_finished().await;

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
    let optimization_time_interval = types::Duration::seconds(10);
    let max_file_size = 50_000_000; // 50 MB

    let store = store::testing::Store::new_random_on_tmp().unwrap();

    let server = common::ServerBuilder::new(common::HOST, pool.clone())
        .build_with_store(store.clone())
        .await;

    let store_optimizer_handle1 = store_optimizer::Builder::new(pool.clone())
        .with_time_interval(optimization_time_interval)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    let store_optimizer_handle2 = store_optimizer::Builder::new(pool)
        .with_time_interval(optimization_time_interval)
        .with_max_file_size(max_file_size)
        .build_with_store(&server.store)
        .await;

    //Wait for the optimizers to run the first time.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
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
        batches.clone(),
        false,
    )
    .await
    .unwrap();

    let topic_record2 = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator2)
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

    let topic_record3 = db::topic_find_by_locator(&mut server.db.connection(), &topic_locator3)
        .await
        .unwrap();

    assert!(topic_record3.optimization_end_timestamp().is_none());

    assert_eq!(server.store.list("", None).await.unwrap().len(), 19);

    // Wait for the optimizer to run.
    tokio::time::sleep(std::time::Duration::from_secs(17)).await;

    // The optimizer writes the merged output alongside the original data. It does not yet
    // clean up the pre-optimization files.
    assert_eq!(server.store.list("", None).await.unwrap().len(), 28);

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

    // Check third topic.
    let optimized_topic_record =
        db::topic_find_by_locator(&mut server.db.connection(), &topic_locator2)
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
    store_optimizer_handle1.shutdown().await;
    store_optimizer_handle2.shutdown().await;
}
