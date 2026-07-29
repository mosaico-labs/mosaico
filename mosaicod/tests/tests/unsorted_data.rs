#![allow(unused_crate_dependencies)]

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use mosaicod_core::params;
use mosaicod_db as db;
use rand::{random, random_range};
use std::sync::Arc;
use tests::{self, actions, common};

fn dummy_batch(batch_size: u32, sorted_ts: bool, min_ts: u32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
            DataType::Int64,
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));

    let ts_vec = if sorted_ts {
        (min_ts..min_ts + batch_size)
            .map(|x| x as i64)
            .collect::<Vec<_>>()
    } else {
        (0..batch_size)
            .map(|_| random_range(min_ts..min_ts + batch_size) as i64)
            .collect::<Vec<_>>()
    };
    let val_vec = (0..batch_size).map(|_| random::<i64>()).collect::<Vec<_>>();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ts_vec)),
            Arc::new(Int64Array::from(val_vec)),
        ],
    )
    .unwrap()
}

/// Checks that server doesn't crash when serving a do_get request for a topic with data not sorted by timestamp.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_with_unsorted_timestamp(pool: sqlx::Pool<db::DatabaseType>) {
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

    let unsorted_batch1 = dummy_batch(100000, false, 0);
    let unsorted_batch2 = dummy_batch(100000, false, 1000000);
    let unsorted_batch3 = dummy_batch(100000, false, 2000000);
    let batches = vec![
        unsorted_batch1.clone(),
        unsorted_batch2.clone(),
        unsorted_batch3.clone(),
    ];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name, None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let (metadata, received_batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    let metadata = metadata.unwrap();

    let json_metadata = metadata["mosaico:properties"]
        .parse::<serde_json::Value>()
        .unwrap();
    let json_metadata_obj = json_metadata.as_object().unwrap();
    assert_eq!(json_metadata_obj["message_count"].as_i64().unwrap(), 300000);

    // Timestamps are not sorted inside a single input batch. But between batches they don't overlap.
    // This make data-fusion believe that a sort is not necessary. That's why it is returning data as it
    // is, only splitting it into batches of 8192 rows (default batch size) and not merging rows from
    // different parquet files (that's why every 13 batches there's one with fewer rows).
    assert_eq!(received_batches.len(), 39);

    for i in 0..3 {
        for batch in received_batches.iter().take(12) {
            assert_eq!(batch.num_rows(), 8192);
            assert_eq!(batch.num_columns(), unsorted_batch1.num_columns());
        }

        assert_eq!(received_batches[12 + 13 * i].num_rows(), 1696);
        assert_eq!(
            received_batches[13 * i].num_columns(),
            unsorted_batch1.num_columns()
        );
    }

    server.shutdown().await;
}
