#![allow(unused_crate_dependencies)]

//! Schema related testing functions
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use mosaicod_core::params;
use mosaicod_db as db;
use std::collections::HashMap;
use std::sync::Arc;
use tests::{self, actions, common};

/// Regression test for a bug where client-supplied schema/field metadata (e.g. from a
/// pyarrow schema passed at `do_put` time) was lost by the time it came back out of
/// `do_get`: schema-level metadata was overwritten wholesale by
/// platform/ontology metadata, and field-level metadata was silently dropped by
/// DataFusion's schema inference.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_schema_metadata_round_trip(pool: sqlx::Pool<db::DatabaseType>) {
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

    // A schema carrying both schema-level and field-level metadata, mirroring what a
    // pyarrow client can attach via `Schema.with_metadata`/`Field.with_metadata`.
    let ts_field = Field::new(
        params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
        DataType::Int64,
        false,
    );
    let value_field = Field::new("value", DataType::Int64, false)
        .with_metadata(HashMap::from([("unit".to_owned(), "meters".to_owned())]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![ts_field, value_field],
        HashMap::from([("client_key".to_owned(), "client_value".to_owned())]),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10000, 10001])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    actions::do_put(&mut client, &topic_uuid, topic_name, vec![batch], false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name, None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let schema = actions::get_schema(&mut client, topic_name).await.unwrap();

    let received_batches = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    let doget_schema = received_batches[0].schema();

    // Field-level metadata from the client must survive.
    let value_field_out = doget_schema.field_with_name("value").unwrap();
    assert_eq!(
        value_field_out.metadata().get("unit"),
        Some(&"meters".to_owned())
    );

    // Schema-level metadata from the client must survive alongside platform metadata.
    assert_eq!(
        doget_schema.metadata().get("client_key"),
        Some(&"client_value".to_owned())
    );

    dbg!(&doget_schema);
    dbg!(&schema);

    // Check schema coherence
    assert_eq!(doget_schema.fields(), schema.fields());
}

/// `get_schema` only supports Topic locators; a Sequence locator must be rejected.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_schema_wrong_locator_kind(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let res = actions::get_schema(&mut client, sequence_name).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);
}

/// `get_schema` on a nonexistent topic must return NotFound.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_schema_nonexistent_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let res = actions::get_schema(&mut client, "ghost_sequence/ghost_topic").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);
}

/// `get_schema` on a topic which exists but has no data uploaded yet must succeed
/// and return an empty schema, instead of failing.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_schema_empty_topic(pool: sqlx::Pool<db::DatabaseType>) {
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
    actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let schema = actions::get_schema(&mut client, topic_name).await.unwrap();

    assert!(schema.fields().is_empty());
}
