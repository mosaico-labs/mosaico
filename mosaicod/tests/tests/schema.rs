#![allow(unused_crate_dependencies)]

//! Schema related testing functions
use arrow::array::{ArrayRef, Int64Array, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use mosaicod_core::constants;
use mosaicod_db as db;
use mosaicod_ext::schema_hash::SchemaHash;
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

    actions::sequence_create(&mut client, sequence_name, "")
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, "")
        .await
        .unwrap();

    // A schema carrying both schema-level and field-level metadata, mirroring what a
    // pyarrow client can attach via `Schema.with_metadata`/`Field.with_metadata`.
    let ts_field = Field::new(
        constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
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

/// A schema read back with `get_schema` hashes like the uploaded one, although its layouts change
/// on the way: arrow-flight's encoder sends a `LargeList` as a `List`, and DataFusion reads
/// top-level strings and binaries back as views.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_schema_hash_round_trip(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, "")
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, "")
        .await
        .unwrap();

    let list = |data_type| DataType::List(Arc::new(Field::new_list_field(data_type, true)));
    let large_list =
        |data_type| DataType::LargeList(Arc::new(Field::new_list_field(data_type, true)));

    // Each field with the type `get_schema` returns it as.
    let layouts = [
        ("utf8", DataType::Utf8, DataType::Utf8View),
        ("large_utf8", DataType::LargeUtf8, DataType::Utf8View),
        ("binary", DataType::Binary, DataType::BinaryView),
        ("large_binary", DataType::LargeBinary, DataType::BinaryView),
        ("list", list(DataType::Float32), list(DataType::Float32)),
        (
            "large_list",
            large_list(DataType::Float32),
            list(DataType::Float32),
        ),
        ("utf8_list", list(DataType::Utf8), list(DataType::Utf8)),
    ];

    let metadata = |key: &str, value: &str| HashMap::from([(key.to_owned(), value.to_owned())]);
    let x = Field::new("x", DataType::Float64, true).with_metadata(metadata("description", "x"));
    let fields = [
        Field::new("position", DataType::Struct(vec![x].into()), true),
        Field::new(
            "stamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]
    .into_iter()
    .chain(
        layouts
            .iter()
            .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true)),
    );
    let ts_field = Field::new(
        constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
        DataType::Int64,
        false,
    );
    let schema = Arc::new(Schema::new_with_metadata(
        std::iter::once(ts_field).chain(fields).collect::<Vec<_>>(),
        metadata("key", "value"),
    ));

    let columns = std::iter::once(Arc::new(Int64Array::from(vec![10000])) as ArrayRef)
        .chain(
            schema
                .fields()
                .iter()
                .skip(1)
                .map(|field| new_null_array(field.data_type(), 1)),
        )
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    actions::do_put(&mut client, &topic_uuid, topic_name, vec![batch], false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let read_back = actions::get_schema(&mut client, topic_name).await.unwrap();

    for (name, _, read_back_type) in &layouts {
        let field = read_back.field_with_name(name).unwrap();
        assert_eq!(field.data_type(), read_back_type, "{name}");
    }
    assert_eq!(SchemaHash::new(&read_back), SchemaHash::new(&schema));
}

/// `get_schema` only supports Topic locators; a Sequence locator must be rejected.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_get_schema_wrong_locator_kind(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, "")
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

    actions::sequence_create(&mut client, sequence_name, "")
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    actions::topic_create(&mut client, &session_uuid, topic_name, "")
        .await
        .unwrap();

    let schema = actions::get_schema(&mut client, topic_name).await.unwrap();

    assert!(schema.fields().is_empty());
}
