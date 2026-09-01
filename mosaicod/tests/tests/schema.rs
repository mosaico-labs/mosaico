#![allow(unused_crate_dependencies)]

//! Schema related testing functions
use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

/// This test will check that the schema returned by the standalone `get_schema` matches the
/// one carried by `do_get` for the same topic.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_schema_coherence(pool: sqlx::Pool<db::DatabaseType>) {
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

    let original_batch = ext::arrow::testing::dummy_list_string_batch();
    let batches = vec![original_batch.clone()];
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

    let schema = actions::get_schema(&mut client, topic_name).await.unwrap();

    let (_, batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    // We should have at least a batch
    assert!(!batches.is_empty());

    let do_get_schema = (*batches[0].schema()).clone();

    dbg!(&do_get_schema);
    dbg!(&schema);

    // Check schema coherence
    assert_eq!(do_get_schema.fields(), schema.fields());
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
