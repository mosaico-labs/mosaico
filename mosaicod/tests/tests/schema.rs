#![allow(unused_crate_dependencies)]

///! Schema related testing functions
use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

/// This test will check that the schema provided by the get_flight_info and
/// do_get is the same.
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
    let info_schema = info.try_decode_schema().unwrap();

    let (_, batches) = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();

    // We should have at least a batch
    assert!(batches.len() > 0);

    let do_get_schema = (*batches[0].schema()).clone();

    dbg!(&do_get_schema);
    dbg!(&info_schema);

    // Check schema coherence
    assert!(do_get_schema.fields() == info_schema.fields());
}
