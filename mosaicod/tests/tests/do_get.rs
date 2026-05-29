#![allow(unused_crate_dependencies)]
use arrow_flight::Ticket;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

// ===========================================================================
// Do get tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_basic(pool: sqlx::Pool<db::DatabaseType>) {
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

    let original_batch = ext::arrow::testing::dummy_batch();
    let batches = vec![original_batch.clone()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let received_batches = actions::do_get_with_ticket(&mut client, ticket)
        .await
        .unwrap();
    assert_eq!(received_batches.len(), 1);
    assert_eq!(received_batches[0].num_rows(), original_batch.num_rows());
    assert_eq!(
        received_batches[0].num_columns(),
        original_batch.num_columns()
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_unlocked_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unlocked", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let _ = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let res = actions::do_get_with_ticket(&mut client, ticket).await;
    assert_eq!(
        res.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "reading from an unlocked topic should fail"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_nonexistent_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let fake_locator = "ghost_sequence/ghost_topic".parse().unwrap();
    let ticket_payload = types::flight::TicketTopic {
        locator: fake_locator,
        timestamp_range: None,
    };

    let fake_ticket = Ticket {
        ticket: marshal::flight::ticket_topic_to_binary(ticket_payload)
            .unwrap()
            .into(),
    };

    let res = actions::do_get_with_ticket(&mut client, fake_ticket).await;
    dbg!(&res);
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);
    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_do_get_empty_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/empty_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let _ = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    assert_eq!(
        actions::do_get_with_ticket(&mut client, ticket)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    server.shutdown().await;
}
