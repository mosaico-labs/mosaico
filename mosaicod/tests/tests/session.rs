#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use tests::{self, actions, common};

// ===========================================================================
// Session tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_create(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let uuid = actions::topic_create(&mut client, &session_uuid, "test_sequence/my_topic", None)
        .await
        .unwrap();
    assert!(uuid.is_valid());

    // Calling finalize with unlocked topics should fail.
    assert_eq!(
        actions::session_finalize(&mut client, &session_uuid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &uuid, "test_sequence/my_topic", batches, false)
        .await
        .unwrap();

    let mut response_reader = response.into_inner();
    if response_reader.message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let ctx = server.context();
    let mut cx = ctx.db.connection();
    let db_session = db::session_find_by_locator(&mut cx, &session_locator)
        .await
        .unwrap();
    assert!(db_session.completion_timestamp().unwrap().as_i64() > 0);

    // Finalize on an empty session should fail.
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());
    assert_eq!(
        actions::session_finalize(&mut client, &session_uuid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    assert!(topic_uuid.is_valid());

    let batches = vec![ext::arrow::testing::dummy_batch()];

    let response = actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    if response.into_inner().message().await.unwrap().is_some() {
        panic!("Received a not-empty response!");
    }

    // Delete must work on both open and finalized sessions.
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    //// Fake session locator and UUID test
    let fake_session_locator = types::SessionLocator::new(sequence_name.parse().unwrap());
    let res = actions::session_delete(&mut client, &fake_session_locator).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let fake_session_uuid = types::Uuid::new();
    let res = actions::topic_create(
        &mut client,
        &fake_session_uuid,
        "test_sequence/topic2",
        None,
    )
    .await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_idempotent(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    // NotFound
    let res = actions::session_delete(&mut client, &session_locator).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_unlocked_with_data(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/unfinalized_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let res = actions::get_flight_info(&mut client, topic_name, None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_cascades_to_topics(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    let topic_name_a = &format!("{}/topic_a", sequence_name);
    let topic_name_b = &format!("{}/topic_b", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_uuid_a = actions::topic_create(&mut client, &session_uuid, topic_name_a, None)
        .await
        .unwrap();
    let topic_uuid_b = actions::topic_create(&mut client, &session_uuid, topic_name_b, None)
        .await
        .unwrap();

    let batches_a = vec![ext::arrow::testing::dummy_batch()];
    let batches_b = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid_a, topic_name_a, batches_a, false)
        .await
        .unwrap();
    actions::do_put(&mut client, &topic_uuid_b, topic_name_b, batches_b, false)
        .await
        .unwrap();

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let res = actions::get_flight_info(&mut client, topic_name_a, None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::get_flight_info(&mut client, topic_name_b, None).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    let res = actions::topic_delete(&mut client, topic_name_a).await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_delete_preserves_sequence(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let (session_locator, _) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    actions::session_delete(&mut client, &session_locator)
        .await
        .unwrap();

    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    assert!(session_uuid.is_valid());

    let info = actions::get_flight_info(&mut client, sequence_name, None).await;
    assert!(info.is_ok());

    server.shutdown().await;
}
