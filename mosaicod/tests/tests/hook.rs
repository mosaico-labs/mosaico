#![allow(unused_crate_dependencies)]
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use httpmock::{HttpMockRequest, HttpMockResponse, Mock, MockServer};
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_facade as facade;
use mosaicod_hook as hook;
use serde_json::json;
use tests::{self, actions, common};
use tokio_util::sync::CancellationToken;

const SEQUENCE: &str = "test_sequence";
const TOPIC: &str = "test_sequence/front_camera";

const WAIT: Duration = Duration::from_secs(5);

async fn register_hook(server: &common::Server, url: &str) {
    facade::hook::create(&server.context(), url).await.unwrap();
}

/// Waits until `hook` got `n` requests, then fails the test unless it got exactly `n`.
async fn wait_for(hook: &Mock<'_>, n: usize) {
    let deadline = Instant::now() + WAIT;
    while hook.calls_async().await < n && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    hook.assert_calls_async(n).await;
}

/// Whether the event has a UUID for id and a positive creation time, which change on every run.
fn has_id_and_creation_time(request: &HttpMockRequest) -> bool {
    let event: serde_json::Value = serde_json::from_slice(request.body_ref()).unwrap_or_default();
    event["id"]
        .as_str()
        .is_some_and(|id| id.parse::<types::Uuid>().is_ok())
        && event["data"]["created_at"].as_i64().is_some_and(|t| t > 0)
}

/// Answers `200` to every request on `receiver`, collecting the ids of the events it gets.
async fn collect_ids(receiver: &MockServer) -> (Mock<'_>, Arc<Mutex<Vec<String>>>) {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let collected = ids.clone();
    let hook = receiver
        .mock_async(|when, then| {
            when.any_request();
            then.respond_with(move |request: &HttpMockRequest| {
                let event: serde_json::Value = serde_json::from_slice(request.body_ref()).unwrap();
                let id = event["id"].as_str().unwrap().to_owned();
                collected.lock().unwrap().push(id);
                HttpMockResponse::builder().status(200).build()
            });
        })
        .await;
    (hook, ids)
}

fn session_uploaded() -> hook::Event {
    types::HookEvent::SessionUploaded(types::SessionMetadata {
        locator: "drive_042:session".parse().unwrap(),
        topics: vec![],
        created_at: 0.into(),
        completed_at: None,
    })
}

/// Creates [`TOPIC`] in the session and uploads one batch to it.
async fn create_and_upload(
    client: &mut common::Client,
    session_uuid: &types::Uuid,
    json_metadata: &str,
) {
    let topic_uuid = actions::topic_create(client, session_uuid, TOPIC, json_metadata)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1)];
    let response = actions::do_put(client, &topic_uuid, TOPIC, batches, false)
        .await
        .unwrap();
    assert!(response.into_inner().message().await.unwrap().is_none());
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_topic_upload_sends_topic_uploaded(
    pool: sqlx::Pool<db::DatabaseType>,
) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let receiver = MockServer::start_async().await;
    register_hook(&server, &receiver.base_url()).await;

    actions::sequence_create(&mut client, SEQUENCE, "")
        .await
        .unwrap();
    let (session, session_uuid) = actions::session_create(&mut client, SEQUENCE)
        .await
        .unwrap();

    let hook = receiver
        .mock_async(|when, then| {
            when.json_body_includes(
                json!({
                    "type": "topic_uploaded",
                    "data": {
                        "topic": TOPIC,
                        "sequence": SEQUENCE,
                        "session": session.to_string(),
                        "ontology_tag": "mock",
                        "user_metadata": { "driver": "Ada" },
                    },
                })
                .to_string(),
            )
            .is_true(has_id_and_creation_time);
            then.status(200);
        })
        .await;

    create_and_upload(&mut client, &session_uuid, r#"{"driver":"Ada"}"#).await;

    wait_for(&hook, 1).await;

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_session_finalize_sends_session_uploaded(
    pool: sqlx::Pool<db::DatabaseType>,
) -> sqlx::Result<()> {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let receiver = MockServer::start_async().await;
    register_hook(&server, &receiver.base_url()).await;

    actions::sequence_create(&mut client, SEQUENCE, "")
        .await
        .unwrap();
    let (session, session_uuid) = actions::session_create(&mut client, SEQUENCE)
        .await
        .unwrap();

    let topic_event = receiver
        .mock_async(|when, then| {
            when.json_body_includes(json!({ "type": "topic_uploaded" }).to_string());
            then.status(200);
        })
        .await;
    let session_event = receiver
        .mock_async(|when, then| {
            when.json_body_includes(
                json!({
                    "type": "session_uploaded",
                    "data": {
                        "session": session.to_string(),
                        "sequence": SEQUENCE,
                        "topics": [TOPIC],
                    },
                })
                .to_string(),
            )
            .is_true(has_id_and_creation_time);
            then.status(200);
        })
        .await;

    create_and_upload(&mut client, &session_uuid, "").await;
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    wait_for(&topic_event, 1).await;
    wait_for(&session_event, 1).await;

    server.shutdown().await;
    Ok(())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_worker_sends_each_event_to_every_hook(
    pool: sqlx::Pool<db::DatabaseType>,
) -> sqlx::Result<()> {
    let database = db::testing::Database::new(pool);
    let first = MockServer::start_async().await;
    let second = MockServer::start_async().await;
    let (first_hook, first_ids) = collect_ids(&first).await;
    let (second_hook, second_ids) = collect_ids(&second).await;

    db::hook_create(&mut database.connection(), &first.base_url(), 0)
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let dispatcher = hook::Dispatcher::spawn((*database).clone(), shutdown.clone()).unwrap();

    // Registered after the worker started: hooks are read on every event.
    db::hook_create(&mut database.connection(), &second.base_url(), 0)
        .await
        .unwrap();

    dispatcher.emit(session_uploaded());
    dispatcher.emit(session_uploaded());

    wait_for(&first_hook, 2).await;
    wait_for(&second_hook, 2).await;

    // Each event has its own id, and both hooks got the same ones.
    let sorted = |ids: &Mutex<Vec<String>>| {
        let mut ids = ids.lock().unwrap().clone();
        ids.sort();
        ids
    };
    let first_ids = sorted(&first_ids);
    assert_ne!(first_ids[0], first_ids[1]);
    assert_eq!(first_ids, sorted(&second_ids));

    shutdown.cancel();

    Ok(())
}
