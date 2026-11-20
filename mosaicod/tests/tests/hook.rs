#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_facade as facade;
use mosaicod_hook::testing::{Receiver, Request};
use serde_json::json;
use tests::{self, actions, common};

const SEQUENCE: &str = "test_sequence";
const TOPIC: &str = "test_sequence/front_camera";

async fn register_hook(server: &common::Server, url: &str) {
    facade::hook::create(&server.context(), url).await.unwrap();
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

    let receiver = Receiver::start().await;
    register_hook(&server, receiver.url()).await;

    actions::sequence_create(&mut client, SEQUENCE, "")
        .await
        .unwrap();
    let (session, session_uuid) = actions::session_create(&mut client, SEQUENCE)
        .await
        .unwrap();
    create_and_upload(&mut client, &session_uuid, r#"{"driver":"Ada"}"#).await;

    let requests = receiver.wait_for_requests(1).await;
    assert_eq!(requests.len(), 1);

    let event = requests[0].json();
    assert!(event["id"].as_str().unwrap().parse::<types::Uuid>().is_ok());
    assert_eq!(event["type"], "topic_uploaded");
    assert_eq!(
        event["data"],
        json!({
            "topic": TOPIC,
            "sequence": SEQUENCE,
            "session": session.to_string(),
            "ontology_tag": "mock",
            "user_metadata": { "driver": "Ada" },
            "created_at": event["data"]["created_at"],
        })
    );
    assert!(event["data"]["created_at"].as_i64().unwrap() > 0);

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

    let receiver = Receiver::start().await;
    register_hook(&server, receiver.url()).await;

    actions::sequence_create(&mut client, SEQUENCE, "")
        .await
        .unwrap();
    let (session, session_uuid) = actions::session_create(&mut client, SEQUENCE)
        .await
        .unwrap();
    create_and_upload(&mut client, &session_uuid, "").await;
    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    // The topic's `topic_uploaded`, and the `session_uploaded`, in either order: deliveries run
    // concurrently.
    let requests = receiver.wait_for_requests(2).await;
    assert_eq!(requests.len(), 2);

    let events: Vec<_> = requests.iter().map(Request::json).collect();
    let session_events: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "session_uploaded")
        .collect();
    assert_eq!(session_events.len(), 1, "{events:?}");

    let data = &session_events[0]["data"];
    assert_eq!(
        *data,
        json!({
            "session": session.to_string(),
            "sequence": SEQUENCE,
            "topics": [TOPIC],
            "created_at": data["created_at"],
        })
    );
    assert!(data["created_at"].as_i64().unwrap() > 0);

    server.shutdown().await;
    Ok(())
}
