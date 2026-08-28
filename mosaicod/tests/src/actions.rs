use super::common::{ActionResponse, Client};
use arrow::array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, FlightDescriptor, FlightInfo, PutResult};
use futures::StreamExt;
use futures::TryStreamExt;
use mosaicod_core::types;
use mosaicod_ext as ext;
use std::collections::HashMap;

use arrow_flight::Ticket;
use mosaicod_marshal::flight::FilterTimestampRange;
use mosaicod_marshal::{self as marshal, Ontology};

use serde_json::json;
use tonic::Streaming;

/// Create a new sequence.
/// Returns the `key` of the newly created sequence, this key is required to perform action
/// like create/upload topics, etc.
pub async fn sequence_create(
    client: &mut Client,
    sequence_name: &str,
    json_metadata: Option<&str>,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "sequence_create".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{}",
            "user_metadata": {}
        }}
        "#,
            sequence_name,
            json_metadata.unwrap_or("{}"),
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_create");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }

    Ok(())
}

pub async fn sequence_delete(client: &mut Client, locator: &str) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "sequence_delete".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{}"
        }}
        "#,
            locator,
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_delete");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }

    Ok(())
}

pub async fn session_create(
    client: &mut Client,
    sequence_name: &str,
) -> Result<(types::SessionLocator, types::Uuid), tonic::Status> {
    let action = Action {
        r#type: "session_create".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{}"
        }}
        "#,
            sequence_name
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    let mut key: Option<(types::SessionLocator, types::Uuid)> = None;

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_create");

        let locator = r.response["locator"]
            .as_str()
            .ok_or_else(|| tonic::Status::internal("locator is not a string"))?
            .parse::<types::SessionLocator>()
            .map_err(|e| {
                tonic::Status::internal(format!("Failed to parse session locator: {e}"))
            })?;

        let uuid = r.response["uuid"]
            .as_str()
            .ok_or_else(|| tonic::Status::internal("uuid is not a string"))?
            .parse::<types::Uuid>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse session uuid: {e}")))?;

        key = Some((locator, uuid));
    }

    key.ok_or_else(|| tonic::Status::internal("Unable to return key"))
}

pub async fn session_finalize(
    client: &mut Client,
    session_uuid: &types::Uuid,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "session_finalize".to_owned(),
        body: format!(
            r#"
        {{
            "session_uuid": "{}"
        }}
        "#,
            session_uuid
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_finalize");

        assert!(r.response.as_object().is_none());
    }

    Ok(())
}

/// Send an action to delete the current session
pub async fn session_delete(
    client: &mut Client,
    session_locator: &types::SessionLocator,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "session_delete".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{}"
        }}
        "#,
            session_locator
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_delete");
        assert!(r.response.as_object().is_none());
    }

    Ok(())
}

/// Create a new topic.
/// Returns the `key` of the newly created topic, this key is required to upload topic data.
pub async fn topic_create(
    client: &mut Client,
    key: &types::Uuid,
    topic_name: &str,
    json_metadata: Option<&str>,
) -> Result<types::Uuid, tonic::Status> {
    let action = Action {
        r#type: "topic_create".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{name}",
            "session_uuid": "{key}",
            "serialization_format": "default",
            "ontology_tag": "mock",
            "user_metadata": {mdata}
        }}
        "#,
            name = topic_name,
            key = key,
            mdata = json_metadata.unwrap_or("{}"),
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    let mut key: Option<types::Uuid> = None;

    while let Some(result) = stream.message().await? {
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_create");

        let uuid: types::Uuid = r.response["uuid"]
            .as_str()
            .ok_or_else(|| tonic::Status::internal("uuid is not a string"))?
            .parse::<types::Uuid>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse uuid: {e}")))?;

        key = Some(uuid);
    }

    key.ok_or_else(|| tonic::Status::internal("Unable to return key"))
}

pub async fn topic_delete(client: &mut Client, locator: &str) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "topic_delete".to_owned(),
        body: format!(
            r#"
        {{
            "locator": "{}"
        }}
        "#,
            locator,
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_delete");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }

    Ok(())
}

pub async fn do_put(
    client: &mut Client,
    topic_uuid: &types::Uuid,
    topic_name: &str,
    batches: Vec<RecordBatch>,
    no_descriptor: bool,
) -> Result<tonic::Response<Streaming<PutResult>>, tonic::Status> {
    let input_stream = futures::stream::iter(batches.into_iter().map(Ok));

    let cmd = format!(
        r#"
        {{
            "resource_locator": "{}",
            "topic_uuid": "{}"
        }}
        "#,
        topic_name, topic_uuid
    );

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_max_flight_data_size(25_000_000)
        .with_flight_descriptor(if no_descriptor {
            None
        } else {
            Some(FlightDescriptor::new_cmd(cmd))
        })
        .build(input_stream)
        .map(|v| v.unwrap());

    client.do_put(flight_data_stream).await
}

pub async fn do_get(
    client: &mut Client,
    topic_name: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let locator = topic_name.parse().unwrap();
    let ticket_payload = types::flight::TicketTopic {
        locator,
        timestamp_range: None,
    };

    let ticket = Ticket {
        ticket: marshal::flight::ticket_topic_to_binary(ticket_payload)
            .unwrap()
            .into(),
    };

    Ok(do_get_with_ticket(client, ticket).await?.1)
}

pub type DoGetMetadata = Option<HashMap<String, String>>;

pub async fn do_get_with_ticket(
    client: &mut Client,
    ticket: arrow_flight::Ticket,
) -> Result<(DoGetMetadata, Vec<RecordBatch>), tonic::Status> {
    let stream = client.do_get(ticket).await?.into_inner();

    let record_batch_stream =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));

    let batches = record_batch_stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| tonic::Status::internal(format!("do_get decode error: {e}")))?;

    let metadata = batches.first().map(|b| b.schema().metadata().clone());

    Ok((metadata, batches))
}

pub async fn server_version(client: &mut Client) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "version".to_owned(),
        body: r#"{}"#.to_string().into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "version");

        assert!(r.response.as_object().unwrap().contains_key("version"));
        let semver = r.response.as_object().unwrap().get("semver").unwrap();

        assert!(semver.as_object().unwrap().contains_key("major"));
        assert!(semver.as_object().unwrap().contains_key("minor"));
        assert!(semver.as_object().unwrap().contains_key("patch"));
        assert!(semver.as_object().unwrap().contains_key("pre"));

        let major = semver.as_object().unwrap().get("major").unwrap();
        assert!(major.is_u64());
        let minor = semver.as_object().unwrap().get("minor").unwrap();
        assert!(minor.is_u64());
        let patch = semver.as_object().unwrap().get("patch").unwrap();
        assert!(patch.is_u64());
        if let Some(pre) = semver.as_object().unwrap().get("pre") {
            assert!(pre.is_string());
        }
    }

    Ok(())
}

/// Returns flight info data for a sequence or a topic in the given interval.
pub async fn get_flight_info(
    client: &mut Client,
    topic_name: &str,
    interval: Option<types::TimestampRange>,
) -> Result<FlightInfo, tonic::Status> {
    let cmd = format!(
        r#"
        {{
            "resource_locator": "{}",
            "timestamp_ns_start": {},
            "timestamp_ns_end": {}
        }}
        "#,
        topic_name,
        interval
            .clone()
            .map_or("null".to_owned(), |range| range.start.to_string()),
        interval.map_or("null".to_owned(), |range| range.end.to_string()),
    );

    dbg!(&cmd);

    let descriptor = FlightDescriptor::new_cmd(cmd);

    let info = client.get_flight_info(descriptor).await?.into_inner();

    Ok(info)
}

/// Returns the arrow schema for a topic.
pub async fn get_schema(
    client: &mut Client,
    topic_name: &str,
) -> Result<arrow::datatypes::Schema, tonic::Status> {
    let cmd = format!(
        r#"
        {{
            "resource_locator": "{}"
        }}
        "#,
        topic_name,
    );

    dbg!(&cmd);

    let descriptor = FlightDescriptor::new_cmd(cmd);

    let schema_result = client.get_schema(descriptor).await?.into_inner();

    arrow::datatypes::Schema::try_from(schema_result)
        .map_err(|e| tonic::Status::internal(format!("failed to decode schema: {e}")))
}

pub async fn sequence_notification_create(
    client: &mut Client,
    locator: &str,
    notification_type: String,
    msg: String,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "sequence_notification_create".to_owned(),
        body: format!(
            r#"{{"locator":"{}", "notification_type": "{}", "msg": "{}"}}"#,
            locator, notification_type, msg
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_notification_create");
    }

    Ok(())
}

pub async fn sequence_notification_list(
    client: &mut Client,
    locator: &str,
) -> Result<serde_json::Value, tonic::Status> {
    let action = Action {
        r#type: "sequence_notification_list".to_owned(),
        body: format!(r#"{{ "locator" : "{}" }}"#, locator).into(),
    };

    dbg!(&action);
    let mut ret = serde_json::Value::Null;
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_notification_list");
        ret = r.response;
    }

    Ok(ret)
}

pub async fn sequence_notification_purge(
    client: &mut Client,
    locator: &str,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "sequence_notification_purge".to_owned(),
        body: format!(r#"{{ "locator" : "{}" }}"#, locator).into(),
    };

    dbg!(&action);
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_notification_purge");
    }

    Ok(())
}

pub async fn topic_notification_create(
    client: &mut Client,
    locator: &str,
    notification_type: String,
    msg: String,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "topic_notification_create".to_owned(),
        body: format!(
            r#"{{"locator":"{}", "notification_type": "{}", "msg": "{}"}}"#,
            locator, notification_type, msg
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_notification_create");
    }

    Ok(())
}

pub async fn topic_notification_list(
    client: &mut Client,
    locator: &str,
) -> Result<serde_json::Value, tonic::Status> {
    let action = Action {
        r#type: "topic_notification_list".to_owned(),
        body: format!(r#"{{ "locator" : "{}" }}"#, locator).into(),
    };

    dbg!(&action);
    let mut ret = serde_json::Value::Null;
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_notification_list");
        ret = r.response;
    }

    Ok(ret)
}

pub async fn topic_notification_purge(
    client: &mut Client,
    locator: &str,
) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "topic_notification_purge".to_owned(),
        body: format!(r#"{{ "locator" : "{}" }}"#, locator).into(),
    };

    dbg!(&action);
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_notification_purge");
    }

    Ok(())
}

pub async fn topic_filter_clusterize(
    client: &mut Client,
    locator: &str,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<FilterTimestampRange>,
) -> Result<Vec<serde_json::Value>, tonic::Status> {
    let body = json!({
        "locator": locator,
        "clustering_dt_ns": clustering_dt_ns,
        "ontology": ontology,
        "timestamp_range": timestamp_range,
    });

    let action = Action {
        r#type: "topic_filter_clusterize".to_owned(),
        body: serde_json::to_vec(&body)
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .into(),
    };

    dbg!(&action);
    let mut ret: Vec<serde_json::Value> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_filter_clusterize");
        ret.push(r.response);
    }

    Ok(ret)
}

pub async fn topic_filter_intersect(
    client: &mut Client,
    topics: Vec<mosaicod_marshal::requests::TopicClusterizeParams>,
    intersect_dt_ns: u64,
) -> Result<Vec<serde_json::Value>, tonic::Status> {
    let body = json!({
        "topics": topics,
        "intersect_dt_ns": intersect_dt_ns,
    });

    let action = Action {
        r#type: "topic_filter_intersect".to_owned(),
        body: serde_json::to_vec(&body)
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .into(),
    };

    dbg!(&action);
    let mut ret: Vec<serde_json::Value> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_filter_intersect");
        ret.push(r.response);
    }

    Ok(ret)
}

pub async fn query(
    client: &mut Client,
    filter: serde_json::Value,
) -> Result<Vec<serde_json::Value>, tonic::Status> {
    let action = Action {
        r#type: "query".to_owned(),
        body: serde_json::to_vec(&filter)
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .into(),
    };

    dbg!(&action);

    let mut items: Vec<serde_json::Value> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "query");
        if let Some(arr) = r.response["items"].as_array() {
            items.extend(arr.iter().cloned());
        }
    }

    Ok(items)
}

/// Helper function to create sequence notifications.
pub async fn setup_sequence_with_notifications(
    client: &mut Client,
    sequence_name: &str,
    notification_type: String,
    notifications_size: usize,
) -> Result<(), tonic::Status> {
    sequence_create(client, sequence_name, None).await.unwrap();
    for i in 0..notifications_size {
        let error_msg = format!("Error {}_{}", sequence_name, i + 1);
        sequence_notification_create(client, sequence_name, notification_type.clone(), error_msg)
            .await?;
    }

    Ok(())
}

/// Helper function to create sequence notifications.
pub async fn setup_topic_with_notifications(
    client: &mut Client,
    sequence_name: &str,
    topic_name: &str,
    notification_type: String,
    notifications_size: usize,
) -> Result<(), tonic::Status> {
    sequence_create(client, sequence_name, None).await.unwrap();
    let (_, session_uuid) = session_create(client, sequence_name).await.unwrap();
    let topic_uuid = topic_create(client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let batches = vec![ext::arrow::testing::dummy_batch(7, 10000, 5, 1, 1)];
    do_put(client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    session_finalize(client, &session_uuid).await.unwrap();

    for i in 0..notifications_size {
        let error_msg = format!("Error {}_{}", topic_name, i + 1);
        topic_notification_create(client, topic_name, notification_type.clone(), error_msg).await?;
    }

    Ok(())
}
