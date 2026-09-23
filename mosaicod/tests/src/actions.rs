use super::common::Client;
use arrow::array::RecordBatch;
use arrow_flight::decode::DecodedPayload;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, FlightDescriptor, FlightInfo, PutResult};
use futures::StreamExt;
use futures::TryStreamExt;
use mosaicod_core::types;
use mosaicod_ext as ext;
use prost::Message;

use arrow_flight::Ticket;
use mosaicod_marshal::{self as marshal, Ontology, requests, responses};

use tonic::Streaming;

/// Encodes `msg` as raw protobuf binary into a Flight `Action` of the given type.
fn action(r#type: &str, msg: &impl Message) -> Action {
    let action = Action {
        r#type: r#type.to_owned(),
        body: msg.encode_to_vec().into(),
    };
    dbg!(&action);
    action
}

/// Create a new sequence.
/// Returns the `key` of the newly created sequence, this key is required to perform action
/// like create/upload topics, etc.
pub async fn sequence_create(
    client: &mut Client,
    sequence_name: &str,
    json_metadata: Option<&str>,
) -> Result<(), tonic::Status> {
    let action = action(
        "sequence_create",
        &requests::SequenceCreate {
            locator: sequence_name.to_owned(),
            user_metadata: json_metadata.unwrap_or("{}").as_bytes().to_vec(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn sequence_delete(client: &mut Client, locator: &str) -> Result<(), tonic::Status> {
    let action = action(
        "sequence_delete",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn session_create(
    client: &mut Client,
    sequence_name: &str,
) -> Result<(types::SessionLocator, types::Uuid), tonic::Status> {
    let action = action(
        "session_create",
        &requests::ResourceLocator {
            locator: sequence_name.to_owned(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    let mut key: Option<(types::SessionLocator, types::Uuid)> = None;

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = responses::SessionCreate::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;

        let locator = r.locator.parse::<types::SessionLocator>().map_err(|e| {
            tonic::Status::internal(format!("Failed to parse session locator: {e}"))
        })?;

        let uuid = r
            .uuid
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
    let action = action(
        "session_finalize",
        &requests::SessionUuid {
            session_uuid: session_uuid.to_string(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

/// Send an action to delete the current session
pub async fn session_delete(
    client: &mut Client,
    session_locator: &types::SessionLocator,
) -> Result<(), tonic::Status> {
    let action = action(
        "session_delete",
        &requests::ResourceLocator {
            locator: session_locator.to_string(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
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
    let action = action(
        "topic_create",
        &requests::TopicCreate {
            locator: topic_name.to_owned(),
            session_uuid: key.to_string(),
            serialization_format: marshal::Format::Default as i32,
            ontology_tag: "mock".to_owned(),
            user_metadata: json_metadata.unwrap_or("{}").as_bytes().to_vec(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    let mut key: Option<types::Uuid> = None;

    while let Some(result) = stream.message().await? {
        let r = responses::ResourceUuid::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;

        let uuid: types::Uuid = r
            .uuid
            .parse::<types::Uuid>()
            .map_err(|e| tonic::Status::internal(format!("Failed to parse uuid: {e}")))?;

        key = Some(uuid);
    }

    key.ok_or_else(|| tonic::Status::internal("Unable to return key"))
}

pub async fn topic_delete(client: &mut Client, locator: &str) -> Result<(), tonic::Status> {
    let action = action(
        "topic_delete",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
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

    let cmd = mosaicod_proto::v1::flight::DoPutCmd {
        resource_locator: topic_name.to_owned(),
        topic_uuid: topic_uuid.to_string(),
    }
    .encode_to_vec();

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
        ticket: marshal::flight::ticket_topic_to_bytes(ticket_payload).into(),
    };

    do_get_with_ticket(client, ticket).await
}

pub async fn do_get_with_ticket(
    client: &mut Client,
    ticket: arrow_flight::Ticket,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let stream = client.do_get(ticket).await?.into_inner();

    let record_batch_stream =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));

    let mut decoder = record_batch_stream.into_inner();

    let mut batches: Vec<RecordBatch> = vec![];

    while let Some(result) = decoder.next().await {
        let decoded = result?;
        if let DecodedPayload::RecordBatch(batch) = decoded.payload {
            batches.push(batch);
        }
    }

    Ok(batches)
}

pub async fn server_info(client: &mut Client) -> Result<(), tonic::Status> {
    let action = action("info", &requests::Empty {});

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = responses::ServerInfo::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;

        assert!(!r.version.is_empty());

        let semver = r.semver.expect("semver is missing");
        // `major` is 0 for a pre-1.0 version: only check the field exists on
        // the type (compile-time), not that it's non-zero.
        let _ = semver.major;
        assert!(semver.minor > 0 || semver.patch > 0 || !semver.pre.is_empty());

        let config = r.config.expect("config is missing");
        assert!(config.max_grpc_message_size > 0);
        assert!(config.target_message_size > 0);
    }

    Ok(())
}

/// Returns flight info data for a sequence or a topic in the given interval.
pub async fn get_flight_info(
    client: &mut Client,
    topic_name: &str,
    interval: Option<types::TimestampRange>,
) -> Result<FlightInfo, tonic::Status> {
    let cmd = mosaicod_proto::v1::flight::GetFlightInfoCmd {
        resource_locator: topic_name.to_owned(),
        timestamp_ns_start: interval.map(|range| range.start.as_i64()),
        timestamp_ns_end: interval.map(|range| range.end.as_i64()),
    }
    .encode_to_vec();

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
    let cmd = mosaicod_proto::v1::flight::GetSchemaCmd {
        resource_locator: topic_name.to_owned(),
    }
    .encode_to_vec();

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
    let action = action(
        "sequence_notification_create",
        &requests::NotificationCreate {
            locator: locator.to_owned(),
            notification_type,
            msg,
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn sequence_notification_list(
    client: &mut Client,
    locator: &str,
) -> Result<responses::NotificationList, tonic::Status> {
    let action = action(
        "sequence_notification_list",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut ret = responses::NotificationList::default();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        ret = responses::NotificationList::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;
    }

    Ok(ret)
}

pub async fn sequence_notification_purge(
    client: &mut Client,
    locator: &str,
) -> Result<(), tonic::Status> {
    let action = action(
        "sequence_notification_purge",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn topic_notification_create(
    client: &mut Client,
    locator: &str,
    notification_type: String,
    msg: String,
) -> Result<(), tonic::Status> {
    let action = action(
        "topic_notification_create",
        &requests::NotificationCreate {
            locator: locator.to_owned(),
            notification_type,
            msg,
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn topic_notification_list(
    client: &mut Client,
    locator: &str,
) -> Result<responses::NotificationList, tonic::Status> {
    let action = action(
        "topic_notification_list",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut ret = responses::NotificationList::default();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        ret = responses::NotificationList::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;
    }

    Ok(ret)
}

pub async fn topic_notification_purge(
    client: &mut Client,
    locator: &str,
) -> Result<(), tonic::Status> {
    let action = action(
        "topic_notification_purge",
        &requests::ResourceLocator {
            locator: locator.to_owned(),
        },
    );

    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        assert!(result.body.is_empty());
    }

    Ok(())
}

pub async fn topic_filter_clusterize(
    client: &mut Client,
    locator: &str,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<marshal::TimestampRange>,
) -> Result<Vec<responses::TopicFilterClusterize>, tonic::Status> {
    let action = action(
        "topic_filter_clusterize",
        &requests::TopicClusterizeParams {
            locator: locator.to_owned(),
            clustering_dt_ns,
            ontology: serde_json::to_vec(&ontology)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
            timestamp_range,
        },
    );

    let mut ret: Vec<responses::TopicFilterClusterize> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = responses::TopicFilterClusterize::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;
        ret.push(r);
    }

    Ok(ret)
}

pub async fn topic_filter_intersect(
    client: &mut Client,
    topics: Vec<mosaicod_marshal::requests::TopicClusterizeParams>,
    intersect_dt_ns: u64,
) -> Result<Vec<responses::TopicFilterClusterize>, tonic::Status> {
    let action = action(
        "topic_filter_intersect",
        &requests::TopicFilterIntersect {
            topics,
            intersect_dt_ns,
        },
    );

    let mut ret: Vec<responses::TopicFilterClusterize> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();
    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = responses::TopicFilterClusterize::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;
        ret.push(r);
    }

    Ok(ret)
}

pub async fn query(
    client: &mut Client,
    filter: serde_json::Value,
) -> Result<Vec<responses::ResponseQueryItem>, tonic::Status> {
    let query_bytes =
        serde_json::to_vec(&filter).map_err(|e| tonic::Status::internal(e.to_string()))?;
    let action = action("query", &requests::Query { query: query_bytes });

    let mut items: Vec<responses::ResponseQueryItem> = Vec::new();
    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await? {
        dbg!(&result);
        let r = responses::Query::decode(result.body.as_ref())
            .map_err(|e| tonic::Status::internal(format!("failed to decode response: {e}")))?;
        items.extend(r.items);
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
