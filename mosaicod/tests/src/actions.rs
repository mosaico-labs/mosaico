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
use mosaicod_marshal::{self as marshal, requests, responses};
use mosaicod_proto::v1::query as proto;
use std::collections::HashMap;

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
    json_metadata: &str,
) -> Result<(), tonic::Status> {
    let action = action(
        "sequence_create",
        &requests::SequenceCreate {
            locator: sequence_name.to_owned(),
            user_metadata: json_metadata.as_bytes().to_vec(),
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
    json_metadata: &str,
) -> Result<types::Uuid, tonic::Status> {
    let action = action(
        "topic_create",
        &requests::TopicCreate {
            locator: topic_name.to_owned(),
            session_uuid: key.to_string(),
            serialization_format: marshal::Format::Default as i32,
            ontology_tag: "mock".to_owned(),
            user_metadata: json_metadata.as_bytes().to_vec(),
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

/// Like [`topic_create`], but takes the raw request so tests can send wire
/// values the `Format` enum can't represent (e.g. an out-of-range
/// `serialization_format`).
pub async fn topic_create_raw(
    client: &mut Client,
    raw: requests::TopicCreate,
) -> Result<types::Uuid, tonic::Status> {
    let action = action("topic_create", &raw);

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
        locator: topic_name.to_owned(),
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

        r.semver.expect("semver is missing");

        let config = r.config.expect("config is missing");
        assert!(config.grpc_max_decode_message_size > 0);
        assert!(config.grpc_max_encode_message_size > 0);
        assert!(config.grpc_target_encode_message_size > 0);
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
        locator: topic_name.to_owned(),
        timestamp_range: interval.map(marshal::timestamp_range_to_proto),
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
        locator: topic_name.to_owned(),
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
    ontology: serde_json::Value,
    timestamp_range: Option<marshal::TimestampRange>,
) -> Result<Vec<responses::TopicFilterClusterize>, tonic::Status> {
    let action = action(
        "topic_filter_clusterize",
        &requests::TopicClusterizeParams {
            locator: locator.to_owned(),
            clustering_dt_ns,
            ontology: Some(ontology_filter_to_proto(&ontology)),
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
    let action = action(
        "query",
        &requests::Query {
            filter: Some(query_filter_to_proto(&filter)),
        },
    );

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

/// Translates a query filter written in json
fn query_filter_to_proto(filter: &serde_json::Value) -> proto::Filter {
    proto::Filter {
        sequence: filter.get("sequence").map(|s| proto::SequenceFilter {
            name: s.get("name").map(json_to_condition),
            created_at_ns: s.get("created_at_ns").map(json_to_condition),
            user_metadata: json_to_conditions(s.get("user_metadata")),
        }),
        topic: filter.get("topic").map(|t| proto::TopicFilter {
            name: t.get("name").map(json_to_condition),
            created_at_ns: t.get("created_at_ns").map(json_to_condition),
            ontology_tag: t.get("ontology_tag").map(json_to_condition),
            serialization_format: t.get("serialization_format").map(json_to_condition),
            user_metadata: json_to_conditions(t.get("user_metadata")),
        }),
        ontology: filter.get("ontology").map(ontology_filter_to_proto),
    }
}

/// Translates an ontology filter written in the JSON DSL (e.g.
/// `{"mock.value": {"$gt": 5}}`) into its protobuf form.
pub fn ontology_filter_to_proto(ontology: &serde_json::Value) -> proto::OntologyFilter {
    proto::OntologyFilter {
        exprs: ontology
            .as_object()
            .into_iter()
            .flatten()
            .map(|(field, op)| proto::OntologyPredicate {
                field: field.clone(),
                aggregator: proto::Aggregator::Unspecified as i32,
                condition: Some(json_to_condition(op)),
            })
            .collect(),
    }
}

fn json_to_conditions(v: Option<&serde_json::Value>) -> HashMap<String, proto::Condition> {
    v.and_then(|v| v.as_object())
        .into_iter()
        .flatten()
        .map(|(k, op)| (k.clone(), json_to_condition(op)))
        .collect()
}

/// `"$ex"`/`"$nex"` are plain strings, every other operator is `{"$op": value}`.
fn json_to_condition(op: &serde_json::Value) -> proto::Condition {
    use proto::Operator;

    let (name, value) = match op {
        serde_json::Value::String(s) => (s.as_str(), None),
        serde_json::Value::Object(m) if m.len() == 1 => {
            let (k, v) = m.iter().next().unwrap();
            (k.as_str(), Some(v))
        }
        _ => ("", None),
    };

    let op = match name {
        "$eq" => Operator::Eq,
        "$neq" => Operator::Neq,
        "$lt" => Operator::Lt,
        "$leq" => Operator::Leq,
        "$gt" => Operator::Gt,
        "$geq" => Operator::Geq,
        "$between" => Operator::Between,
        "$outside" => Operator::Outside,
        "$in" => Operator::In,
        "$match" => Operator::Match,
        "$ex" => Operator::Ex,
        "$nex" => Operator::Nex,
        _ => Operator::Unspecified,
    };

    proto::Condition {
        op: op as i32,
        value: value.map(json_to_value),
    }
}

fn json_to_value(v: &serde_json::Value) -> proto::Value {
    use proto::value::Kind;

    let kind = match v {
        serde_json::Value::Bool(b) => Some(Kind::Boolean(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Kind::Integer)
            .or_else(|| n.as_f64().map(Kind::Float)),
        serde_json::Value::String(s) => Some(Kind::Text(s.clone())),
        serde_json::Value::Array(a) => json_to_array(a),
        serde_json::Value::Null | serde_json::Value::Object(_) => None,
    };

    proto::Value { kind }
}

/// Picks the array type from the elements, mixed or nested arrays are not representable.
fn json_to_array(a: &[serde_json::Value]) -> Option<proto::value::Kind> {
    use proto::value::Kind;

    if let Some(values) = a.iter().map(|v| v.as_i64()).collect() {
        return Some(Kind::IntegerArray(proto::IntegerArray { values }));
    }
    if let Some(values) = a.iter().map(|v| v.as_f64()).collect() {
        return Some(Kind::FloatArray(proto::FloatArray { values }));
    }
    if let Some(values) = a.iter().map(|v| v.as_str().map(str::to_owned)).collect() {
        return Some(Kind::TextArray(proto::TextArray { values }));
    }
    if let Some(values) = a.iter().map(|v| v.as_bool()).collect() {
        return Some(Kind::BooleanArray(proto::BooleanArray { values }));
    }
    None
}

/// Helper function to create sequence notifications.
pub async fn setup_sequence_with_notifications(
    client: &mut Client,
    sequence_name: &str,
    notification_type: String,
    notifications_size: usize,
) -> Result<(), tonic::Status> {
    sequence_create(client, sequence_name, "").await.unwrap();
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
    sequence_create(client, sequence_name, "").await.unwrap();
    let (_, session_uuid) = session_create(client, sequence_name).await.unwrap();
    let topic_uuid = topic_create(client, &session_uuid, topic_name, "")
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
