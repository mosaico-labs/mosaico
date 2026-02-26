use super::common::{ActionResponse, Client};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, FlightDescriptor, PutResult};
use mosaicod_core::types;
use tonic::Streaming;

use arrow::array::RecordBatch;

use futures::StreamExt;

/// Create a new sequence.
/// Returns the `key` of the newly created sequence, this key is required to perform action
/// like create/upload topics, etc.
pub async fn sequence_create(
    client: &mut Client,
    sequence_name: &str,
    json_metadata: Option<&str>,
) {
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_create");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }
}

pub async fn sequence_delete(client: &mut Client, locator: &str) {
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_delete");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }
}

pub async fn session_create(client: &mut Client, sequence_name: &str) -> types::Uuid {
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    let mut key: Option<types::Uuid> = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_create");

        let uuid: types::Uuid = r.response["uuid"]
            .as_str()
            .expect("Error casting to string")
            .parse()
            .expect("Error parsing uuid");

        key = Some(uuid);
    }

    key.expect("Unable to return key")
}

pub async fn session_finalize(client: &mut Client, session_uuid: types::Uuid) {
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_finalize");

        assert!(r.response.as_object().is_none());
    }
}

/// Send an action to abort the current session
pub async fn session_abort(client: &mut Client, session_uuid: types::Uuid) {
    let action = Action {
        r#type: "session_abort".to_owned(),
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "session_abort");

        assert!(r.response.as_object().is_none());
    }
}

/// Create a new topic.
/// Returns the `key` of the newly created topic, this key is required to upload topic data.
pub async fn topic_create(
    client: &mut Client,
    key: &types::Uuid,
    topic_name: &str,
    json_metadata: Option<&str>,
) -> types::Uuid {
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

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    let mut key: Option<types::Uuid> = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_create");

        let uuid: types::Uuid = r.response["uuid"]
            .as_str()
            .expect("Error casting to string")
            .parse()
            .expect("Error parsing uuid");

        key = Some(uuid);
    }

    key.expect("Unable to return key")
}

pub async fn do_put(
    client: &mut Client,
    topic_uuid: &types::Uuid,
    topic_name: &str,
    batches: Vec<RecordBatch>,
) -> tonic::Response<Streaming<PutResult>> {
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
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(cmd)))
        .build(input_stream)
        .map(|v| v.unwrap());

    client.do_put(flight_data_stream).await.unwrap()
}
