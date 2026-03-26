use super::common::{ActionResponse, Client};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, FlightDescriptor, FlightInfo, PutResult};
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

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_create");

        let available_keys = r.response.as_object().map(|o| o.len()).unwrap_or(0);
        assert_eq!(available_keys, 0);
    }

    Ok(())
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
pub async fn session_delete(client: &mut Client, session_uuid: &types::Uuid) {
    let action = Action {
        r#type: "session_delete".to_owned(),
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
        assert_eq!(r.action, "session_delete");
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
            .expect("Error casting to string")
            .parse()
            .expect("Error parsing uuid");

        key = Some(uuid);
    }

    Ok(key.expect("Unable to return key"))
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
        .with_flight_descriptor(if no_descriptor {
            None
        } else {
            Some(FlightDescriptor::new_cmd(cmd))
        })
        .build(input_stream)
        .map(|v| v.unwrap());

    client.do_put(flight_data_stream).await
}

pub async fn server_version(client: &mut Client) {
    let action = Action {
        r#type: "version".to_owned(),
        body: r#"{}"#.to_string().into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await.unwrap().into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
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
}

/// Returns flight info data for a sequence or a topic.
pub async fn get_flight_info(
    client: &mut Client,
    topic_name: &str,
) -> Result<FlightInfo, tonic::Status> {
    let cmd = format!(
        r#"
        {{
            "resource_locator": "{}"
        }}
        "#,
        topic_name
    );

    dbg!(&cmd);

    let descriptor = FlightDescriptor::new_cmd(cmd);

    let info = client.get_flight_info(descriptor).await?.into_inner();

    Ok(info)
}

pub async fn api_key_create(
    client: &mut Client,
    permissions: types::auth::Permission,
    description: String,
    expires_at: Option<types::Timestamp>,
) -> Result<types::auth::Token, tonic::Status> {
    let action = Action {
        r#type: "api_key_create".to_owned(),
        body: format!(
            r#"{{
            "permissions": "{}",
            "description": "{}",
            "expires_at_ns": {}
        }}"#,
            String::from(permissions),
            description,
            expires_at.map_or(String::from("null"), |t| { t.to_string() })
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    let mut api_key_token: Option<types::auth::Token> = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "api_key_create");

        api_key_token = Some(
            r.response["api_key_token"]
                .as_str()
                .expect("Error casting api key token to string")
                .parse()
                .unwrap(),
        );
    }

    Ok(api_key_token.expect("unable to read api key token"))
}

pub async fn api_key_status(
    client: &mut Client,
    fingerprint: &str,
) -> Result<(String, String, i64, Option<i64>), tonic::Status> {
    let action = Action {
        r#type: "api_key_status".to_owned(),
        body: format!(
            r#"{{
            "api_key_fingerprint": "{}"
        }}"#,
            fingerprint
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    let mut api_key_status = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "api_key_status");

        api_key_status = Some((
            r.response["api_key_fingerprint"]
                .as_str()
                .expect("Error casting api key fingerprint to string")
                .to_string(),
            r.response["description"]
                .as_str()
                .expect("Error casting api key description to string")
                .to_string(),
            r.response["created_at_ns"]
                .as_i64()
                .expect("Error casting api key created_at_ns into an i64"),
            r.response["expires_at_ns"].as_i64(),
        ));
    }

    Ok(api_key_status.expect("unable to read api key status"))
}

pub async fn api_key_revoke(client: &mut Client, fingerprint: &str) -> Result<(), tonic::Status> {
    let action = Action {
        r#type: "api_key_revoke".to_owned(),
        body: format!(
            r#"{{
            "api_key_fingerprint": "{}"
        }}"#,
            fingerprint
        )
        .into(),
    };

    dbg!(&action);

    let mut stream = client.do_action(action).await?.into_inner();

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        dbg!(&result);
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "api_key_revoke");
        assert!(r.response.as_object().is_none());
    }

    Ok(())
}
