use super::common::{ActionResponse, Client};
use arrow_flight::Action;

/// Create a new sequence.
/// Returns the `key` of the newly created sequence, this key is required to perform action
/// like create/upload topics, etc.
pub async fn sequence_create(
    client: &mut Client,
    sequence_name: &str,
    json_metadata: Option<&str>,
) -> uuid::Uuid {
    let action = Action {
        r#type: "sequence_create".to_owned(),
        body: format!(
            r#"
        {{
            "name": "{}",
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

    let mut key: Option<uuid::Uuid> = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "sequence_create");

        let key_str = r.response["key"].as_str().unwrap();
        key = Some(uuid::Uuid::parse_str(key_str).expect("Invalid uuid"));
    }

    key.expect("Unable to return key")
}

/// Create a new topic.
/// Returns the `key` of the newly created sequence, this key is required to upload topic data.
pub async fn topic_create(
    client: &mut Client,
    key: &uuid::Uuid,
    topic_name: &str,
    json_metadata: Option<&str>,
) -> uuid::Uuid {
    let action = Action {
        r#type: "topic_create".to_owned(),
        body: format!(
            r#"
        {{
            "name": "{name}",
            "sequence_key": "{key}",
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

    let mut key: Option<uuid::Uuid> = None;

    while let Some(result) = stream.message().await.expect("Problem while streaming") {
        let r = ActionResponse::from_body(&result.body);
        assert_eq!(r.action, "topic_create");

        let key_str = r.response["key"].as_str().unwrap();
        key = Some(uuid::Uuid::parse_str(key_str).expect("Invalid uuid"));
    }

    key.expect("Unable to return key")
}
