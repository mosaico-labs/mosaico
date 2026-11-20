//! JSON projection of [`types::HookEvent`]: the body mosaicod `POST`s to hooks.

use mosaicod_core::types;
use serde::Serialize;

use super::{Error, JsonMetadataBlob};

/// The body of a hook request:
///
/// ```json
/// { "id": "…", "type": "topic_uploaded", "data": { … } }
/// ```
#[derive(Serialize)]
pub struct JsonHookEvent {
    /// The same across retries of the event, and across the hooks it's sent to.
    id: String,
    #[serde(flatten)]
    event: JsonHookEventKind,
}

impl JsonHookEvent {
    pub fn new(id: &types::Uuid, event: types::HookEvent<JsonMetadataBlob>) -> Self {
        let event = match event {
            types::HookEvent::TopicUploaded(metadata) => {
                JsonHookEventKind::TopicUploaded(JsonTopicUploaded {
                    topic: metadata.properties.locator.to_string(),
                    sequence: metadata.properties.locator.sequence.to_string(),
                    session: metadata.properties.session_locator.to_string(),
                    ontology_tag: metadata.ontology_metadata.ontology_tag,
                    user_metadata: metadata.ontology_metadata.user_metadata,
                    created_at: metadata.properties.created_at.as_i64(),
                })
            }

            types::HookEvent::SessionUploaded(metadata) => {
                JsonHookEventKind::SessionUploaded(JsonSessionUploaded {
                    session: metadata.locator.to_string(),
                    sequence: metadata.locator.sequence.to_string(),
                    topics: metadata.topics.iter().map(ToString::to_string).collect(),
                    created_at: metadata.created_at.as_i64(),
                })
            }
        };

        Self {
            id: id.to_string(),
            event,
        }
    }

    /// The request body.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|e| Error::SerializationError(e.to_string()))
    }
}

/// `type` is the variant's name, as given by [`types::HookEvent::name`].
#[derive(Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum JsonHookEventKind {
    TopicUploaded(JsonTopicUploaded),
    SessionUploaded(JsonSessionUploaded),
}

/// `created_at` is in Unix nanoseconds, here and in [`JsonSessionUploaded`].
#[derive(Serialize)]
struct JsonTopicUploaded {
    topic: String,
    sequence: String,
    session: String,
    ontology_tag: String,
    user_metadata: Option<JsonMetadataBlob>,
    created_at: i64,
}

#[derive(Serialize)]
struct JsonSessionUploaded {
    session: String,
    sequence: String,
    topics: Vec<String>,
    created_at: i64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event_id() -> types::Uuid {
        "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90".parse().unwrap()
    }

    fn topic_uploaded(
        user_metadata: Option<serde_json::Value>,
    ) -> types::HookEvent<JsonMetadataBlob> {
        types::HookEvent::TopicUploaded(types::TopicMetadata {
            properties: types::TopicMetadataProperties {
                created_at: 1_000.into(),
                completed_at: Some(2_000.into()),
                session_locator: "drive_042:session".parse().unwrap(),
                locator: "drive_042/front_camera".parse().unwrap(),
            },
            ontology_metadata: types::TopicOntologyMetadata {
                serialization_format: types::Format::Image,
                ontology_tag: "image".to_owned(),
                user_metadata: user_metadata.map(JsonMetadataBlob::from),
            },
        })
    }

    fn to_json(event: types::HookEvent<JsonMetadataBlob>) -> serde_json::Value {
        let body = JsonHookEvent::new(&event_id(), event).to_bytes().unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[test]
    fn topic_uploaded_json() {
        let event = topic_uploaded(Some(json!({ "driver": "Ada" })));

        assert_eq!(
            to_json(event),
            json!({
                "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
                "type": "topic_uploaded",
                "data": {
                    "topic": "drive_042/front_camera",
                    "sequence": "drive_042",
                    "session": "drive_042:session",
                    "ontology_tag": "image",
                    "user_metadata": { "driver": "Ada" },
                    "created_at": 1_000,
                },
            })
        );
    }

    #[test]
    fn topic_uploaded_json_without_user_metadata() {
        let json = to_json(topic_uploaded(None));

        assert_eq!(json["data"]["user_metadata"], serde_json::Value::Null);
    }

    #[test]
    fn session_uploaded_json() {
        let event = types::HookEvent::SessionUploaded(types::SessionMetadata {
            locator: "drive_042:session".parse().unwrap(),
            topics: vec![
                "drive_042/front_camera".parse().unwrap(),
                "drive_042/imu".parse().unwrap(),
            ],
            created_at: 1_000.into(),
            completed_at: Some(2_000.into()),
        });

        assert_eq!(
            to_json(event),
            json!({
                "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
                "type": "session_uploaded",
                "data": {
                    "session": "drive_042:session",
                    "sequence": "drive_042",
                    "topics": ["drive_042/front_camera", "drive_042/imu"],
                    "created_at": 1_000,
                },
            })
        );
    }

    #[test]
    fn type_matches_event_name() {
        let events = [
            topic_uploaded(None),
            types::HookEvent::SessionUploaded(types::SessionMetadata {
                locator: "drive_042:session".parse().unwrap(),
                topics: vec![],
                created_at: 1_000.into(),
                completed_at: None,
            }),
        ];

        for event in events {
            let name = event.name();
            assert_eq!(to_json(event)["type"], name);
        }
    }
}
