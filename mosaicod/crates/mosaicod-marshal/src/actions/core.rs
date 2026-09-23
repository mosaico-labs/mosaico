use super::{requests, responses};
use crate::time::timestamp_range_to_proto;
use mosaicod_core as core;
use mosaicod_ext as ext;
use prost::Message;
use thiserror::Error;

/// Represents possible errors that can occur while handling an [`ActionRequest`].
#[derive(Error, Debug)]
pub enum ActionError {
    /// No matching action found for the provided string.
    ///
    /// The string that failed to match is included in the error.
    #[error("no available action for string `{0}`")]
    MissingAction(String),

    /// Failed to decode the request body as the expected protobuf message.
    #[error("body decode error")]
    BodyDecodeError(#[from] prost::DecodeError),

    /// Failed to parse a raw-bytes JSON field (e.g. `user_metadata`, the
    /// query filter) as JSON.
    #[error("body deserialization error")]
    BodyDeserializationError(#[from] serde_json::Error),
}

impl core::error::PublicError for ActionError {
    fn error(&self) -> core::Error {
        match self {
            Self::MissingAction(_)
            | Self::BodyDecodeError(_)
            | Self::BodyDeserializationError(_) => core::Error::bad_request(self.to_string()),
        }
    }
}

/// Represents the list of actions allowed in the system.
pub enum ActionRequest {
    /// Creates a new sequence in the system.
    ///
    /// If the action completes successfully, a new (empty) sequence will be available.
    SequenceCreate(requests::SequenceCreate),

    /// Deletes an unlocked sequence from the system.
    SequenceDelete(requests::ResourceLocator),

    /// Creates a notification associated with a sequence.
    SequenceNotificationCreate(requests::NotificationCreate),

    /// Get all notifications for a given sequence
    SequenceNotificationList(requests::ResourceLocator),

    /// Deletes all notifications associated with a sequence
    SequenceNotificationPurge(requests::ResourceLocator),

    /// Creates a new topic in the system without any data.
    TopicCreate(requests::TopicCreate),

    /// Deletes an unlocked topic from the system.
    TopicDelete(requests::ResourceLocator),

    /// Creates a notification associated with a topic.
    TopicNotificationCreate(requests::NotificationCreate),

    /// Get all notifications for a given topic
    TopicNotificationList(requests::ResourceLocator),

    /// Deletes all notifications associated with a topic
    TopicNotificationPurge(requests::ResourceLocator),

    /// Filters a topic by ontology and timestamp range,
    /// then clusters matching timestamps by a time-gap threshold.
    TopicFilterClusterize(requests::TopicClusterizeParams),

    /// Filters multiple topics by ontology and timestamp range,
    /// then returns the intersection of their matching timestamps.
    TopicFilterIntersect(requests::TopicFilterIntersect),

    /// Creates a new upload session for the given sequence.
    SessionCreate(requests::ResourceLocator),

    /// Finalizes the upload session
    SessionFinalize(requests::SessionUuid),

    /// Deletes the selected session.
    SessionDelete(requests::ResourceLocator),

    /// Perform a query in the system
    Query(requests::Query),

    /// Returns server info: version and the server's configured limits (e.g.
    /// `max_grpc_message_size`, `target_message_size`). Replaces the former `version` action.
    Info(requests::Empty),
}

impl std::fmt::Display for ActionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SequenceCreate(_) => write!(f, "SequenceCreate"),
            Self::SequenceDelete(_) => write!(f, "SequenceDelete"),
            Self::SequenceNotificationCreate(_) => {
                write!(f, "SequenceNotificationCreate")
            }
            Self::SequenceNotificationList(_) => write!(f, "SequenceNotificationList"),
            Self::SequenceNotificationPurge(_) => write!(f, "SequenceNotificationPurge"),
            Self::TopicCreate(_) => write!(f, "TopicCreate"),
            Self::TopicDelete(_) => write!(f, "TopicDelete"),
            Self::TopicNotificationCreate(_) => write!(f, "TopicNotificationCreate"),
            Self::TopicNotificationList(_) => write!(f, "TopicNotificationList"),
            Self::TopicNotificationPurge(_) => write!(f, "TopicNotificationPurge"),
            Self::TopicFilterClusterize(_) => write!(f, "TopicFilterClusterize"),
            Self::TopicFilterIntersect(_) => write!(f, "TopicFilterIntersect"),
            Self::SessionCreate(_) => write!(f, "SessionCreate"),
            Self::SessionFinalize(_) => write!(f, "SessionFinalize"),
            Self::SessionDelete(_) => write!(f, "SessionDelete"),
            Self::Query(_) => write!(f, "Query"),
            Self::Info(_) => write!(f, "Info"),
        }
    }
}

/// Internal macro used to parse action requests
macro_rules! parse_action_req {
    ($variant:ident, $body:expr) => {
        Ok(ActionRequest::$variant(Message::decode($body)?))
    };
}

impl ActionRequest {
    pub fn try_new(value: &str, body: &[u8]) -> Result<Self, ActionError> {
        match value {
            "sequence_create" => parse_action_req!(SequenceCreate, body),
            "sequence_delete" => parse_action_req!(SequenceDelete, body),
            "sequence_notification_create" => parse_action_req!(SequenceNotificationCreate, body),
            "sequence_notification_list" => parse_action_req!(SequenceNotificationList, body),
            "sequence_notification_purge" => parse_action_req!(SequenceNotificationPurge, body),

            "topic_create" => parse_action_req!(TopicCreate, body),
            "topic_delete" => parse_action_req!(TopicDelete, body),
            "topic_notification_create" => parse_action_req!(TopicNotificationCreate, body),
            "topic_notification_list" => parse_action_req!(TopicNotificationList, body),
            "topic_notification_purge" => parse_action_req!(TopicNotificationPurge, body),
            "topic_filter_clusterize" => parse_action_req!(TopicFilterClusterize, body),
            "topic_filter_intersect" => parse_action_req!(TopicFilterIntersect, body),

            "session_create" => parse_action_req!(SessionCreate, body),
            "session_finalize" => parse_action_req!(SessionFinalize, body),
            "session_delete" => parse_action_req!(SessionDelete, body),

            "query" => parse_action_req!(Query, body),

            "info" => parse_action_req!(Info, body),

            _ => Err(ActionError::MissingAction(value.to_owned())),
        }
    }
}

/// `Action.body`/`arrow_flight::Result.body` values, as raw protobuf binary
/// (not JSON): the client already knows which `mosaicod.v1.responses`
/// message to decode from the action it sent, so unlike the previous JSON
/// envelope there's no `{"action": ..., "response": ...}` wrapper — actions
/// with no meaningful response just send zero bytes.
pub enum ActionResponse {
    SequenceCreate(()),
    SequenceDelete(()),
    SequenceNotificationCreate(()),
    SequenceNotificationPurge(()),
    SequenceNotificationList(responses::NotificationList),

    TopicCreate(responses::ResourceUuid),
    TopicDelete(()),
    TopicNotificationCreate(()),
    TopicNotificationPurge(()),
    TopicNotificationList(responses::NotificationList),
    TopicFilterClusterize(responses::TopicFilterClusterize),
    TopicFilterIntersect(responses::TopicFilterClusterize),

    /// Returns the response key associated with the session just created
    SessionCreate(responses::SessionCreate),
    SessionFinalize(()),
    SessionDelete(()),

    Query(responses::Query),

    Info(responses::ServerInfo),
}

impl ActionResponse {
    /// Encodes the action response as raw protobuf binary. Infallible:
    /// encoding a well-formed prost message cannot fail.
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Self::SequenceCreate(())
            | Self::SequenceDelete(())
            | Self::SequenceNotificationCreate(())
            | Self::SequenceNotificationPurge(())
            | Self::TopicDelete(())
            | Self::TopicNotificationCreate(())
            | Self::TopicNotificationPurge(())
            | Self::SessionFinalize(())
            | Self::SessionDelete(()) => vec![],

            Self::SequenceNotificationList(r) | Self::TopicNotificationList(r) => r.encode_to_vec(),
            Self::TopicCreate(r) => r.encode_to_vec(),
            Self::TopicFilterClusterize(r) | Self::TopicFilterIntersect(r) => r.encode_to_vec(),
            Self::SessionCreate(r) => r.encode_to_vec(),
            Self::Query(r) => r.encode_to_vec(),
            Self::Info(r) => r.encode_to_vec(),
        }
    }

    pub fn sequence_create() -> Self {
        Self::SequenceCreate(())
    }

    pub fn sequence_delete() -> Self {
        Self::SequenceDelete(())
    }

    pub fn sequence_notification_create() -> Self {
        Self::SequenceNotificationCreate(())
    }

    pub fn sequence_notification_purge() -> Self {
        Self::SequenceNotificationPurge(())
    }

    pub fn sequence_notification_list<L: core::types::Locator>(
        notifications: Vec<core::types::Notification<L>>,
    ) -> Self {
        Self::SequenceNotificationList(responses::notification_list(notifications))
    }

    pub fn topic_create(uuid: core::types::Uuid) -> Self {
        Self::TopicCreate(responses::ResourceUuid {
            uuid: uuid.to_string(),
        })
    }

    pub fn topic_delete() -> Self {
        Self::TopicDelete(())
    }

    pub fn topic_notification_create() -> Self {
        Self::TopicNotificationCreate(())
    }

    pub fn topic_notification_purge() -> Self {
        Self::TopicNotificationPurge(())
    }

    pub fn topic_notification_list<L: core::types::Locator>(
        notifications: Vec<core::types::Notification<L>>,
    ) -> Self {
        Self::TopicNotificationList(responses::notification_list(notifications))
    }

    pub fn topic_filter_clusterize(cluster: ext::arrow_filter::Cluster) -> Self {
        let response = responses::TopicFilterClusterize {
            ts: Some(timestamp_range_to_proto(cluster.timestamp_range)),
            id: cluster.id,
        };
        Self::TopicFilterClusterize(response)
    }

    pub fn topic_filter_intersect(cluster: ext::arrow_filter::Cluster) -> Self {
        let response = responses::TopicFilterClusterize {
            ts: Some(timestamp_range_to_proto(cluster.timestamp_range)),
            id: cluster.id,
        };
        Self::TopicFilterIntersect(response)
    }

    pub fn session_create(
        session_locator: core::types::SessionLocator,
        session_uuid: core::types::Uuid,
    ) -> Self {
        Self::SessionCreate(responses::SessionCreate {
            locator: session_locator.to_string(),
            uuid: session_uuid.to_string(),
        })
    }

    pub fn session_finalize() -> Self {
        Self::SessionFinalize(())
    }

    pub fn session_delete() -> Self {
        Self::SessionDelete(())
    }

    pub fn query(groups: core::types::SequenceTopicGroupSet) -> Self {
        Self::Query(responses::query_response(groups))
    }

    pub fn info(version: &str, config: responses::ServerConfig) -> Result<Self, semver::Error> {
        Ok(Self::Info(responses::server_info(version, config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::ActionRequest;
    use crate::format::{Format, format_from_i32};
    use mosaicod_core::types;
    use prost::Message;

    /// Ensure that user_metadata field in [`requests::TopicCreate`] is carried
    /// as raw bytes and can be converted to a parsable json if required.
    #[test]
    fn request_topic_create() {
        let raw = super::requests::TopicCreate {
            locator: "sequence/test_topic".to_owned(),
            session_uuid: "some_uuid".to_owned(),
            serialization_format: Format::Default as i32,
            ontology_tag: "my_sensor".to_owned(),
            user_metadata: br#"{"calibration":[0,1,2],"driver":"Jon"}"#.to_vec(),
        };

        let action = ActionRequest::try_new("topic_create", &raw.encode_to_vec())
            .expect("Problem parsing action request `topic_create`");

        if let ActionRequest::TopicCreate(action) = action {
            assert_eq!(action.locator, "sequence/test_topic");
            assert_eq!(action.session_uuid, "some_uuid");
            assert_eq!(
                format_from_i32(action.serialization_format),
                types::Format::Default
            );
            assert_eq!(action.ontology_tag, "my_sensor");

            let raw_json =
                std::str::from_utf8(&action.user_metadata).expect("Unable to get `user_metadata`");

            assert_eq!(raw_json, r#"{"calibration":[0,1,2],"driver":"Jon"}"#);
        } else {
            panic!("Wrong action request, expecting `topic_create`")
        }
    }
}
