use super::{requests, responses};
use serde::Serialize;
use thiserror::Error;

/// Represents possible errors that can occur while handling an [`ActionRequest`].
#[derive(Error, Debug)]
pub enum ActionError {
    /// No matching action found for the provided string.
    ///
    /// The string that failed to match is included in the error.
    #[error("no available action for string `{0}`")]
    MissingAction(String),

    /// Failed to deserialize the request body.
    #[error("body deserialization error")]
    BodyDeserializationError(#[from] serde_json::Error),

    /// Failed to serialize the response.
    #[error("response serialization error: {0}")]
    ResponseSerializationError(String),
}

/// Represents the list of actions allowed in the system.
///
/// ### Usage Example
/// ```rust
///   use mosaicod_marshal::ActionRequest;
///
///   let raw = r#"
///    {
///          "locator" : "test_sequence",
///          "user_metadata" : {
///              "calibration" : [0, 1, 2],
///              "driver" : "jon"
///          }
///      }
///  "#;
///  let action = ActionRequest::try_new("sequence_create", raw.as_bytes()).unwrap();
/// ```
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

    /// Creates a new upload session for a sequence
    SessionCreate(requests::ResourceLocator),

    /// Finalizes the upload session
    SessionFinalize(requests::SessionUuid),

    /// Aborts the session upload process and deletes all resources associated with the session.
    ///
    /// This action can only be called on **unlocked** sessions.  
    /// Calling it on a **locked** session will result in an error.
    SessionAbort(requests::SessionUuid),

    /// Deletes the selected session.
    SessionDelete(requests::SessionUuid),

    /// Perform a query in the system
    Query(requests::Query),

    /// Creates a new layer in the database
    LayerCreate(requests::LayerCreate),

    /// Deletes an existing layer in the database
    LayerDelete(requests::LayerDelete),

    /// Updates the name and description of an existing layer
    LayerUpdate(requests::LayerUpdate),

    /// Ask for the list of existing layers in the system
    LayerList(requests::Empty),

    /// Ask to create a new api key with given permissions and duration.
    ApiKeyCreate(requests::ApiKeyCreate),

    /// Ask for the status of the given api key (specified using its fingerprint).
    ApiKeyStatus(requests::ApiKeyFingerprint),

    Version(requests::Empty),
}

/// Internal macro used to parse action requests
macro_rules! parse_action_req {
    ($variant:ident, $body:expr) => {
        Ok(ActionRequest::$variant(serde_json::from_slice($body)?))
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

            "session_create" => parse_action_req!(SessionCreate, body),
            "session_finalize" => parse_action_req!(SessionFinalize, body),
            "session_abort" => parse_action_req!(SessionAbort, body),
            "session_delete" => parse_action_req!(SessionDelete, body),

            "layer_create" => parse_action_req!(LayerCreate, body),
            "layer_delete" => parse_action_req!(LayerDelete, body),
            "layer_update" => parse_action_req!(LayerUpdate, body),
            "layer_list" => parse_action_req!(LayerList, body),

            "query" => parse_action_req!(Query, body),

            "api_key_create" => parse_action_req!(ApiKeyCreate, body),
            "api_key_status" => parse_action_req!(ApiKeyStatus, body),

            "version" => parse_action_req!(Version, body),

            _ => Err(ActionError::MissingAction(value.to_owned())),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "action", content = "response", rename_all = "snake_case")]
pub enum ActionResponse {
    SequenceCreate(()),
    SequenceDelete(()),
    SequenceNotificationCreate(()),
    SequenceNotificationPurge(()),
    SequenceNotificationList(responses::NotificationList),

    TopicCreate(responses::ResourceUuid),
    TopicNotificationList(responses::NotificationList),

    /// Returns the response key associated with the session just created
    SessionCreate(responses::ResourceUuid),
    SessionFinalize(()),
    SessionAbort(()),
    SessionDelete(()),

    /// Returns the list of layers
    LayerList(responses::LayerList),

    Query(responses::Query),

    ApiKeyCreate(responses::ApiKeyToken),
    ApiKeyStatus(responses::ApiKeyStatus),

    Version(responses::ServerVersion),

    // Empty response, no data to send
    Empty,
}

impl ActionResponse {
    /// Converts to bytes the action response
    pub fn bytes(&self) -> Result<Vec<u8>, ActionError> {
        serde_json::to_vec(self).map_err(|e| ActionError::ResponseSerializationError(e.to_string()))
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

    pub fn sequence_notification_list(response: responses::NotificationList) -> Self {
        Self::SequenceNotificationList(response)
    }

    pub fn session_create(response: responses::ResourceUuid) -> Self {
        Self::SessionCreate(response)
    }

    pub fn session_finalize() -> Self {
        Self::SessionFinalize(())
    }

    pub fn session_abort() -> Self {
        Self::SessionAbort(())
    }

    pub fn session_delete() -> Self {
        Self::SessionDelete(())
    }

    pub fn api_key_create(response: responses::ApiKeyToken) -> Self {
        Self::ApiKeyCreate(response)
    }

    pub fn api_key_status(response: responses::ApiKeyStatus) -> Self {
        Self::ApiKeyStatus(response)
    }
}

#[cfg(test)]
mod tests {
    use super::ActionRequest;
    use crate::Format;
    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    struct DecodedMetadata {
        calibration: Vec<i32>,
        driver: String,
    }

    /// Ensure that user_metadata field in [`RequestTopicCreate`] is serialized
    /// correctly as a string and can be converted to a parsable json if required.
    #[test]
    fn request_topic_create() {
        let raw = r#"
            {
                "locator" : "sequence/test_topic",
                "session_uuid" : "some_uuid",
                "serialization_format" : "default",
                "ontology_tag" : "my_sensor",
                "user_metadata" : {
                    "calibration" : [0, 1, 2],
                    "driver" : "jon"
                }
            } 
        "#;

        let action = ActionRequest::try_new("topic_create", raw.as_bytes())
            .expect("Problem parsing action request `topic_create`");

        if let ActionRequest::TopicCreate(action) = action {
            assert_eq!(action.locator, "sequence/test_topic");
            assert_eq!(action.session_uuid, "some_uuid");
            assert_eq!(action.serialization_format, Format::Default);
            assert_eq!(action.ontology_tag, "my_sensor");
            let raw_json = action
                .user_metadata()
                .expect("Unable to get `user_metadata`");

            let decoded_metadata: DecodedMetadata =
                serde_json::from_str(&raw_json).expect("Unable to convert `user_metadata` to json");

            assert_eq!(decoded_metadata.calibration, [0, 1, 2]);
            assert_eq!(decoded_metadata.driver, "jon");
        } else {
            panic!("Wrong action request, expecting `topic_create`")
        }
    }
}
