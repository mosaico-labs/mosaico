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
    #[error("body deserialization error: {0}")]
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
///          "name" : "test_sequence",
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

    /// Aborts the sequence upload process and deletes all resources associated with the sequence.
    ///
    /// This action can only be called on **unlocked** sequences.  
    /// Calling it on a **locked** sequence will result in an error.
    SequenceAbort(requests::UploadToken),

    /// Ask for system informations about the sequence
    SequenceSystemInfo(requests::ResourceLocator),

    /// Finalizes the upload of a sequence and locks it.
    ///
    /// After this action, the sequence will no longer be editable.  
    /// If there are any unlocked topics in the sequence, the action will fail.
    SequenceFinalize(requests::UploadToken),

    /// Creates a notification associated with a sequence.
    SequenceNotifyCreate(requests::NotifyCreate),

    /// Get all nofifications for a given sequence
    SequenceNotifyList(requests::ResourceLocator),

    /// Deletes all notifications associated with a sequence
    SequenceNotifyPurge(requests::ResourceLocator),

    /// Creates a new topic in the system without any data.
    TopicCreate(requests::TopicCreate),

    /// Deletes an unlocked topic from the system.
    TopicDelete(requests::ResourceLocator),

    /// Creates a notification associated with a topic.
    TopicNotifyCreate(requests::NotifyCreate),

    /// Get all nofifications for a given topic
    TopicNotifyList(requests::ResourceLocator),

    /// Deletes all notifications associated with a topic
    TopicNotifyPurge(requests::ResourceLocator),

    /// Ask for system informations about the topic
    TopicSystemInfo(requests::ResourceLocator),

    Query(requests::Query),

    /// Creates a new layer in the repository
    LayerCreate(requests::LayerCreate),

    /// Deletes an existing layer in the repository
    LayerDelete(requests::LayerDelete),

    /// Updates the name and description of an existing layer
    LayerUpdate(requests::LayerUpdate),

    /// Ask for the list of existing layers in the system
    LayerList(requests::Empty),
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
            "sequence_abort" => parse_action_req!(SequenceAbort, body),
            "sequence_finalize" => parse_action_req!(SequenceFinalize, body),
            "sequence_system_info" => parse_action_req!(SequenceSystemInfo, body),
            "sequence_notify_create" => parse_action_req!(SequenceNotifyCreate, body),
            "sequence_notify_list" => parse_action_req!(SequenceNotifyList, body),
            "sequence_notify_purge" => parse_action_req!(SequenceNotifyPurge, body),

            "topic_create" => parse_action_req!(TopicCreate, body),
            "topic_delete" => parse_action_req!(TopicDelete, body),
            "topic_system_info" => parse_action_req!(TopicSystemInfo, body),
            "topic_notify_create" => parse_action_req!(TopicNotifyCreate, body),
            "topic_notify_list" => parse_action_req!(TopicNotifyList, body),
            "topic_notify_purge" => parse_action_req!(TopicNotifyPurge, body),

            "layer_create" => parse_action_req!(LayerCreate, body),
            "layer_delete" => parse_action_req!(LayerDelete, body),
            "layer_update" => parse_action_req!(LayerUpdate, body),
            "layer_list" => parse_action_req!(LayerList, body),

            "query" => parse_action_req!(Query, body),

            _ => Err(ActionError::MissingAction(value.to_owned())),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "action", content = "response", rename_all = "snake_case")]
pub enum ActionResponse {
    SequenceCreate(responses::ResourceKey),
    SequenceSystemInfo(responses::SequenceSystemInfo),
    SequenceNotifyList(responses::NotifyList),

    TopicCreate(responses::ResourceKey),
    TopicSystemInfo(responses::TopicSystemInfo),
    TopicNotifyList(responses::NotifyList),

    LayerList(responses::LayerList),

    Query(responses::Query),

    // Empty response, no data to send
    Empty,
}

impl ActionResponse {
    /// Converts to bytes the action response
    pub fn bytes(&self) -> Result<Vec<u8>, ActionError> {
        serde_json::to_vec(self).map_err(|e| ActionError::ResponseSerializationError(e.to_string()))
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
                "name" : "test_topic",
                "sequence_key" : "some_uuid",
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
            assert_eq!(action.name, "test_topic");
            assert_eq!(action.sequence_key, "some_uuid");
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
