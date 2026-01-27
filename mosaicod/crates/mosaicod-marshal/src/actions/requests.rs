use super::ActionError;
use crate::Format;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Empty {}

/// Specialized message used to create a new sequence in the platform
#[derive(Deserialize, Debug)]
pub struct SequenceCreate {
    pub name: String,
    user_metadata: serde_json::Value,
}

impl SequenceCreate {
    pub fn user_metadata(&self) -> Result<String, ActionError> {
        Ok(serde_json::to_string(&self.user_metadata)?)
    }
}

/// Specialized message used to create a new sequence in the platform
#[derive(Deserialize, Debug)]
pub struct TopicCreate {
    pub name: String,
    pub sequence_key: String,
    pub serialization_format: Format,
    pub ontology_tag: String,

    user_metadata: serde_json::Value,
}

impl TopicCreate {
    pub fn user_metadata(&self) -> Result<String, ActionError> {
        Ok(serde_json::to_string(&self.user_metadata)?)
    }
}

/// Request used to locate a specific resource by name.
#[derive(Deserialize, Debug)]
pub struct ResourceLocator {
    pub name: String,
}

/// Request used to locate a resource deterministically,
/// typically by combining the resource name and a unique key.
/// Used for topics, sequences, or other keyed resources.
#[derive(Deserialize, Debug)]
pub struct UploadToken {
    pub name: String,
    pub key: String,
}

/// Generic request message used to create nofifications
#[derive(Deserialize, Debug)]
pub struct NotifyCreate {
    pub name: String,
    pub notify_type: String,
    pub msg: String,
}

/// Creates a new layer
#[derive(Deserialize, Debug)]
pub struct LayerCreate {
    pub name: String,
    pub description: String,
}

/// Delete an existing layer identified by `name`
#[derive(Deserialize, Debug)]
pub struct LayerDelete {
    pub name: String,
}

/// Update `name` and `description` on an existing layer
#[derive(Deserialize, Debug)]
pub struct LayerUpdate {
    pub prev_name: String,
    pub curr_name: String,
    pub curr_description: String,
}

#[derive(Deserialize, Debug)]
pub struct Query {
    #[serde(flatten)]
    /// Query filter used to find matches in the system
    pub query: serde_json::Value,
}
