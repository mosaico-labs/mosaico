//! One-directional JSON projections of [`types::SequenceMetadata`] and
//! [`types::TopicMetadata`], used only to write the on-disk `metadata.json`
//! sidecar files.

use super::{Format, JsonMetadataBlob};
use mosaicod_core::types;
use serde::{Deserialize, Serialize};

type Error = types::MetadataError;

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE
// ////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
pub struct JsonSequenceMetadata {
    pub user_metadata: JsonMetadataBlob,
}

impl TryFrom<Vec<u8>> for JsonSequenceMetadata {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(serde_json::from_slice(&bytes).map_err(|e| Error::DeserializationError(e.to_string())))?
    }
}

impl TryInto<Vec<u8>> for JsonSequenceMetadata {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC
// ////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicOntologyMetadata {
    pub serialization_format: Format,
    pub ontology_tag: String,
    pub user_metadata: JsonMetadataBlob,
}

impl From<types::TopicOntologyMetadata<JsonMetadataBlob>> for JsonTopicOntologyMetadata {
    fn from(value: types::TopicOntologyMetadata<JsonMetadataBlob>) -> Self {
        Self {
            user_metadata: value
                .user_metadata
                .unwrap_or(JsonMetadataBlob::from(serde_json::Value::Null)),
            ontology_tag: value.ontology_tag,
            serialization_format: super::format_to_proto(value.serialization_format),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicMetadata {
    pub properties: JsonTopicProperties,
    pub ontology_metadata: JsonTopicOntologyMetadata,
}

impl From<types::TopicMetadata<JsonMetadataBlob>> for JsonTopicMetadata {
    fn from(value: types::TopicMetadata<JsonMetadataBlob>) -> Self {
        Self {
            ontology_metadata: value.ontology_metadata.into(),
            properties: value.properties.into(),
        }
    }
}

impl TryFrom<Vec<u8>> for JsonTopicMetadata {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(serde_json::from_slice(&bytes).map_err(|e| Error::DeserializationError(e.to_string())))?
    }
}

impl TryInto<Vec<u8>> for JsonTopicMetadata {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicProperties {
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub session_locator: String,
    pub resource_locator: String,
}

impl From<types::TopicMetadataProperties> for JsonTopicProperties {
    fn from(value: types::TopicMetadataProperties) -> Self {
        Self {
            created_at: value.created_at.as_i64(),
            completed_at: value.completed_at.map(Into::into),
            session_locator: value.session_locator.to_string(),
            resource_locator: value.resource_locator.to_string(),
        }
    }
}
