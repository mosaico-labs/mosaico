use super::Format;
use mosaicod_core::types::{self, MetadataBlob, MetadataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type Error = MetadataError;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonMetadataBlob(serde_json::Value);

impl MetadataBlob for JsonMetadataBlob {
    fn try_to_string(&self) -> Result<String, Error> {
        Ok(serde_json::to_string(&self.0).map_err(|e| Error::SerializationError(e.to_string())))?
    }

    #[allow(refining_impl_trait)]
    fn try_from_str(v: &str) -> Result<JsonMetadataBlob, Error> {
        Ok(JsonMetadataBlob(
            serde_json::from_str(v).map_err(|e| Error::DeserializationError(e.to_string()))?,
        ))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }
}

impl From<JsonMetadataBlob> for serde_json::Value {
    fn from(value: JsonMetadataBlob) -> Self {
        value.0
    }
}

impl From<serde_json::Value> for JsonMetadataBlob {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

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

impl JsonSequenceMetadata {
    /// Converts the metadata into a flattened [`HashMap`] representation.
    pub fn to_flat_hashmap(self) -> Result<HashMap<String, String>, MetadataError> {
        Ok(HashMap::from([
            (
                "mosaico:context".to_owned(), //
                "sequence".into(),
            ),
            (
                "mosaico:user_metadata".to_owned(),
                self.user_metadata.try_to_string()?,
            ),
        ]))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicOntologyProperties {
    pub serialization_format: Format,
    pub ontology_tag: String,
}

impl From<JsonTopicOntologyProperties> for types::TopicOntologyProperties {
    fn from(value: JsonTopicOntologyProperties) -> Self {
        Self {
            ontology_tag: value.ontology_tag,
            serialization_format: value.serialization_format.into(),
        }
    }
}

impl From<types::TopicOntologyProperties> for JsonTopicOntologyProperties {
    fn from(value: types::TopicOntologyProperties) -> Self {
        Self {
            ontology_tag: value.ontology_tag,
            serialization_format: value.serialization_format.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicOntologyMetadata {
    pub properties: JsonTopicOntologyProperties,
    pub user_metadata: JsonMetadataBlob,
}

impl JsonTopicOntologyMetadata {
    pub fn to_flat_hashmap(self) -> Result<HashMap<String, String>, MetadataError> {
        Ok(HashMap::from([
            (
                "mosaico:context".to_owned(), //
                "topic".to_owned(),
            ),
            (
                "mosaico:properties".to_owned(),
                serde_json::to_string(&self.properties)
                    .map_err(|e| Error::SerializationError(e.to_string()))?,
            ),
            (
                "mosaico:user_metadata".to_owned(),
                self.user_metadata.try_to_string()?,
            ),
        ]))
    }
}

impl From<JsonTopicOntologyMetadata> for types::TopicOntologyMetadata<JsonMetadataBlob> {
    fn from(value: JsonTopicOntologyMetadata) -> Self {
        Self {
            user_metadata: Some(value.user_metadata),
            properties: value.properties.into(),
        }
    }
}

impl From<types::TopicOntologyMetadata<JsonMetadataBlob>> for JsonTopicOntologyMetadata {
    fn from(value: types::TopicOntologyMetadata<JsonMetadataBlob>) -> Self {
        Self {
            user_metadata: value
                .user_metadata
                .unwrap_or(JsonMetadataBlob(serde_json::Value::Null)),
            properties: JsonTopicOntologyProperties::from(value.properties),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicMetadata {
    pub properties: JsonTopicProperties,
    pub ontology_metadata: JsonTopicOntologyMetadata,
}

impl TryFrom<JsonTopicMetadata> for types::TopicMetadata<JsonMetadataBlob> {
    type Error = Error;

    fn try_from(v: JsonTopicMetadata) -> Result<Self, Error> {
        Ok(Self {
            ontology_metadata: v.ontology_metadata.into(),
            properties: JsonTopicProperties::try_into(v.properties)?,
        })
    }
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
    pub session_uuid: String,
    pub resource_locator: String,
}

impl TryFrom<JsonTopicProperties> for types::TopicMetadataProperties {
    type Error = Error;

    fn try_from(value: JsonTopicProperties) -> Result<Self, Error> {
        Ok(Self {
            created_at: value.created_at.into(),
            completed_at: value.completed_at.map(Into::into),
            session_uuid: value.session_uuid.parse().map_err(|_| {
                MetadataError::DeserializationError(format!(
                    "error parsing session UUID ({}) for topic {}",
                    value.session_uuid, value.resource_locator
                ))
            })?,
            resource_locator: value.resource_locator.into(),
        })
    }
}

impl From<types::TopicMetadataProperties> for JsonTopicProperties {
    fn from(value: types::TopicMetadataProperties) -> Self {
        Self {
            created_at: value.created_at.as_i64(),
            completed_at: value.completed_at.map(Into::into),
            session_uuid: value.session_uuid.to_string(),
            resource_locator: value.resource_locator.into(),
        }
    }
}
