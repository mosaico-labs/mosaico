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

#[derive(Serialize, Deserialize)]
pub struct JsonSequenceMetadata {
    pub user_metadata: JsonMetadataBlob,
}

impl From<JsonSequenceMetadata> for types::SequenceMetadata<JsonMetadataBlob> {
    fn from(value: JsonSequenceMetadata) -> Self {
        Self {
            user_metadata: value.user_metadata,
        }
    }
}

impl From<types::SequenceMetadata<JsonMetadataBlob>> for JsonSequenceMetadata {
    fn from(value: types::SequenceMetadata<JsonMetadataBlob>) -> Self {
        Self {
            user_metadata: value.user_metadata,
        }
    }
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
pub struct JsonTopicMetadata {
    pub properties: JsonTopicProperties,
    pub user_metadata: JsonMetadataBlob,
}

impl JsonTopicMetadata {
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

impl From<JsonTopicMetadata> for types::TopicMetadata<JsonMetadataBlob> {
    fn from(v: JsonTopicMetadata) -> Self {
        Self {
            user_metadata: v.user_metadata,
            properties: v.properties.into(),
        }
    }
}

impl From<types::TopicMetadata<JsonMetadataBlob>> for JsonTopicMetadata {
    fn from(value: types::TopicMetadata<JsonMetadataBlob>) -> Self {
        Self {
            user_metadata: value.user_metadata,
            properties: JsonTopicProperties::from(value.properties),
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
    pub serialization_format: Format,
    pub ontology_tag: String,
}

impl From<JsonTopicProperties> for types::TopicProperties {
    fn from(value: JsonTopicProperties) -> Self {
        Self {
            serialization_format: value.serialization_format.into(),
            ontology_tag: value.ontology_tag,
        }
    }
}

impl From<types::TopicProperties> for JsonTopicProperties {
    fn from(value: types::TopicProperties) -> Self {
        Self {
            serialization_format: value.serialization_format.into(),
            ontology_tag: value.ontology_tag,
        }
    }
}
