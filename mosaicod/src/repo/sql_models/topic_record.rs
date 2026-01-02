use std::str::FromStr;

use crate::{marshal, repo, rw, types};

#[derive(Debug, Clone)]
pub struct TopicRecord {
    pub topic_id: i32,
    pub topic_uuid: uuid::Uuid,
    pub locator_name: String,
    pub sequence_id: i32,
    pub ontology_tag: Option<String>,

    pub(super) locked: bool,
    pub(super) serialization_format: Option<String>,

    /// This metadata field is only for database query access and
    /// should not be exposed
    pub(super) user_metadata: Option<serde_json::Value>,

    /// UNIX timestamp in milliseconds from the creation
    pub(super) creation_unix_tstamp: i64,
}

impl From<TopicRecord> for types::ResourceId {
    fn from(value: TopicRecord) -> Self {
        Self {
            id: value.topic_id,
            uuid: value.topic_uuid,
        }
    }
}

impl TopicRecord {
    pub fn new(name: &str, sequence_id: i32) -> Self {
        Self {
            topic_id: repo::UNREGISTERED,
            topic_uuid: uuid::Uuid::new_v4(),
            sequence_id,
            locator_name: name.to_owned(),
            locked: false,
            ontology_tag: None,
            serialization_format: None,
            user_metadata: None,
            creation_unix_tstamp: types::Timestamp::now().into(),
        }
    }

    pub fn with_ontology_tag(mut self, ontology_tag: &str) -> Self {
        self.ontology_tag = Some(ontology_tag.to_owned());
        self
    }

    pub fn with_serialization_format(mut self, serialization_format: &str) -> Self {
        self.serialization_format = Some(serialization_format.to_owned());
        self
    }

    pub fn with_user_metadata(mut self, user_metadata: marshal::JsonMetadataBlob) -> Self {
        self.user_metadata = Some(user_metadata.into());
        self
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn serialization_format(&self) -> Option<rw::Format> {
        self.serialization_format.as_ref().map(|value| {
            rw::Format::from_str(value).expect("BUG: invalid serialization format in database")
        })
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }
}
