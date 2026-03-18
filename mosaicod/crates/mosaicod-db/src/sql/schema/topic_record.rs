use crate as db;
use log::error;
use mosaicod_core::types;
use mosaicod_marshal as marshal;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct TopicRecord {
    pub topic_id: i32,
    pub(crate) topic_uuid: uuid::Uuid,
    pub(crate) locator_name: String,
    pub sequence_id: i32,
    pub session_id: i32,
    pub ontology_tag: String,

    pub(crate) locked: bool,

    pub(crate) serialization_format: String,

    /// This metadata field is only for database query access and
    /// should not be exposed
    pub(crate) user_metadata: Option<serde_json::Value>,

    /// UNIX timestamp in milliseconds from the creation
    pub(crate) creation_unix_tstamp: i64,

    /// System info.
    /// ATTENTION: They actually contain UNSIGNED int 64bit values,
    /// converted into i64 just for compatibility with SQL Bigint standard.
    pub(crate) total_bytes: Option<i64>,
    pub(crate) chunks_number: Option<i64>,

    /// First and last timestamps stored inside topic's data.
    pub(crate) start_index_timestamp: Option<i64>,
    pub(crate) end_index_timestamp: Option<i64>,
}

impl From<TopicRecord> for types::Identifiers {
    fn from(value: TopicRecord) -> Self {
        Self {
            id: value.topic_id,
            uuid: value.topic_uuid.into(),
        }
    }
}

impl TopicRecord {
    pub fn new(
        locator: &str,
        sequence_id: i32,
        session_id: i32,
        ontology_tag: &str,
        serialization_format: &str,
    ) -> Self {
        Self {
            topic_id: db::UNREGISTERED,
            topic_uuid: types::Uuid::new().into(),
            sequence_id,
            session_id,
            locator_name: locator.to_owned(),
            locked: false,
            ontology_tag: ontology_tag.to_owned(),
            serialization_format: serialization_format.to_owned(),
            user_metadata: None,
            creation_unix_tstamp: types::Timestamp::now().into(),
            chunks_number: None,
            total_bytes: None,
            start_index_timestamp: None,
            end_index_timestamp: None,
        }
    }

    pub fn with_user_metadata(mut self, user_metadata: marshal::JsonMetadataBlob) -> Self {
        self.user_metadata = Some(user_metadata.into());
        self
    }

    pub fn locked(&self) -> bool {
        self.locked
    }

    pub fn uuid(&self) -> types::Uuid {
        self.topic_uuid.into()
    }

    pub fn locator(&self) -> types::TopicResourceLocator {
        self.locator_name.clone().into()
    }

    pub fn identifiers(&self) -> types::Identifiers {
        types::Identifiers {
            uuid: self.uuid(),
            id: self.topic_id,
        }
    }

    pub fn serialization_format(&self) -> Option<types::Format> {
        types::Format::from_str(self.serialization_format.as_ref())
            .inspect_err(|e| error!("BUG: invalid serialization format in database: {}", e))
            .ok()
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    /// Either all the fields are set, or none.
    /// Mixed combinations are a symptom that something went wrong
    /// and most likely these metrics need to be recalculated.
    pub fn info(&self) -> Option<types::TopicDataInfo> {
        let info: Option<types::TopicDataInfo> = (|| {
            Some(types::TopicDataInfo {
                chunks_number: self.chunks_number? as u64,
                total_bytes: self.total_bytes? as u64,
                timestamp_range: types::TimestampRange::between(
                    self.start_index_timestamp?.into(),
                    self.end_index_timestamp?.into(),
                ),
            })
        })();

        info
    }
}
