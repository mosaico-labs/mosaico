use mosaicod_core::types;
use mosaicod_marshal as marshal;
use tracing::error;

#[derive(Debug, Clone)]
pub struct TopicRecord {
    pub topic_id: i32,
    pub(crate) topic_uuid: uuid::Uuid,
    pub(crate) locator_name: String,
    pub sequence_id: i32,
    pub session_id: i32,
    pub ontology_tag: String,

    pub(crate) serialization_format: String,

    // Do not expose directly this field
    pub(crate) user_metadata: Option<serde_json::Value>,

    /// Path inside Object store where to find data and backup files.
    pub(crate) path_in_store: Option<String>,

    /// UNIX timestamp in milliseconds from the creation
    pub(crate) creation_unix_tstamp: i64,
    pub(crate) completion_unix_tstamp: Option<i64>,

    /// First and last timestamps stored inside topic's data.
    pub(crate) start_index_timestamp: Option<i64>,
    pub(crate) end_index_timestamp: Option<i64>,

    pub(crate) optimization_end_unix_tstamp: Option<i64>,
}

impl TopicRecord {
    pub fn uuid(&self) -> types::Uuid {
        self.topic_uuid.into()
    }

    /// Returns the resource locator for this topic.
    ///
    /// Because a [`TopicRecord`] should only be created using [`TopicRecord::new`], that requires a [`types::TopicLocator`],
    /// we can assume the locator value inside the DB is always valid. It should panic only if somebody
    /// changed it manually directly inside the database.
    pub fn locator(&self) -> types::TopicLocator {
        self.locator_name
            .parse()
            .unwrap_or_else(|_| panic!("Invalid topic locator in DB {}", self.locator_name))
    }

    pub fn path_in_store(&self) -> Option<types::TopicPathInStore> {
        self.path_in_store.clone().map(Into::into)
    }

    pub fn serialization_format(&self) -> Option<types::Format> {
        self.serialization_format
            .parse()
            .inspect_err(|e| error!("BUG: invalid serialization format in database: {}", e))
            .ok()
    }

    pub fn user_metadata(&self) -> Option<marshal::JsonMetadataBlob> {
        self.user_metadata.clone().map(Into::into)
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    pub fn completion_timestamp(&self) -> Option<types::Timestamp> {
        self.completion_unix_tstamp.map(|ts| ts.into())
    }

    pub fn optimization_end_timestamp(&self) -> Option<types::Timestamp> {
        self.optimization_end_unix_tstamp.map(|ts| ts.into())
    }

    /// Returns first and last timestamps stored inside topic's data.
    pub fn timestamp_range(&self) -> Option<types::TimestampRange> {
        Some(types::TimestampRange::between(
            self.start_index_timestamp?.into(),
            self.end_index_timestamp?.into(),
        ))
    }
}
