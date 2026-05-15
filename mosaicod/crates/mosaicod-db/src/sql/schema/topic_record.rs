use crate as db;
use log::error;
use mosaicod_core::types;
use mosaicod_marshal as marshal;

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

    /// System info.
    /// ATTENTION: They actually contain UNSIGNED int 64bit values,
    /// converted into i64 just for compatibility with SQL Bigint standard.
    pub(crate) total_bytes: Option<i64>,
    pub(crate) chunks_number: Option<i64>,

    /// First and last timestamps stored inside topic's data.
    pub(crate) start_index_timestamp: Option<i64>,
    pub(crate) end_index_timestamp: Option<i64>,
}

impl TopicRecord {
    pub fn new(
        locator: types::TopicLocator,
        sequence_id: i32,
        session_id: i32,
        ontology_tag: &str,
        serialization_format: &str,
        path_in_store: Option<types::TopicPathInStore>,
    ) -> Self {
        Self {
            topic_id: db::UNREGISTERED,
            topic_uuid: types::Uuid::new().into(),
            sequence_id,
            session_id,
            locator_name: locator.to_string(),
            ontology_tag: ontology_tag.to_owned(),
            serialization_format: serialization_format.to_owned(),
            user_metadata: None,
            path_in_store: path_in_store.map(Into::into),
            creation_unix_tstamp: types::Timestamp::now().into(),
            completion_unix_tstamp: None,
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
