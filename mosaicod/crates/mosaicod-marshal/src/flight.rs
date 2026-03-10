use super::Error;
use bincode::{Decode, Encode};
use mosaicod_core::types;
use mosaicod_core::types::UuidError;
use serde::{Deserialize, Serialize};
// ////////////////////////////////////////////////////////////////////////////
// GET FLIGHT INFO CMD
// ////////////////////////////////////////////////////////////////////////////

/// Non-exported type for deserialize [`GetFlightInfoCmd`]
#[derive(Deserialize)]
struct GetFlightInfoCmd {
    resource_locator: String,
    timestamp_ns_start: Option<i64>,
    timestamp_ns_end: Option<i64>,
}

impl From<GetFlightInfoCmd> for types::flight::GetFlightInfoCmd {
    fn from(value: GetFlightInfoCmd) -> Self {
        let up = value
            .timestamp_ns_end
            .map_or_else(types::Timestamp::unbounded_pos, |e| e.into());

        let lb = value
            .timestamp_ns_start
            .map_or_else(types::Timestamp::unbounded_neg, |e| e.into());

        let mut ts_range: Option<types::TimestampRange> = None;
        if !lb.is_unbounded() || !up.is_unbounded() {
            ts_range = Some(types::TimestampRange::between(lb, up));
        }

        types::flight::GetFlightInfoCmd {
            resource_locator: value.resource_locator,
            timestamp_range: ts_range,
        }
    }
}

/// Convert a raw flight command into a [`GetFlightInfoCmd`]
pub fn get_flight_info_cmd(v: &[u8]) -> Result<types::flight::GetFlightInfoCmd, super::Error> {
    serde_json::from_slice::<GetFlightInfoCmd>(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))
        .map(|v| v.into())
}

// ////////////////////////////////////////////////////////////////////////////
// DO PUT
// ////////////////////////////////////////////////////////////////////////////
#[derive(Deserialize)]
struct DoPutCmd {
    resource_locator: String,
    topic_uuid: String,
}

impl From<DoPutCmd> for types::flight::DoPutCmd {
    fn from(value: DoPutCmd) -> Self {
        types::flight::DoPutCmd {
            resource_locator: value.resource_locator,
            key: value.topic_uuid,
        }
    }
}

pub fn do_put_cmd(v: &[u8]) -> Result<types::flight::DoPutCmd, super::Error> {
    serde_json::from_slice::<DoPutCmd>(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))
        .map(|v| v.into())
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE APP METADATA
// ////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
pub struct SessionAppMetadataTimestamps {
    created_timestamp: i64,
    completed_timestamp: i64,
}

/// For a still running session only the uuid field is filled.
/// For a closed session also the timestamps are available.
#[derive(Serialize, Deserialize)]
pub struct SessionAppMetadata {
    uuid: String,
    #[serde(flatten)]
    timestamps: Option<SessionAppMetadataTimestamps>,
    topics: Vec<String>,
}

impl From<types::SessionManifest> for SessionAppMetadata {
    fn from(value: types::SessionManifest) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            timestamps: Some(SessionAppMetadataTimestamps {
                created_timestamp: value.created_timestamp.as_i64(),
                completed_timestamp: value.completed_timestamp.as_i64(),
            }),
            topics: value.topics.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<SessionAppMetadata> for (types::Uuid, Option<types::SessionManifest>) {
    type Error = super::Error;

    fn try_from(value: SessionAppMetadata) -> Result<Self, Self::Error> {
        let uuid: types::Uuid = value
            .uuid
            .parse()
            .map_err(|e: UuidError| Error::DeserializationError(e.to_string()))?;

        let manifest = value.timestamps.map(|ts| types::SessionManifest {
            uuid: uuid.clone(),
            created_timestamp: ts.created_timestamp.into(),
            completed_timestamp: ts.completed_timestamp.into(),
            topics: value.topics.into_iter().map(Into::into).collect(),
        });

        Ok((uuid, manifest))
    }
}

/// Sequence app metadata sent when requesting flight info topics and sequences flights
#[derive(Serialize, Deserialize)]
pub struct SequenceAppMetadata {
    created_timestamp: i64,
    resource_locator: String,
    sessions: Vec<SessionAppMetadata>,
}

impl From<types::SequenceManifest> for SequenceAppMetadata {
    fn from(value: types::SequenceManifest) -> Self {
        Self {
            created_timestamp: value.created_timestamp.as_i64(),
            resource_locator: value.resource_locator.into(),
            sessions: value
                .sessions
                .into_iter()
                .map(|v| {
                    v.1.map_or_else(
                        || SessionAppMetadata {
                            uuid: v.0.to_string(),
                            timestamps: None,
                            topics: vec![],
                        },
                        |v| SessionAppMetadata {
                            uuid: v.uuid.to_string(),
                            timestamps: Some(SessionAppMetadataTimestamps {
                                created_timestamp: v.created_timestamp.as_i64(),
                                completed_timestamp: v.completed_timestamp.as_i64(),
                            }),
                            topics: v.topics.into_iter().map(Into::into).collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl TryFrom<SequenceAppMetadata> for types::SequenceManifest {
    type Error = super::Error;

    fn try_from(value: SequenceAppMetadata) -> Result<Self, Self::Error> {
        let mut res = Self {
            created_timestamp: value.created_timestamp.into(),
            resource_locator: value.resource_locator.into(),
            sessions: vec![],
        };

        for session in value.sessions {
            res.sessions.push(session.try_into()?);
        }

        Ok(res)
    }
}

impl From<SequenceAppMetadata> for bytes::Bytes {
    fn from(value: SequenceAppMetadata) -> Self {
        serde_json::to_vec(&value).unwrap_or_default().into()
    }
}

impl TryFrom<bytes::Bytes> for SequenceAppMetadata {
    type Error = Error;
    fn try_from(value: bytes::Bytes) -> Result<Self, Error> {
        serde_json::from_slice(value.as_ref())
            .map_err(|e| Error::DeserializationError(e.to_string()))
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TICKET TOPIC
// ////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode)]
struct TicketTopic {
    locator: String,
    timestamp_range_start: Option<i64>,
    timestamp_range_end: Option<i64>,
}

impl From<types::flight::TicketTopic> for TicketTopic {
    fn from(value: types::flight::TicketTopic) -> Self {
        Self {
            locator: value.locator,
            timestamp_range_start: value.timestamp_range.as_ref().map(|tsr| tsr.start.into()),
            timestamp_range_end: value.timestamp_range.map(|tsr| tsr.end.into()),
        }
    }
}

impl From<TicketTopic> for types::flight::TicketTopic {
    fn from(value: TicketTopic) -> Self {
        let ub: types::Timestamp = value
            .timestamp_range_end
            .map_or_else(types::Timestamp::unbounded_pos, |v| v.into());
        let lb: types::Timestamp = value
            .timestamp_range_start
            .map_or_else(types::Timestamp::unbounded_neg, |v| v.into());

        let ts = types::TimestampRange::between(lb, ub);

        let timestamp_range = if ts.is_unbounded() { None } else { Some(ts) };

        Self {
            locator: value.locator,
            timestamp_range,
        }
    }
}

pub fn ticket_topic_to_binary(tt: types::flight::TicketTopic) -> Result<Vec<u8>, super::Error> {
    let tt: TicketTopic = tt.into();
    let config = bincode::config::standard();

    bincode::encode_to_vec(tt, config).map_err(|e| super::Error::SerializationError(e.to_string()))
}

pub fn ticket_topic_from_binary(v: &[u8]) -> Result<types::flight::TicketTopic, super::Error> {
    let config = bincode::config::standard();

    let (ticket, _): (TicketTopic, usize) = bincode::decode_from_slice(v, config)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))?;

    Ok(ticket.into())
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC APP METADATA
// ////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
struct TopicAppMetadataTimestamp {
    /// Minimum timestamp observed in the topic
    min: i64,
    /// Maximum timestamp observed in the topic
    max: i64,
}

impl From<types::TopicManifestTimestamp> for TopicAppMetadataTimestamp {
    fn from(value: types::TopicManifestTimestamp) -> Self {
        Self {
            min: value.range.start.as_i64(),
            max: value.range.end.as_i64(),
        }
    }
}

impl From<TopicAppMetadataTimestamp> for types::TopicManifestTimestamp {
    fn from(value: TopicAppMetadataTimestamp) -> Self {
        let range = types::TimestampRange {
            start: value.min.into(),
            end: value.max.into(),
        };
        Self { range }
    }
}

#[derive(Serialize, Deserialize)]
struct TopicAppMetadataInfo {
    chunks_number: usize,
    is_locked: bool,
    total_size_bytes: usize,
    created_timestamp: i64,
}

impl From<types::TopicInfo> for TopicAppMetadataInfo {
    fn from(value: types::TopicInfo) -> Self {
        Self {
            chunks_number: value.chunks_number,
            is_locked: value.is_locked,
            total_size_bytes: value.total_size_bytes,
            created_timestamp: value.created_timestamp.as_i64(),
        }
    }
}

impl From<TopicAppMetadataInfo> for types::TopicInfo {
    fn from(value: TopicAppMetadataInfo) -> Self {
        Self {
            chunks_number: value.chunks_number,
            is_locked: value.is_locked,
            total_size_bytes: value.total_size_bytes,
            created_timestamp: value.created_timestamp.into(),
        }
    }
}

/// Topic app metadata sent when requesting flight info topics and sequences flights
#[derive(Serialize, Deserialize)]
pub struct TopicAppMetadata {
    /// Topic timestamp data
    timestamp: TopicAppMetadataTimestamp,
    info: TopicAppMetadataInfo,
}

impl From<types::TopicManifest> for TopicAppMetadata {
    fn from(value: types::TopicManifest) -> Self {
        Self {
            timestamp: value.timestamp.into(),
            info: value.info.into(),
        }
    }
}

impl From<TopicAppMetadata> for types::TopicManifest {
    fn from(value: TopicAppMetadata) -> Self {
        Self {
            timestamp: value.timestamp.into(),
            info: value.info.into(),
        }
    }
}

impl From<TopicAppMetadata> for bytes::Bytes {
    fn from(value: TopicAppMetadata) -> Self {
        serde_json::to_vec(&value).unwrap_or_default().into()
    }
}

impl TryFrom<bytes::Bytes> for TopicAppMetadata {
    type Error = Error;
    fn try_from(value: bytes::Bytes) -> Result<Self, Error> {
        serde_json::from_slice(value.as_ref())
            .map_err(|e| Error::DeserializationError(e.to_string()))
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TESTS
// ////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use mosaicod_core::types;

    /// Check that the conversion between [`super::GetFlightInfoCmd`] and
    /// [`types::flight::GetFlightInfoCmd`] is correct from a fully bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_full() {
        let src = super::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: Some(100000),
            timestamp_ns_end: Some(110000),
        };

        let name = src.resource_locator.clone();
        let start = src.timestamp_ns_start.unwrap();
        let end = src.timestamp_ns_end.unwrap();

        let dest: types::flight::GetFlightInfoCmd = src.into();

        assert_eq!(dest.resource_locator, name);
        assert_eq!(dest.timestamp_range.as_ref().unwrap().start.as_i64(), start);
        assert_eq!(dest.timestamp_range.as_ref().unwrap().end.as_i64(), end);
    }

    /// Check that the conversion between [`super::GetFlightInfoCmd`] and
    /// [`types::flight::GetFlightInfoCmd`] is correct from a lower bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_lb() {
        let src = super::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: Some(100000),
            timestamp_ns_end: None,
        };

        let name = src.resource_locator.clone();
        let start = src.timestamp_ns_start.unwrap();

        let dest: types::flight::GetFlightInfoCmd = src.into();

        assert_eq!(dest.resource_locator, name);
        assert_eq!(dest.timestamp_range.as_ref().unwrap().start.as_i64(), start);
        assert!(dest.timestamp_range.as_ref().unwrap().end.is_unbounded());
    }

    /// Check that the conversion between [`super::GetFlightInfoCmd`] and
    /// [`types::flight::GetFlightInfoCmd`] is correct from a upper bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_ub() {
        let src = super::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: None,
            timestamp_ns_end: Some(110000),
        };

        let name = src.resource_locator.clone();
        let end = src.timestamp_ns_end.unwrap();

        let dest: types::flight::GetFlightInfoCmd = src.into();

        assert_eq!(dest.resource_locator, name);
        assert!(dest.timestamp_range.as_ref().unwrap().start.is_unbounded());
        assert_eq!(dest.timestamp_range.as_ref().unwrap().end.as_i64(), end);
    }

    /// Check that the conversion between [`super::GetFlightInfoCmd`] and
    /// [`types::flight::GetFlightInfoCmd`] is correct from a message without timestamp.
    #[test]
    fn get_flight_info_cmd_to_types_no_bounds() {
        let src = super::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: None,
            timestamp_ns_end: None,
        };

        let name = src.resource_locator.clone();
        let dest: types::flight::GetFlightInfoCmd = src.into();

        assert_eq!(dest.resource_locator, name);
        assert!(dest.timestamp_range.is_none());
    }
}
