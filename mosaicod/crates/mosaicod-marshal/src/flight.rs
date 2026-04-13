use super::Error;
use bincode::{Decode, Encode};
use mosaicod_core::types;
use mosaicod_core::types::{SessionMetadata, UuidError};
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

/// Sequence app metadata sent when requesting flight info topics and sequences flights
#[derive(Serialize, Deserialize)]
pub struct SequenceAppMetadata {
    created_at_ns: i64,
    resource_locator: String,
    sessions: Vec<SessionAppMetadata>,
}

impl<M> From<types::SequenceMetadata<M>> for SequenceAppMetadata {
    fn from(value: types::SequenceMetadata<M>) -> Self {
        Self {
            created_at_ns: value.created_at.as_i64(),
            resource_locator: value.resource_locator.into(),
            sessions: value.sessions.into_iter().map(Into::into).collect(),
        }
    }
}

impl<M> TryFrom<SequenceAppMetadata> for types::SequenceMetadata<M> {
    type Error = super::Error;

    fn try_from(value: SequenceAppMetadata) -> Result<Self, Self::Error> {
        let res = Self {
            created_at: value.created_at_ns.into(),
            resource_locator: value.resource_locator.into(),
            sessions: value
                .sessions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            user_metadata: None,
        };

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
// SESSION APP METADATA
// ////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
pub struct SessionAppMetadata {
    uuid: String,
    created_at_ns: i64,
    completed_at_ns: Option<i64>,
    topics: Vec<String>,
    locked: bool,
}

impl From<types::SessionMetadata> for SessionAppMetadata {
    fn from(value: types::SessionMetadata) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            created_at_ns: value.created_at.as_i64(),
            completed_at_ns: value.completed_at.map(Into::into),
            topics: value.topics.into_iter().map(Into::into).collect(),
            locked: value.completed_at.is_some(),
        }
    }
}

impl TryFrom<SessionAppMetadata> for types::SessionMetadata {
    type Error = super::Error;

    fn try_from(value: SessionAppMetadata) -> Result<Self, Self::Error> {
        let uuid: types::Uuid = value
            .uuid
            .parse()
            .map_err(|e: UuidError| Error::DeserializationError(e.to_string()))?;

        Ok(SessionMetadata {
            uuid,
            created_at: value.created_at_ns.into(),
            completed_at: value.completed_at_ns.map(Into::into),
            topics: value.topics.into_iter().map(Into::into).collect(),
        })
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TICKET TOPIC
// ////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode)]
struct TicketTopic {
    locator: String,
    timestamp_ns_start: Option<i64>,
    timestamp_ns_end: Option<i64>,
}

impl From<types::flight::TicketTopic> for TicketTopic {
    fn from(value: types::flight::TicketTopic) -> Self {
        Self {
            locator: value.locator,
            timestamp_ns_start: value.timestamp_range.as_ref().map(|tsr| tsr.start.into()),
            timestamp_ns_end: value.timestamp_range.map(|tsr| tsr.end.into()),
        }
    }
}

impl From<TicketTopic> for types::flight::TicketTopic {
    fn from(value: TicketTopic) -> Self {
        let ub: types::Timestamp = value
            .timestamp_ns_end
            .map_or_else(types::Timestamp::unbounded_pos, |v| v.into());
        let lb: types::Timestamp = value
            .timestamp_ns_start
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
pub struct TopicAppMetadataTimestamp {
    /// First timestamp observed in the topic
    start_ns: i64,
    /// Last timestamp observed in the topic
    end_ns: i64,
}

impl From<types::TimestampRange> for TopicAppMetadataTimestamp {
    fn from(value: types::TimestampRange) -> Self {
        Self {
            start_ns: value.start.as_i64(),
            end_ns: value.end.as_i64(),
        }
    }
}

impl From<TopicAppMetadataTimestamp> for types::TimestampRange {
    fn from(value: TopicAppMetadataTimestamp) -> Self {
        Self {
            start: value.start_ns.into(),
            end: value.end_ns.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TopicAppMetadataInfo {
    pub chunks_number: u64,
    pub total_bytes: u64,
    pub timestamp: Option<TopicAppMetadataTimestamp>,
}

/// Topic app metadata sent when requesting flight info topics and sequences flights
#[derive(Serialize, Deserialize)]
pub struct TopicAppMetadata {
    pub created_at_ns: i64,
    pub completed_at_ns: Option<i64>,
    pub locked: bool,
    pub resource_locator: String,
    pub info: Option<TopicAppMetadataInfo>,
}

impl TopicAppMetadata {
    pub fn new(metadata: types::TopicMetadataProperties) -> Self {
        Self {
            created_at_ns: metadata.created_at.as_i64(),
            completed_at_ns: metadata.completed_at.map(Into::into),
            locked: metadata.completed_at.is_some(),
            resource_locator: metadata.resource_locator.to_string(),
            info: None,
        }
    }

    pub fn with_info(mut self, info: types::TopicDataInfo) -> Self {
        self.info = Some(TopicAppMetadataInfo {
            chunks_number: info.chunks_number,
            total_bytes: info.total_bytes,
            timestamp: if info.timestamp_range.is_unbounded() {
                None
            } else {
                Some(info.timestamp_range.into())
            },
        });
        self
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
