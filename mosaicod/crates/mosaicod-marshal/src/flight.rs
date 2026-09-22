use super::{Error, JsonMetadataBlob, metadata};
use mosaicod_core::types;
use mosaicod_core::types::TopicLocator;
use mosaicod_proto::v1::flight as proto_flight;
use prost::Message;

// ////////////////////////////////////////////////////////////////////////////
// GET FLIGHT INFO CMD
// ////////////////////////////////////////////////////////////////////////////

/// Convert a raw flight command into a [`types::flight::GetFlightInfoCmd`]
pub fn get_flight_info_cmd(v: &[u8]) -> Result<types::flight::GetFlightInfoCmd, super::Error> {
    let cmd = proto_flight::GetFlightInfoCmd::decode(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))?;

    let lb = cmd
        .timestamp_ns_start
        .map_or_else(types::Timestamp::unbounded_neg, |e| e.into());
    let up = cmd
        .timestamp_ns_end
        .map_or_else(types::Timestamp::unbounded_pos, |e| e.into());

    let mut ts_range: Option<types::TimestampRange> = None;
    if !lb.is_unbounded() || !up.is_unbounded() {
        ts_range = Some(types::TimestampRange::between(lb, up));
    }

    Ok(types::flight::GetFlightInfoCmd {
        resource_locator: cmd.resource_locator,
        timestamp_range: ts_range,
    })
}

// ////////////////////////////////////////////////////////////////////////////
// GET SCHEMA CMD
// ////////////////////////////////////////////////////////////////////////////

/// Convert a raw flight command into a [`types::flight::GetSchemaCmd`]
pub fn get_schema_cmd(v: &[u8]) -> Result<types::flight::GetSchemaCmd, super::Error> {
    let cmd = proto_flight::GetSchemaCmd::decode(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))?;

    Ok(types::flight::GetSchemaCmd {
        resource_locator: cmd.resource_locator,
    })
}

// ////////////////////////////////////////////////////////////////////////////
// DO PUT
// ////////////////////////////////////////////////////////////////////////////

pub fn do_put_cmd(v: &[u8]) -> Result<types::flight::DoPutCmd, super::Error> {
    let cmd = proto_flight::DoPutCmd::decode(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))?;

    Ok(types::flight::DoPutCmd {
        resource_locator: cmd.resource_locator,
        key: cmd.topic_uuid,
    })
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE APP METADATA
// ////////////////////////////////////////////////////////////////////////////

pub fn sequence_metadata_to_bytes(
    value: types::SequenceMetadata<JsonMetadataBlob>,
) -> Result<Vec<u8>, Error> {
    Ok(proto_flight::SequenceAppMetadata {
        created_at_ns: value.created_at.as_i64(),
        resource_locator: value.resource_locator.to_string(),
        sessions: value
            .sessions
            .into_iter()
            .map(session_metadata_to_proto)
            .collect(),
        user_metadata: metadata::user_metadata_to_bytes(value.user_metadata)
            .map_err(|e| Error::SerializationError(e.to_string()))?,
    }
    .encode_to_vec())
}

pub fn sequence_metadata_from_bytes(
    value: &[u8],
) -> Result<types::SequenceMetadata<JsonMetadataBlob>, Error> {
    let value = proto_flight::SequenceAppMetadata::decode(value)
        .map_err(|e| Error::DeserializationError(e.to_string()))?;

    let res = types::SequenceMetadata {
        created_at: value.created_at_ns.into(),
        resource_locator: value
            .resource_locator
            .parse()
            .map_err(|_| Error::DeserializationError(value.resource_locator))?,
        sessions: value
            .sessions
            .into_iter()
            .map(session_metadata_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        user_metadata: metadata::user_metadata_from_bytes(value.user_metadata)
            .map_err(|e| Error::DeserializationError(e.to_string()))?,
    };

    Ok(res)
}

/// Internal utility method used to convert [`types::SessionMetadata`] to its proto counterpart.
fn session_metadata_to_proto(value: types::SessionMetadata) -> proto_flight::SessionAppMetadata {
    proto_flight::SessionAppMetadata {
        locator: value.locator.to_string(),
        created_at_ns: value.created_at.as_i64(),
        completed_at_ns: value.completed_at.map(Into::into),
        topics: value.topics.into_iter().map(|x| x.to_string()).collect(),
        locked: value.completed_at.is_some(),
    }
}

/// Internal utility method used to obtain a [`types::SessionMetadata`] from its proto counterpart.
///
/// Used only for testing.
fn session_metadata_from_proto(
    value: proto_flight::SessionAppMetadata,
) -> Result<types::SessionMetadata, Error> {
    let locator = value
        .locator
        .parse::<types::SessionLocator>()
        .map_err(|e| Error::DeserializationError(e.to_string()))?;

    Ok(types::SessionMetadata {
        locator,
        created_at: value.created_at_ns.into(),
        completed_at: value.completed_at_ns.map(Into::into),
        topics: value
            .topics
            .into_iter()
            .map(|x| x.parse().map_err(|_| Error::DeserializationError(x)))
            .collect::<Result<Vec<TopicLocator>, _>>()?,
    })
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC APP METADATA
// ////////////////////////////////////////////////////////////////////////////

fn data_info_to_proto(value: types::TopicDataInfo) -> proto_flight::TopicAppMetadataDataInfo {
    proto_flight::TopicAppMetadataDataInfo {
        interval: if value.timestamp_range.is_unbounded() {
            None
        } else {
            Some(super::timestamp_range_to_proto(&value.timestamp_range))
        },
        total_row_count: value.total_row_count,
        total_bytes: value.total_bytes,
        total_chunks_count: value.total_chunks,
    }
}

fn data_info_from_proto(value: proto_flight::TopicAppMetadataDataInfo) -> types::TopicDataInfo {
    types::TopicDataInfo {
        total_chunks: value.total_chunks_count,
        total_bytes: value.total_bytes,
        timestamp_range: value
            .interval
            .map(|r| super::timestamp_range_from_proto(&r))
            .unwrap_or(types::TimestampRange::unbounded()),
        total_row_count: value.total_row_count,
    }
}

fn time_window_info_to_proto(
    value: types::TopicTimeWindowInfo,
) -> proto_flight::TopicAppMetadataTimeWindow {
    proto_flight::TopicAppMetadataTimeWindow {
        interval: if value.timestamp_range.is_unbounded() {
            None
        } else {
            Some(super::timestamp_range_to_proto(&value.timestamp_range))
        },
        row_count: value.row_count,
    }
}

fn time_window_info_from_proto(
    value: proto_flight::TopicAppMetadataTimeWindow,
) -> types::TopicTimeWindowInfo {
    types::TopicTimeWindowInfo {
        timestamp_range: value
            .interval
            .map(|r| super::timestamp_range_from_proto(&r))
            .unwrap_or(types::TimestampRange::unbounded()),
        row_count: value.row_count,
    }
}

pub fn topic_info_to_bytes(value: types::TopicInfo<JsonMetadataBlob>) -> Result<Vec<u8>, Error> {
    let properties = value.metadata.properties;
    let ontology_metadata = value.metadata.ontology_metadata;

    Ok(proto_flight::TopicAppMetadata {
        created_at_ns: properties.created_at.as_i64(),
        completed_at_ns: properties.completed_at.map(Into::into),
        locked: properties.completed_at.is_some(),
        resource_locator: properties.resource_locator.to_string(),
        ontology_tag: ontology_metadata.ontology_tag,
        serialization_format: super::format_to_proto(ontology_metadata.serialization_format) as i32,
        user_metadata: metadata::user_metadata_to_bytes(ontology_metadata.user_metadata)
            .map_err(|e| Error::SerializationError(e.to_string()))?,
        data_info: Some(data_info_to_proto(value.data_info)),
        time_window_info: value.time_window_info.map(time_window_info_to_proto),
        session_locator: properties.session_locator.to_string(),
    }
    .encode_to_vec())
}

pub fn topic_info_from_bytes(value: &[u8]) -> Result<types::TopicInfo<JsonMetadataBlob>, Error> {
    let value = proto_flight::TopicAppMetadata::decode(value)
        .map_err(|e| Error::DeserializationError(e.to_string()))?;

    let format =
        super::Format::try_from(value.serialization_format).unwrap_or(super::Format::Default);

    let metadata = types::TopicMetadata {
        properties: types::TopicMetadataProperties {
            created_at: value.created_at_ns.into(),
            completed_at: value.completed_at_ns.map(Into::into),
            session_locator: value
                .session_locator
                .parse()
                .map_err(|_| Error::DeserializationError(value.session_locator))?,
            resource_locator: value
                .resource_locator
                .parse()
                .map_err(|_| Error::DeserializationError(value.resource_locator))?,
        },
        ontology_metadata: types::TopicOntologyMetadata {
            serialization_format: super::format_from_proto(format),
            ontology_tag: value.ontology_tag,
            user_metadata: metadata::user_metadata_from_bytes(value.user_metadata)
                .map_err(|e| Error::DeserializationError(e.to_string()))?,
        },
    };

    Ok(types::TopicInfo {
        metadata,
        data_info: data_info_from_proto(
            value
                .data_info
                .ok_or_else(|| Error::DeserializationError("missing data_info".to_owned()))?,
        ),
        time_window_info: value.time_window_info.map(time_window_info_from_proto),
    })
}

// ////////////////////////////////////////////////////////////////////////////
// TICKET TOPIC
// ////////////////////////////////////////////////////////////////////////////

pub fn ticket_topic_to_bytes(tt: types::flight::TicketTopic) -> Vec<u8> {
    proto_flight::TicketTopic {
        resource_locator: tt.locator.to_string(),
        timestamp_ns_start: tt.timestamp_range.as_ref().map(|tsr| tsr.start.into()),
        timestamp_ns_end: tt.timestamp_range.map(|tsr| tsr.end.into()),
    }
    .encode_to_vec()
}

pub fn ticket_topic_from_bytes(v: &[u8]) -> Result<types::flight::TicketTopic, super::Error> {
    let cmd = proto_flight::TicketTopic::decode(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))?;

    let ub: types::Timestamp = cmd
        .timestamp_ns_end
        .map_or_else(types::Timestamp::unbounded_pos, |v| v.into());
    let lb: types::Timestamp = cmd
        .timestamp_ns_start
        .map_or_else(types::Timestamp::unbounded_neg, |v| v.into());

    let ts = types::TimestampRange::between(lb, ub);
    let timestamp_range = if ts.is_unbounded() { None } else { Some(ts) };

    Ok(types::flight::TicketTopic {
        locator: cmd
            .resource_locator
            .parse::<types::TopicLocator>()
            .map_err(|_| Error::DeserializationError(cmd.resource_locator))?,
        timestamp_range,
    })
}

// ////////////////////////////////////////////////////////////////////////////
// TESTS
// ////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use prost::Message;

    /// Check that decoding a [`super::proto_flight::GetFlightInfoCmd`] into
    /// [`types::flight::GetFlightInfoCmd`] is correct from a fully bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_full() {
        let cmd = super::proto_flight::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: Some(100000),
            timestamp_ns_end: Some(110000),
        };

        let dest = super::get_flight_info_cmd(&cmd.encode_to_vec()).unwrap();

        assert_eq!(dest.resource_locator, "test_sequence/topic/a");
        assert_eq!(
            dest.timestamp_range.as_ref().unwrap().start.as_i64(),
            100000
        );
        assert_eq!(dest.timestamp_range.as_ref().unwrap().end.as_i64(), 110000);
    }

    /// Check that decoding is correct from a lower bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_lb() {
        let cmd = super::proto_flight::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: Some(100000),
            timestamp_ns_end: None,
        };

        let dest = super::get_flight_info_cmd(&cmd.encode_to_vec()).unwrap();

        assert_eq!(dest.resource_locator, "test_sequence/topic/a");
        assert_eq!(
            dest.timestamp_range.as_ref().unwrap().start.as_i64(),
            100000
        );
        assert!(dest.timestamp_range.as_ref().unwrap().end.is_unbounded());
    }

    /// Check that decoding is correct from an upper bounded info message.
    #[test]
    fn get_flight_info_cmd_to_types_ub() {
        let cmd = super::proto_flight::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: None,
            timestamp_ns_end: Some(110000),
        };

        let dest = super::get_flight_info_cmd(&cmd.encode_to_vec()).unwrap();

        assert_eq!(dest.resource_locator, "test_sequence/topic/a");
        assert!(dest.timestamp_range.as_ref().unwrap().start.is_unbounded());
        assert_eq!(dest.timestamp_range.as_ref().unwrap().end.as_i64(), 110000);
    }

    /// Check that decoding is correct from a message without timestamp.
    #[test]
    fn get_flight_info_cmd_to_types_no_bounds() {
        let cmd = super::proto_flight::GetFlightInfoCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
            timestamp_ns_start: None,
            timestamp_ns_end: None,
        };

        let dest = super::get_flight_info_cmd(&cmd.encode_to_vec()).unwrap();

        assert_eq!(dest.resource_locator, "test_sequence/topic/a");
        assert!(dest.timestamp_range.is_none());
    }

    /// Check that decoding a [`super::proto_flight::GetSchemaCmd`] is correct.
    #[test]
    fn get_schema_cmd_to_types() {
        let cmd = super::proto_flight::GetSchemaCmd {
            resource_locator: "test_sequence/topic/a".to_owned(),
        };

        let dest = super::get_schema_cmd(&cmd.encode_to_vec()).unwrap();

        assert_eq!(dest.resource_locator, "test_sequence/topic/a");
    }
}
