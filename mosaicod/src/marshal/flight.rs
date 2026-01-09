use crate::types;
use bincode::{Decode, Encode};
use serde::Deserialize;

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

#[derive(Deserialize)]
struct DoPutCmd {
    resource_locator: String,
    key: String,
}

impl From<DoPutCmd> for types::flight::DoPutCmd {
    fn from(value: DoPutCmd) -> Self {
        types::flight::DoPutCmd {
            resource_locator: value.resource_locator,
            key: value.key,
        }
    }
}

pub fn do_put_cmd(v: &[u8]) -> Result<types::flight::DoPutCmd, super::Error> {
    serde_json::from_slice::<DoPutCmd>(v)
        .map_err(|e| super::Error::DeserializationError(e.to_string()))
        .map(|v| v.into())
}

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
