use crate::types::TimestampRange;

/// Message used to initiate the flight communication to upload a new datastream
pub struct DoPutCmd {
    pub resource_locator: String, //(cabba) TODO: replace this with a resource locator
    pub key: String,
}

/// Request info on a mosaico resource (topic or sequence)
pub struct GetFlightInfoCmd {
    pub resource_locator: String, //(cabba) TODO: replace this with a resource locator
    pub timestamp_range: Option<TimestampRange>,
}

pub struct TicketTopic {
    /// Locator for the topic
    pub locator: String, //(cabba) TODO: replace this with a resource locator
    /// Optional timestamp range used to limit the data stream
    pub timestamp_range: Option<TimestampRange>,
}
