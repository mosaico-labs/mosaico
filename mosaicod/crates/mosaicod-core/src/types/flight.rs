use crate::types;
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

/// Request the schema of a mosaico topic
pub struct GetSchemaCmd {
    pub resource_locator: String, //(cabba) TODO: replace this with a resource locator
}

pub struct TicketTopic {
    /// Locator for the topic
    pub locator: types::TopicLocator,
    /// Optional timestamp range used to limit the data stream
    pub timestamp_range: Option<TimestampRange>,
}
