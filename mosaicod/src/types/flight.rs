/// Message used to initiate the flight communication to upload a new datastream
pub struct DoPutCmd {
    pub resource_locator: String,
    pub key: String,
}

/// Request info on a mosaico resource (topic or sequence)
pub struct GetFlightInfoCmd {
    pub resource_locator: String,
}
