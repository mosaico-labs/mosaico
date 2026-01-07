use crate::types;
use serde::Deserialize;

/// Non-exported type for deserialize [`GetFlightInfoCmd`]
#[derive(Deserialize)]
struct GetFlightInfoCmd {
    resource_locator: String,
}

impl From<GetFlightInfoCmd> for types::flight::GetFlightInfoCmd {
    fn from(value: GetFlightInfoCmd) -> Self {
        types::flight::GetFlightInfoCmd {
            resource_locator: value.resource_locator,
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
