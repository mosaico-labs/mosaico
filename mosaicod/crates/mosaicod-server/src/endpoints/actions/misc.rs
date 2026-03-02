use crate::ServerError;
use log::info;
use mosaicod_core::params;
use mosaicod_marshal::ActionResponse;
use semver;

/// Returns the server version.
pub fn version() -> Result<ActionResponse, ServerError> {
    info!("requested server version");
    Ok(ActionResponse::Version(params::version().parse().map_err(
        |e: semver::Error| ServerError::InternalError(e.to_string()),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_version() {
        if let ActionResponse::Version(v) = version().unwrap() {
            println!("server version: {:?}", v);
        }
    }
}
