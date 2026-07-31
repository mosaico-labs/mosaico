use mosaicod_core::params;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::ActionResponse;
use semver;
use tracing::info;

/// Returns the server version.
pub fn version() -> grpc_common::Result<ActionResponse> {
    info!("requested server version");
    Ok(ActionResponse::Version(params::version().parse().map_err(
        |e: semver::Error| grpc_common::Error::not_a_semver(e.to_string()),
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
