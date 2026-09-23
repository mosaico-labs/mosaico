use mosaicod_config::params;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{self as marshal, ActionResponse};
use tracing::info;

/// Returns the server version and configured limits (e.g. `max_grpc_message_size`).
pub fn info() -> grpc_common::Result<ActionResponse> {
    info!("requested server info");

    let params = params::params();
    let config = marshal::ServerConfig {
        max_grpc_message_size: params.max_grpc_message_size.value as u64,
        target_message_size: params.target_message_size as u64,
    };

    Ok(ActionResponse::info(&params::version(), config)
        .map_err(|e: semver::Error| grpc_common::Error::not_a_semver(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_info() {
        // `info` reads `params::params()` internally, so it must be initialized first.
        let _ = params::load_params(params::ParamsLoadOptions::testing());

        if let ActionResponse::Info(v) = info().unwrap() {
            println!("server info: {:?}", v);
        }
    }
}
