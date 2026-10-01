use mosaicod_config::params;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{self as marshal, ActionResponse};
use tracing::info;

/// Returns the server version and the server's configured limits (e.g.
/// `grpc_max_decode_message_size`, `grpc_max_encode_message_size`,
/// `grpc_target_encode_message_size`) that clients should respect.
pub fn info() -> grpc_common::Result<ActionResponse> {
    info!("requested server info");

    let params = params::params();
    let config = marshal::ServerConfig {
        grpc_max_decode_message_size: params.grpc_max_decode_message_size.value as u64,
        grpc_max_encode_message_size: params::GRPC_MSG_MAX_SIZE_BYTES as u64,
        grpc_target_encode_message_size: params.grpc_target_encode_message_size.value as u64,
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
