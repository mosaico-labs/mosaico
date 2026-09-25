use mosaicod_config::params;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{ActionResponse, ServerConfig, ServerInfo};
use tracing::info as log_info;

/// Returns the server version and the server's configured limits (e.g.
/// `max_grpc_decode_message_size`, `max_grpc_encode_message_size`,
/// `target_grpc_encode_message_size`) that clients should respect.
pub fn info() -> grpc_common::Result<ActionResponse> {
    log_info!("requested server info");

    let params = params::params();
    let config = ServerConfig {
        max_grpc_decode_message_size: params.max_grpc_decode_message_size.value,
        max_grpc_encode_message_size: params::GRPC_MSG_MAX_SIZE_BYTES,
        target_grpc_encode_message_size: params.target_grpc_encode_message_size.value,
    };

    let server_info = ServerInfo::new(&params::version(), config)
        .map_err(|e: semver::Error| grpc_common::Error::not_a_semver(e.to_string()))?;

    Ok(ActionResponse::Info(server_info))
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
