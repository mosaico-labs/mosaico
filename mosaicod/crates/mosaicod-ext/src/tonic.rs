use std::fs;

use tonic::transport::{Identity, ServerTlsConfig};

pub fn load_tls_config(
    tls_certificate_file: impl AsRef<std::path::Path>,
    tls_private_key_file: impl AsRef<std::path::Path>,
) -> Result<ServerTlsConfig, String> {
    let cert = fs::read_to_string(tls_certificate_file)
        .map_err(|e| format!("Unable to read certificate. error: {}", e))?;
    let key = fs::read_to_string(tls_private_key_file)
        .map_err(|e| format!("Unable to read private key. error: {}", e))?;

    let identity = Identity::from_pem(cert, key);
    Ok(ServerTlsConfig::new().identity(identity))
}
