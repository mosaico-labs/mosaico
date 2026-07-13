//! Authentication hook: the extension point for pluggable API-key verification
//! and permission checks. A plugin implements [`AuthPlugin`] and registers it
//! via `PluginRegistrar::register_auth`.

use mosaicod_core::types::auth::Permissions;
use mosaicod_facade as facade;
use mosaicod_marshal::ActionRequest;

/// Why a presented credential was not accepted.
///
/// The host maps each variant to a distinct response, so a plugin can reproduce
/// the built-in behavior where a malformed token and a rejected token yield
/// different status codes.
#[derive(Debug)]
pub enum AuthError {
    /// Well-formed but rejected (unknown key, expired, revoked, ...).
    /// The host turns this into an unauthorized / permission-denied response.
    Denied,
    /// Malformed credential that could not be interpreted at all.
    /// The host turns this into a bad-request / invalid-argument response.
    Malformed,
}

#[async_trait::async_trait]
pub trait AuthPlugin: Send + Sync {
    // Init of the plugin through context (db, store, ...)
    async fn init(&mut self, ctx: facade::Context) -> Result<(), String>;

    /// Middleware calls this to verify the token. Returns the granted
    /// permissions on success, or why the credential was rejected.
    async fn verify_token(&self, token: &str) -> Result<Permissions, AuthError>;

    // Endpoint calls this to check if the key has the perms to execute the action
    fn has_permission(&self, action: &ActionRequest, perms: &Permissions) -> bool;
}
