use mosaicod_core::types;
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct AuthorizationPolicy {
    pub expired: bool,
    pub description: String,
    pub permissions: Vec<String>,
}

impl From<types::AuthorizationPolicy> for AuthorizationPolicy {
    fn from(value: types::AuthorizationPolicy) -> Self {
        Self {
            expired: value.is_expired(),
            description: value.description,
            permissions: value.permissions.into(),
        }
    }
}

impl std::fmt::Display for AuthorizationPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(&self).unwrap_or("malformed".to_owned())
        )
    }
}
