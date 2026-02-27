use mosaicod_core::types;
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct AuthScope {
    pub expired: bool,
    pub description: String,
    pub permissions: Vec<String>,
}

impl From<types::AuthScope> for AuthScope {
    fn from(value: types::AuthScope) -> Self {
        Self {
            expired: value.is_expired(),
            description: value.description,
            permissions: value.permissions.into(),
        }
    }
}

impl std::fmt::Display for AuthScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(&self).unwrap_or("malformed".to_owned())
        )
    }
}
