use mosaicod_core::types;
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct ApiKey {
    pub expired: bool,
    pub description: String,
    pub permission: String,
}

impl From<types::ApiKey> for ApiKey {
    fn from(value: types::ApiKey) -> Self {
        Self {
            expired: value.is_expired(),
            description: value.description,
            permission: value.permission.into(),
        }
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(&self).unwrap_or("malformed".to_owned())
        )
    }
}
