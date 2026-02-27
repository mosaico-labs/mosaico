use mosaicod_core::types;

/// To inspect inner fields this type needs to be converted in a [`types::AuthScope`].
struct AuthScopeRecord {
    /// Unique identifier of the auth scope used as primary key.
    ///
    /// This identifier is the checksum part of the API key.
    pub(crate) api_key_checksum: String,

    /// Payload part of the API key
    pub(crate) api_key_payload: String,

    pub(crate) permissions: u8,

    /// Auth scope description
    pub description: String,

    /// UNIX timestamp in milliseconds since the creation
    pub(crate) creation_unix_timestamp: i64,

    /// UNIX timestamp in milliseconds of the expiration date
    pub(crate) expiration_unix_timestamp: Option<i64>,
}

impl TryFrom<AuthScopeRecord> for types::AuthScope {
    type Error = types::ApiKeyError;

    fn try_from(value: AuthScopeRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            key: types::ApiKey::try_from_parts(value.api_key_payload, value.api_key_checksum)?,
            permissions: value.permissions.into(),
            description: value.description,
            creation_timestamp: value.creation_unix_timestamp.into(),
            expiration_timestamp: value.expiration_unix_timestamp.map(Into::into),
        })
    }
}
