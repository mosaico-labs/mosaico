use mosaicod_core::types;

/// To inspect inner fields this type needs to be converted in a [`types::AuthorizationPolicy`].
pub struct ApiKeyRecord {
    /// Unique identifier of the authorization policy used as primary key.
    ///
    /// This identifier is the fingerprint part of the API key.
    pub(crate) fingerprint: Vec<u8>,

    /// Payload part of the API key
    pub(crate) payload: Vec<u8>,

    /// Permissions are stored as 2 bytes (for future usages, and since some modern database
    /// system have no support for 1 byte words)
    pub(crate) permissions: i16,

    /// Authorization policy description
    pub description: String,

    /// UNIX timestamp in milliseconds since the creation
    pub(crate) creation_unix_timestamp: i64,

    /// UNIX timestamp in milliseconds of the expiration date
    pub(crate) expiration_unix_timestamp: Option<i64>,
}

impl TryFrom<ApiKeyRecord> for types::ApiKey {
    type Error = types::auth::ApiKeyError;

    fn try_from(value: ApiKeyRecord) -> Result<Self, Self::Error> {
        let payload: types::auth::TokenPayload = value
            .payload
            .try_into()
            .map_err(|_| types::auth::ApiKeyError::BadTokenPayload)?;

        let fingerprint: types::auth::TokenFingerprint = value
            .fingerprint
            .try_into()
            .map_err(|_| types::auth::ApiKeyError::BadTokenFingerprint)?;

        Ok(Self {
            key: types::auth::Token::from_bytes(payload, fingerprint),
            permissions: (value.permissions as u8).into(),
            description: value.description,
            creation_timestamp: value.creation_unix_timestamp.into(),
            expiration_timestamp: value.expiration_unix_timestamp.map(Into::into),
        })
    }
}

impl From<types::ApiKey> for ApiKeyRecord {
    fn from(value: types::ApiKey) -> Self {
        Self {
            fingerprint: value.token().fingerprint().as_bytes().into(),
            payload: value.token().payload().as_bytes().into(),
            permissions: value.permissions.as_u8() as i16,
            description: value.description,
            creation_unix_timestamp: value.creation_timestamp.into(),
            expiration_unix_timestamp: value.expiration_timestamp.map(|v| v.into()),
        }
    }
}
