use mosaicod_core::types;

/// To inspect inner fields this type needs to be converted in a [`types::AuthorizationPolicy`].
pub struct AuthzPolicyRecord {
    /// Unique identifier of the authorization policy used as primary key.
    ///
    /// This identifier is the fingerprint part of the API key.
    pub(crate) api_key_fingerprint: Vec<u8>,

    /// Payload part of the API key
    pub(crate) api_key_payload: Vec<u8>,

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

impl TryFrom<AuthzPolicyRecord> for types::AuthorizationPolicy {
    type Error = types::ApiKeyError;

    fn try_from(value: AuthzPolicyRecord) -> Result<Self, Self::Error> {
        let payload: types::ApiKeyPayload = value
            .api_key_payload
            .try_into()
            .map_err(|_| types::ApiKeyError::BadPayload)?;

        let fingerprint: types::ApiKeyFingerprint = value
            .api_key_fingerprint
            .try_into()
            .map_err(|_| types::ApiKeyError::BadFingerprint)?;

        Ok(Self {
            key: types::ApiKey::from_bytes(payload, fingerprint),
            permissions: (value.permissions as u8).into(),
            description: value.description,
            creation_timestamp: value.creation_unix_timestamp.into(),
            expiration_timestamp: value.expiration_unix_timestamp.map(Into::into),
        })
    }
}

impl From<types::AuthorizationPolicy> for AuthzPolicyRecord {
    fn from(value: types::AuthorizationPolicy) -> Self {
        Self {
            api_key_fingerprint: value.key().fingerprint().as_bytes().into(),
            api_key_payload: value.key().payload().as_bytes().into(),
            permissions: value.permissions.as_u8() as i16,
            description: value.description,
            creation_unix_timestamp: value.creation_timestamp.into(),
            expiration_unix_timestamp: value.expiration_timestamp.map(|v| v.into()),
        }
    }
}
