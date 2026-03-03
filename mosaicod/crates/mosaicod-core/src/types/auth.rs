use super::*;
use crc32fast::Hasher;
use std::{ops::BitOr, str::FromStr};

#[derive(thiserror::Error, Debug)]
pub enum ApiKeyError {
    #[error("the api key is incomplete")]
    IncompleteApiKey,

    #[error("bad header")]
    BadHeader,

    #[error("bad payload")]
    BadPayload,

    #[error("bad fingerprint")]
    BadFingerprint,

    #[error("bad api key length")]
    BadLength,

    #[error("fingerprint mismatch")]
    FingerprintMismatch,
}

pub type ApiKeyPayload = [u8; ApiKey::PAYLOAD_LENGTH];
pub type ApiKeyFingerprint = [u8; ApiKey::FINGERPRINT_LENGTH];

/// Mosaico API Key.
#[derive(PartialEq, Debug, Clone, Copy)]
pub struct ApiKey {
    payload: ApiKeyPayload,
    fingerprint: ApiKeyFingerprint,
}

fn compute_fingerprint(payload: &ApiKeyPayload) -> ApiKeyFingerprint {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let hash = hasher.finalize();

    let tmp = format!("{:0length$x}", hash, length = ApiKey::FINGERPRINT_LENGTH);
    let bytes = tmp.as_bytes();

    bytes.try_into().unwrap()
}

/// Perform all checks required to cast a payload string to
/// the [`Payload`] fixed size array
fn cast_payload(payload: &str) -> Result<ApiKeyPayload, ApiKeyError> {
    let payload_size_ok = payload.chars().count() == ApiKey::PAYLOAD_LENGTH;

    let payload_is_alphanumeric: bool = payload
        .chars()
        .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

    if !(payload_size_ok && payload_is_alphanumeric) {
        return Err(ApiKeyError::BadPayload);
    }

    Ok(payload.as_bytes().try_into().unwrap())
}

/// Perform all checks required to cast fingerprint string to
/// the [`Fingerprint`] fixed size array
fn cast_fingerprint(fingerprint: &str) -> Result<ApiKeyFingerprint, ApiKeyError> {
    let fingerprint_size_ok = fingerprint.chars().count() == ApiKey::FINGERPRINT_LENGTH;

    let fingerprint_is_alphanumeric: bool = fingerprint
        .chars()
        .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

    if !(fingerprint_size_ok && fingerprint_is_alphanumeric) {
        return Err(ApiKeyError::BadFingerprint);
    }

    Ok(fingerprint.as_bytes().try_into().unwrap())
}

impl ApiKey {
    /// Header included in the token
    pub const HEADER: &str = "msco";

    /// Number of characters used to generate the key payload
    const PAYLOAD_LENGTH: usize = 32;

    /// Number of characters used to store the fingerprint.
    ///
    /// The fingerprint is set to 8 characters since is also used as general
    /// api key identifier to perform actions like: list, revoke, etc
    const FINGERPRINT_LENGTH: usize = 8;

    /// Character used to separate header, payload and checksum in the API key
    const SEPARATOR: &str = "_";

    /// Generates a new random API key
    pub fn new() -> Self {
        // Use of `.unwrap()` since we are creating a string of known size with alphanumeric chars
        let payload: ApiKeyPayload = crate::random::alphanumeric(ApiKey::PAYLOAD_LENGTH)
            .to_lowercase()
            .as_bytes()
            .try_into()
            .unwrap();

        Self {
            fingerprint: compute_fingerprint(&payload),
            payload,
        }
    }

    pub fn try_from_parts(payload: &str, checksum: &str) -> Result<Self, ApiKeyError> {
        let payload = cast_payload(payload)?;
        let checksum = cast_fingerprint(checksum)?;

        Ok(Self {
            payload,
            fingerprint: checksum,
        })
    }

    pub fn from_bytes(payload: ApiKeyPayload, fingerprint: ApiKeyFingerprint) -> Self {
        Self {
            payload,
            fingerprint,
        }
    }

    pub fn fingerprint(&self) -> &str {
        std::str::from_utf8(&self.fingerprint).unwrap()
    }

    pub fn payload(&self) -> &str {
        std::str::from_utf8(&self.payload).unwrap()
    }
}

impl FromStr for ApiKey {
    type Err = ApiKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(ApiKey::SEPARATOR).collect();

        if parts.len() != 3 {
            return Err(ApiKeyError::IncompleteApiKey);
        }

        let (header, payload, checksum) = (parts[0], parts[1], parts[2]);

        if header != ApiKey::HEADER {
            return Err(ApiKeyError::BadHeader);
        }

        let payload = cast_payload(payload)?;
        let checksum = cast_fingerprint(checksum)?;

        if checksum != compute_fingerprint(&payload) {
            return Err(ApiKeyError::FingerprintMismatch);
        }

        Ok(Self {
            payload,
            fingerprint: checksum,
        })
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{header}{separator}{payload}{separator}{checksum}",
            header = ApiKey::HEADER,
            payload = std::str::from_utf8(&self.payload).unwrap(),
            checksum = std::str::from_utf8(&self.fingerprint).unwrap(),
            separator = ApiKey::SEPARATOR,
        )
    }
}

impl Default for ApiKey {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Copy, Clone, PartialEq)]
pub struct Permissions(u8);

impl Permissions {
    pub const READ: Self = Self(0b0000_0001);
    pub const WRITE: Self = Self(0b0000_0010);
    pub const DELETE: Self = Self(0b0000_0100);
    pub const MANAGE: Self = Self(0b0000_1000);

    /// Creates a new permission scope from a set of permissions.
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permissions;
    ///
    /// let perm = Permissions::new(Permissions::READ | Permissions::WRITE);
    /// ```
    pub fn new(perm: Permissions) -> Self {
        Self(perm.0)
    }

    /// Adds new permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permissions;
    ///
    /// let mut perm = Permissions::default();
    /// assert!(!perm.has(Permissions::MANAGE));
    /// perm = perm.add(Permissions::MANAGE);
    /// assert!(perm.has(Permissions::MANAGE));
    /// ```
    pub fn add(&self, permission: Permissions) -> Permissions {
        Self(self.0 | permission.0)
    }

    /// Removes permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permissions;
    ///
    /// let perm = Permissions::new(Permissions::WRITE | Permissions::READ);
    /// let perm = perm.remove(Permissions::WRITE);
    /// assert!(!perm.has(Permissions::WRITE));
    /// ```
    pub fn remove(&self, permission: Permissions) -> Permissions {
        Self(self.0 & !permission.0)
    }

    /// Checks if the current permission has the `target` permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permissions;
    ///
    /// let perm = Permissions::new(Permissions::READ | Permissions::WRITE);
    /// assert!(perm.has(Permissions::READ));
    /// assert!(perm.has(Permissions::WRITE));
    /// assert!(!perm.has(Permissions::MANAGE));
    /// ```
    pub fn has(&self, target: Permissions) -> bool {
        target.0 & self.0 == target.0
    }

    /// Check if the current permission is empty (i.e. has no permissions set)
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permissions;
    ///
    /// let perm = Permissions::default();
    /// assert!(perm.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Returns the permissions as 1 byte
    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl From<u8> for Permissions {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<Permissions> for u8 {
    fn from(value: Permissions) -> Self {
        value.0
    }
}

/// Convert a permissions into a vector of strings
/// like `["read", "write" ...]`
impl From<Permissions> for Vec<String> {
    fn from(value: Permissions) -> Self {
        let mut vec: Vec<String> = Vec::new();
        if value.has(Permissions::READ) {
            vec.push("read".to_owned());
        }

        if value.has(Permissions::WRITE) {
            vec.push("write".to_owned());
        }

        if value.has(Permissions::DELETE) {
            vec.push("delete".to_owned());
        }

        if value.has(Permissions::MANAGE) {
            vec.push("manage".to_owned());
        }
        vec
    }
}

impl std::fmt::Debug for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Permissions({:08b})", self.0)
    }
}

impl Default for Permissions {
    /// Returns an empty permission
    fn default() -> Self {
        Self(0)
    }
}

impl BitOr for Permissions {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(rhs.0 | self.0)
    }
}

/// Represent an authorization policy.
///
/// The policy is composed of:
/// * an API Key like `msco:0938n8b37r378brf`
/// * the associated permissions (like: read, write, ..)
/// * a description to keep track of the purpose of the key
/// * an optional expire date
#[derive(Clone)]
pub struct AuthorizationPolicy {
    pub key: ApiKey,

    /// Permissions associated with the scope
    pub permissions: Permissions,

    /// Description to keep track of the purpose of the key
    pub description: String,

    /// Creation timestamp
    pub creation_timestamp: Timestamp,

    /// Expiration timestamp
    pub expiration_timestamp: Option<Timestamp>,
}

impl AuthorizationPolicy {
    /// Create a new API key scope
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::{AuthorizationPolicy, Permissions};
    ///
    /// // Single permission
    /// let policy = AuthorizationPolicy::new(Permissions::READ, "dummy key".to_owned(), None);
    ///
    /// // Multiple permissions
    /// let policy = AuthorizationPolicy::new(
    ///     Permissions::READ | Permissions::WRITE,
    ///     "dummy key".to_owned(),
    ///     None
    /// );
    pub fn new(
        permission: Permissions,
        description: String,
        expires: Option<std::time::Duration>,
    ) -> Self {
        Self {
            key: ApiKey::new(),
            permissions: permission,
            creation_timestamp: Timestamp::now(),
            expiration_timestamp: expires.map(|delta| Timestamp::now() + delta),
            description,
        }
    }

    /// Get the API key associated with this authorization policy
    pub fn key(&self) -> &ApiKey {
        &self.key
    }

    /// Check if the policy is expired
    pub fn is_expired(&self) -> bool {
        if let Some(ts) = self.expiration_timestamp {
            return ts <= Timestamp::now();
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissions() {
        let perm = Permissions::new(Permissions::READ | Permissions::WRITE);

        assert!(perm.has(Permissions::READ | Permissions::WRITE));
        assert!(perm.has(Permissions::READ));
        assert!(perm.has(Permissions::WRITE));

        let perm = Permissions::new(Permissions::MANAGE);
        assert!(perm.has(Permissions::MANAGE));
        assert!(!perm.has(Permissions::READ));
        assert!(!perm.has(Permissions::WRITE));
        assert!(!perm.has(Permissions::DELETE));

        let mut perm = Permissions::new(Permissions::READ | Permissions::WRITE);
        perm = perm.add(Permissions::MANAGE);
        assert!(perm.has(Permissions::READ | Permissions::WRITE | Permissions::MANAGE),);
    }

    #[test]
    fn api_key_create_and_parse() {
        let key = ApiKey::new();
        dbg!(&key.to_string());

        let key_str = key.to_string();

        let _: ApiKey = key_str.parse().expect("Error parsing API key");
    }

    #[test]
    fn bad_apy_key() {
        let res: Result<ApiKey, ApiKeyError> =
            "mosaico_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadHeader)));

        // Removed char in payload
        let res: Result<ApiKey, ApiKeyError> =
            "msco_rfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadPayload)));

        // added non ascii char in fingerprint
        let res: Result<ApiKey, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_©b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadFingerprint)));

        // Removed char from fingerprint
        let res: Result<ApiKey, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadFingerprint)));
    }
}
