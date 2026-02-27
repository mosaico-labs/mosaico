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

    #[error("fingerprint mismatch")]
    ChecksumMismatch,
}

type Payload = [u8; ApiKey::PAYLOAD_LENGTH];
type Fingerprint = [u8; ApiKey::FINGERPRINT_LENGTH];

/// Mosaico API Key.
#[derive(PartialEq, Debug)]
pub struct ApiKey {
    payload: Payload,
    fingerprint: Fingerprint,
}

fn compute_fingerprint(payload: &Payload) -> Fingerprint {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let hash = hasher.finalize();

    format!("{:04x}", hash % 0xFFFF)
        .as_bytes()
        .try_into()
        .unwrap()
}

/// Perform all checks required to cast a payload string to
/// the [`Payload`] fixed size array
fn cast_payload(payload: &str) -> Result<Payload, ApiKeyError> {
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
fn cast_fingerprint(fingerprint: &str) -> Result<Fingerprint, ApiKeyError> {
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
        let payload: Payload = crate::random::alphanumeric(ApiKey::PAYLOAD_LENGTH)
            .to_lowercase()
            .as_bytes()
            .try_into()
            .unwrap();

        Self {
            fingerprint: compute_fingerprint(&payload),
            payload: payload,
        }
    }

    pub fn try_from_parts(payload: String, checksum: String) -> Result<Self, ApiKeyError> {
        let payload = cast_payload(&payload)?;
        let checksum = cast_fingerprint(&checksum)?;

        Ok(Self {
            payload,
            fingerprint: checksum,
        })
    }

    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
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
            return Err(ApiKeyError::ChecksumMismatch);
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

#[derive(PartialEq)]
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
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::READ | Permission::WRITE);
    /// }
    /// ```
    pub fn new(perm: Permissions) -> Self {
        Self(perm.0)
    }

    /// Adds new permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let mut perm = Permission::default();
    ///     assert!(!perm.has(Permission::MANAGE));
    ///     perm = perm.add(Permission::MANAGE);
    ///     assert!(perm.has(Permission::MANAGE));
    /// }
    /// ```
    pub fn add(&self, permission: Permissions) -> Permissions {
        Self(self.0 | permission.0)
    }

    /// Removes permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::WRITE | Permission::READ);
    ///     let perm = perm.remove(Permission::WRITE);
    ///     assert!(!perm.has(Permission::WRITE));
    /// }
    /// ```
    pub fn remove(&self, permission: Permissions) -> Permissions {
        Self(self.0 & !permission.0)
    }

    /// Checks if the current permission has the `target` permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::READ | Permission::WRITE);
    ///     assert!(perm.has(Permission::READ));
    ///     assert!(perm.has(Permission::WRITE));
    ///     assert!(!perm.has(Permission::MANAGE));
    /// }
    /// ```
    pub fn has(&self, target: Permissions) -> bool {
        target.0 & self.0 == target.0
    }

    /// Check if the current permission is empty (i.e. has no permissions set)
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::default();
    ///     assert!(perm.is_empty());
    /// }
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

impl From<u8> for Permissions {
    fn from(value: u8) -> Self {
        Self(value)
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

/// Represent an api key scope. The scope is composed of:
/// * an API Key like `msco:0938n8b37r378brf`
/// * the associated permissions (like: read, write, ..)
/// * a description to keep track of the purpose of the key
pub struct AuthScope {
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

impl AuthScope {
    /// Create a new API key scope
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::{ApiKeyScope, Permission};
    ///
    /// fn main(){
    ///     // Single permission
    ///     let scope = ApiKeyScope::new(Permission::READ, "dummy key".to_owned());
    ///
    ///     // Multiple permissions
    ///     let scope = ApiKeyScope::new(
    ///         Permission::READ | Permission::WRITE,
    ///         "dummy key".to_owned(),
    ///     );
    /// }
    pub fn new(
        permission: Permissions,
        description: String,
        expires: Option<std::time::Duration>,
    ) -> Self {
        Self {
            key: ApiKey::new(),
            permissions: permission,
            creation_timestamp: Timestamp::now(),
            // FIXME
            expiration_timestamp: expires.map(|delta| Timestamp::now() + delta),
            description,
        }
    }

    /// Get the scope api key
    pub fn key(&self) -> &ApiKey {
        &self.key
    }

    /// Check if the key is expired
    pub fn is_expired(&self) -> bool {
        if let Some(ts) = self.expiration_timestamp {
            return ts >= Timestamp::now();
        }

        return false;
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

        let key: ApiKey = key_str.parse().expect("Error parsing API key");
    }

    #[test]
    fn bad_apy_key() {
        let res: Result<ApiKey, ApiKeyError> =
            "mosaico_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c2".parse();
        assert!(matches!(res, Err(ApiKeyError::BadHeader)));

        // Removed char in payload
        let res: Result<ApiKey, ApiKeyError> = "msco_m8osbmxriljmgkyeb7aybirba4jeysw_e2c2".parse();
        assert!(matches!(res, Err(ApiKeyError::BadPayload)));

        // e -> E in checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_E2c2".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadFingerprint)));

        // Removed char from checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadFingerprint)));

        // Changed checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c3".parse();
        assert!(matches!(res, Err(ApiKeyError::ChecksumMismatch)));
    }
}
