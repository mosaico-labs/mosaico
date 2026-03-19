use super::*;
use crate::types;
use crc32fast::Hasher;
use std::{ops::BitOr, str::FromStr};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ApiKeyError {
    #[error("the token is incomplete")]
    IncompleteToken,

    #[error("bad header")]
    BadTokenHeader,

    #[error("bad payload")]
    BadTokenPayload,

    #[error("bad fingerprint")]
    BadTokenFingerprint,

    #[error("bad length")]
    BadTokenLength,

    #[error("fingerprint mismatch")]
    TokenFingerprintMismatch,

    #[error("unrecognized permission string")]
    UnrecognizedPermissionString,

    #[error("missing permissions")]
    MissingPermissions,
}

pub type TokenPayload = [u8; Token::PAYLOAD_LENGTH];
pub type TokenFingerprint = [u8; Token::FINGERPRINT_LENGTH];

/// Mosaico API Key.
#[derive(PartialEq, Debug, Clone, Copy)]
pub struct Token {
    payload: TokenPayload,
    fingerprint: TokenFingerprint,
}

fn compute_fingerprint(payload: &TokenPayload) -> TokenFingerprint {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let hash = hasher.finalize();

    let tmp = format!("{:0length$x}", hash, length = Token::FINGERPRINT_LENGTH);
    let bytes = tmp.as_bytes();

    bytes.try_into().unwrap()
}

/// Perform all checks required to cast a payload string to
/// the [`Payload`] fixed size array
fn cast_payload(payload: &str) -> Result<TokenPayload, ApiKeyError> {
    let payload_size_ok = payload.chars().count() == Token::PAYLOAD_LENGTH;

    let payload_is_alphanumeric: bool = payload
        .chars()
        .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

    if !(payload_size_ok && payload_is_alphanumeric) {
        return Err(ApiKeyError::BadTokenPayload);
    }

    Ok(payload.as_bytes().try_into().unwrap())
}

/// Perform all checks required to cast fingerprint string to
/// the [`Fingerprint`] fixed size array
fn cast_fingerprint(fingerprint: &str) -> Result<TokenFingerprint, ApiKeyError> {
    let fingerprint_size_ok = fingerprint.chars().count() == Token::FINGERPRINT_LENGTH;

    let fingerprint_is_alphanumeric: bool = fingerprint
        .chars()
        .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

    if !(fingerprint_size_ok && fingerprint_is_alphanumeric) {
        return Err(ApiKeyError::BadTokenFingerprint);
    }

    Ok(fingerprint.as_bytes().try_into().unwrap())
}

impl Token {
    /// Header included in the token
    pub const HEADER: &str = "msco";

    /// Number of characters used to generate the token payload
    const PAYLOAD_LENGTH: usize = 32;

    /// Number of characters used to store the fingerprint.
    ///
    /// The fingerprint is set to 8 characters since is also used as general
    /// token identifier to perform actions like: list, revoke, etc
    const FINGERPRINT_LENGTH: usize = 8;

    /// Character used to separate header, payload and checksum in the token
    const SEPARATOR: &str = "_";

    /// Generates a new random token
    pub fn new() -> Self {
        // Use of `.unwrap()` since we are creating a string of known size with alphanumeric chars
        let payload: TokenPayload = crate::random::alphanumeric(Token::PAYLOAD_LENGTH)
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

    pub fn from_bytes(payload: TokenPayload, fingerprint: TokenFingerprint) -> Self {
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

impl FromStr for Token {
    type Err = ApiKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(Token::SEPARATOR).collect();

        if parts.len() != 3 {
            return Err(ApiKeyError::IncompleteToken);
        }

        let (header, payload, checksum) = (parts[0], parts[1], parts[2]);

        if header != Token::HEADER {
            return Err(ApiKeyError::BadTokenHeader);
        }

        let payload = cast_payload(payload)?;
        let checksum = cast_fingerprint(checksum)?;

        if checksum != compute_fingerprint(&payload) {
            return Err(ApiKeyError::TokenFingerprintMismatch);
        }

        Ok(Self {
            payload,
            fingerprint: checksum,
        })
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{header}{separator}{payload}{separator}{checksum}",
            header = Token::HEADER,
            payload = std::str::from_utf8(&self.payload).unwrap(),
            checksum = std::str::from_utf8(&self.fingerprint).unwrap(),
            separator = Token::SEPARATOR,
        )
    }
}

impl Default for Token {
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// PERMISSIONS
////////////////////////////////////////////////////////////////////////////////////////////////////

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
    /// use mosaicod_core::types::auth::Permissions;
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
    /// use mosaicod_core::types::auth::Permissions;
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
    /// use mosaicod_core::types::auth::Permissions;
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
    /// use mosaicod_core::types::auth::Permissions;
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
    /// use mosaicod_core::types::auth::Permissions;
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

    pub fn is_read(&self) -> bool {
        self.has(Permissions::READ)
    }

    pub fn is_write(&self) -> bool {
        self.has(Permissions::WRITE)
    }

    pub fn is_delete(&self) -> bool {
        self.has(Permissions::DELETE)
    }

    pub fn is_manage(&self) -> bool {
        self.has(Permissions::MANAGE)
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

/// Convert a set of permissions into a vector of strings
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

impl FromStr for Permissions {
    type Err = ApiKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read" => Ok(Permissions::READ),
            "write" => Ok(Permissions::WRITE),
            "delete" => Ok(Permissions::DELETE),
            "manage" => Ok(Permissions::MANAGE),
            _ => Err(ApiKeyError::UnrecognizedPermissionString),
        }
    }
}

impl TryFrom<&[&str]> for Permissions {
    type Error = ApiKeyError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(ApiKeyError::MissingPermissions);
        }
        value.iter().try_fold(Permissions::default(), |mut b, &s| {
            b = b.add(s.parse()?);
            Ok(b)
        })
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
pub struct ApiKey {
    pub key: Token,

    /// Permissions associated with the scope
    pub permissions: Permissions,

    /// Description to keep track of the purpose of the key
    pub description: String,

    /// Creation timestamp
    pub creation_timestamp: Timestamp,

    /// Expiration timestamp
    pub expiration_timestamp: Option<Timestamp>,
}

impl ApiKey {
    /// Create a new API key
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::{ApiKey, auth::Permissions};
    ///
    /// // Single permission
    /// let policy = ApiKey::new(Permissions::READ, "dummy key".to_owned(), None);
    ///
    /// // Multiple permissions
    /// let policy = ApiKey::new(
    ///     Permissions::READ | Permissions::WRITE,
    ///     "dummy key".to_owned(),
    ///     None
    /// );
    pub fn new(
        permission: Permissions,
        description: String,
        expires_at: Option<types::Timestamp>,
    ) -> Self {
        Self {
            key: Token::new(),
            permissions: permission,
            creation_timestamp: Timestamp::now(),
            expiration_timestamp: expires_at,
            description,
        }
    }

    /// Get the token associated with this API key
    pub fn token(&self) -> &Token {
        &self.key
    }

    /// Check if the API key is expired
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

        // Check string-to-permission conversion.
        assert_eq!(
            "".parse::<Permissions>().unwrap_err(),
            ApiKeyError::UnrecognizedPermissionString
        );
        assert_eq!("read".parse::<Permissions>().unwrap(), Permissions::READ);
        assert_eq!("write".parse::<Permissions>().unwrap(), Permissions::WRITE);
        assert_eq!(
            "delete".parse::<Permissions>().unwrap(),
            Permissions::DELETE
        );
        assert_eq!(
            "manage".parse::<Permissions>().unwrap(),
            Permissions::MANAGE
        );
        assert_eq!(
            "wrong_string".parse::<Permissions>().unwrap_err(),
            ApiKeyError::UnrecognizedPermissionString
        );

        // Check conversion from vector of strings to permission.
        let perm: Result<Permissions, ApiKeyError> = vec![].as_slice().try_into();
        assert_eq!(perm.unwrap_err(), ApiKeyError::MissingPermissions);
        let perm: Permissions = vec!["read"].as_slice().try_into().unwrap();
        assert_eq!(perm, Permissions::READ);
        let perm: Permissions = vec!["read", "write"].as_slice().try_into().unwrap();
        assert_eq!(perm, Permissions::READ | Permissions::WRITE);
        let perm: Permissions = vec!["read", "write", "delete", "manage"]
            .as_slice()
            .try_into()
            .unwrap();
        assert_eq!(
            perm,
            Permissions::READ | Permissions::WRITE | Permissions::DELETE | Permissions::MANAGE
        );
        let perm: Result<Permissions, ApiKeyError> =
            vec!["read", "wrong_string"].as_slice().try_into();
        assert_eq!(perm.unwrap_err(), ApiKeyError::UnrecognizedPermissionString);
        let perm: Result<Permissions, ApiKeyError> = vec![""].as_slice().try_into();
        assert_eq!(perm.unwrap_err(), ApiKeyError::UnrecognizedPermissionString);
    }

    #[test]
    fn api_key_create_and_parse() {
        let key = Token::new();
        dbg!(&key.to_string());

        let key_str = key.to_string();

        let _: Token = key_str.parse().expect("Error parsing API key");
    }

    #[test]
    fn bad_apy_key() {
        let res: Result<Token, ApiKeyError> =
            "mosaico_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenHeader)));

        // Removed char in payload
        let res: Result<Token, ApiKeyError> =
            "msco_rfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // added non ascii char in fingerprint
        let res: Result<Token, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_©b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenFingerprint)));

        // Removed char from fingerprint
        let res: Result<Token, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenFingerprint)));
    }
}
