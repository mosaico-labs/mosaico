use super::*;
use crate::{Error, error::PublicError, types};
use crc32fast::Hasher;
use std::str::FromStr;

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

    #[error("invalid string to permission cast")]
    InvalidStringToPermissionCast,

    #[error("invalid integer to permission cast")]
    InvalidIntToPermissionCast,

    #[error("missing permissions")]
    MissingPermissions,
}

impl PublicError for ApiKeyError {
    fn error(&self) -> Error {
        match self {
            Self::MissingPermissions => Error::unauthorized("missing permissions".to_string()),
            _ => Error::bad_request(self.to_string()),
        }
    }
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
// PERMISSIONS
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A single, independent capability that can be granted to an API Key.
///
/// Permissions are granular and non-hierarchical: they are combined explicitly
/// in a [`Permissions`] set (e.g. `read|write`) and no capability implies
/// another.
/// - **Read**: grants only read access to data
/// - **Write**: grants only write access to data
/// - **Delete**: grants only delete access to data
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Permission {
    Read = 0b0000_0001,
    Write = 0b0000_0010,
    Delete = 0b0000_0100,
}

/// A set of granted [`Permission`]s, stored as an independent bitmask.
///
/// A valid set always grants at least one capability
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Permissions(u8);

impl Permissions {
    const ALL: u8 = Permission::Read as u8 | Permission::Write as u8 | Permission::Delete as u8;

    /// Returns a set granting every capability.
    pub fn all() -> Self {
        Permissions(Self::ALL)
    }

    pub fn can_read(&self) -> bool {
        (self.0 & Permission::Read as u8) != 0
    }

    pub fn can_write(&self) -> bool {
        (self.0 & Permission::Write as u8) != 0
    }

    pub fn can_delete(&self) -> bool {
        (self.0 & Permission::Delete as u8) != 0
    }

    pub fn bits(&self) -> u8 {
        self.0
    }
}

impl From<Permission> for Permissions {
    fn from(value: Permission) -> Self {
        Permissions(value as u8)
    }
}

impl std::fmt::Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut p = Vec::new();
        if self.can_read() {
            p.push("read");
        }
        if self.can_write() {
            p.push("write");
        }
        if self.can_delete() {
            p.push("delete");
        }
        write!(f, "{}", p.join("|"))
    }
}

impl FromStr for Permissions {
    type Err = ApiKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut perms: u8 = 0;

        for p in value.split('|') {
            perms |= match p {
                "read" => Permission::Read as u8,
                "write" => Permission::Write as u8,
                "delete" => Permission::Delete as u8,
                _ => return Err(ApiKeyError::InvalidStringToPermissionCast),
            }
        }

        // Every valid token sets a bit, so a non-empty, all-valid input can
        // never be empty here; the guard in `TryFrom` still enforces the
        // invariant for any other construction path.
        Self::try_from(perms)
    }
}

impl TryFrom<u8> for Permissions {
    type Error = ApiKeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value & !Self::ALL != 0 {
            return Err(ApiKeyError::InvalidIntToPermissionCast);
        }
        if value == 0 {
            return Err(ApiKeyError::MissingPermissions);
        }
        Ok(Permissions(value))
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
    pub permission: Permissions,

    /// Description to keep track of the purpose of the key
    pub description: String,

    /// Creation timestamp
    pub created_at: Timestamp,

    /// Expiration timestamp
    pub expires_at: Option<Timestamp>,
}

impl ApiKey {
    /// Create a new API key
    ///
    /// Permissions are granular and independent: each one grants only its own
    /// capability and can be combined explicitly.
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::{ApiKey, auth::{Permission, Permissions}};
    ///
    /// // Read-only key.
    /// let policy = ApiKey::new(Permission::Read.into(), "dummy key".to_owned(), None);
    ///
    /// // Combined read + write key (write does NOT imply read).
    /// let policy = ApiKey::new(
    ///     "read|write".parse::<Permissions>().unwrap(),
    ///     "dummy key".to_owned(),
    ///     None,
    /// );
    /// ```
    pub fn new(
        permission: Permissions,
        description: String,
        expires_at: Option<types::Timestamp>,
    ) -> Self {
        Self {
            key: Token::new(),
            permission,
            created_at: Timestamp::now(),
            expires_at,
            description,
        }
    }

    /// Get the token associated with this API key
    pub fn token(&self) -> &Token {
        &self.key
    }

    /// Check if the API key is expired
    pub fn is_expired(&self) -> bool {
        if let Some(ts) = self.expires_at {
            return ts <= Timestamp::now();
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permissions() {
        let perm = Permissions(Permission::Read as u8);
        assert!(perm.can_read());
        assert!(!perm.can_write());
        assert!(!perm.can_delete());

        let perm = Permissions(Permission::Write as u8);
        assert!(!perm.can_read());
        assert!(perm.can_write());
        assert!(!perm.can_delete());

        let perm = Permissions(Permission::Delete as u8);
        assert!(!perm.can_read());
        assert!(!perm.can_write());
        assert!(perm.can_delete());

        // Check string-to-permission conversion.
        assert_eq!(
            "".parse::<Permissions>().unwrap_err(),
            ApiKeyError::InvalidStringToPermissionCast
        );
        assert_eq!(
            "read".parse::<Permissions>().unwrap(),
            Permissions(Permission::Read as u8)
        );
        assert_eq!(
            "write".parse::<Permissions>().unwrap(),
            Permissions(Permission::Write as u8)
        );
        assert_eq!(
            "delete".parse::<Permissions>().unwrap(),
            Permissions(Permission::Delete as u8)
        );
        assert_eq!(
            "wrong_string".parse::<Permissions>().unwrap_err(),
            ApiKeyError::InvalidStringToPermissionCast
        );

        // Combined permissions round-trip through Display/FromStr.
        let combined = "read|write".parse::<Permissions>().unwrap();
        assert!(combined.can_read() && combined.can_write() && !combined.can_delete());
        assert_eq!(combined.to_string(), "read|write");
        assert_eq!(Permissions::all().to_string(), "read|write|delete");

        // The empty set is rejected on every construction path.
        assert_eq!(
            Permissions::try_from(0).unwrap_err(),
            ApiKeyError::MissingPermissions
        );
        assert_eq!(
            Permissions::try_from(0b1000_0000).unwrap_err(),
            ApiKeyError::InvalidIntToPermissionCast
        );
    }

    #[test]
    fn api_key_create_and_parse() {
        let key = Token::new();
        dbg!(&key.to_string());

        let key_str = key.to_string();

        let _: Token = key_str.parse().expect("Error parsing API key");
    }

    #[test]
    fn api_key_ok() {
        let _: Token = "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530"
            .parse()
            .expect("Unable to parse APi key token");
    }

    #[test]
    fn api_key_bad() {
        // Change header with a longer string
        let res: Result<Token, ApiKeyError> =
            "mosaico_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenHeader)));

        // Change header with a string of same length
        let res: Result<Token, ApiKeyError> =
            "xyzw_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenHeader)));

        // Removed char in payload
        let res: Result<Token, ApiKeyError> =
            "msco_rfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // Added char in payload
        let res: Result<Token, ApiKeyError> =
            "msco_xvrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // Add special ascii char in payload
        let res: Result<Token, ApiKeyError> =
            "msco_vrfecej!4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // Add uppercase letter in payload
        let res: Result<Token, ApiKeyError> =
            "msco_vrfecejU4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // Add special ascii char in payload
        let res: Result<Token, ApiKeyError> =
            "msco_vrfecej©4lqivysxgaseefa3tsxs0vrl_1b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenPayload)));

        // Added non ascii char in fingerprint
        let res: Result<Token, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_©1b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenFingerprint)));

        // Removed char from fingerprint
        let res: Result<Token, ApiKeyError> =
            "msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_b676530".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadTokenFingerprint)));
    }
}
