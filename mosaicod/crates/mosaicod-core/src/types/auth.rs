use super::*;
use crate::types;
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

/// List of possible permissions associated to an API Key (only one per API Key).
/// They are arranged in a hierarchy where an element defines its permission and gets also all the permissions from the previous ones:
/// - **Read**: grants read access to data
/// - **Write**: grants read and write access to data
/// - **Delete**: grants read, write and delete access to data
/// - **Manage**: grants read, write and delete access to data. Plus the authorization to manage other API keys.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Permission {
    Read = 0b0000_0001,
    Write = 0b0000_0011,
    Delete = 0b0000_0111,
    Manage = 0b0000_1111,
}

impl Permission {
    const READ_BIT: u8 = 0b0000_0001;
    const WRITE_BIT: u8 = 0b0000_0010;
    const DELETE_BIT: u8 = 0b0000_0100;
    const MANAGE_BIT: u8 = 0b0000_1000;

    pub fn can_read(&self) -> bool {
        (*self as u8 & Self::READ_BIT) != 0
    }

    pub fn can_write(&self) -> bool {
        (*self as u8 & Self::WRITE_BIT) != 0
    }

    pub fn can_delete(&self) -> bool {
        (*self as u8 & Self::DELETE_BIT) != 0
    }

    pub fn can_manage(&self) -> bool {
        (*self as u8 & Self::MANAGE_BIT) != 0
    }
}

/// Convert a permission into a string
impl From<Permission> for String {
    fn from(value: Permission) -> Self {
        match value {
            Permission::Read => String::from("read"),
            Permission::Write => String::from("write"),
            Permission::Delete => String::from("delete"),
            Permission::Manage => String::from("manage"),
        }
    }
}

impl FromStr for Permission {
    type Err = ApiKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read" => Ok(Permission::Read),
            "write" => Ok(Permission::Write),
            "delete" => Ok(Permission::Delete),
            "manage" => Ok(Permission::Manage),
            _ => Err(ApiKeyError::InvalidStringToPermissionCast),
        }
    }
}

impl TryFrom<u8> for Permission {
    type Error = ApiKeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == Permission::Read as u8 => Ok(Permission::Read),
            x if x == Permission::Write as u8 => Ok(Permission::Write),
            x if x == Permission::Delete as u8 => Ok(Permission::Delete),
            x if x == Permission::Manage as u8 => Ok(Permission::Manage),
            _ => Err(ApiKeyError::InvalidIntToPermissionCast),
        }
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
    pub permission: Permission,

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
    /// # Example
    /// ```
    /// use mosaicod_core::types::{ApiKey, auth::Permission};
    ///
    /// // Read permission
    /// let policy = ApiKey::new(Permission::Read, "dummy key".to_owned(), None);
    ///
    /// // Write permissions (read is implicitly inherited)
    /// let policy = ApiKey::new(
    ///     Permission::Write,
    ///     "dummy key".to_owned(),
    ///     None
    /// );
    pub fn new(
        permission: Permission,
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
        let perm = Permission::Read;
        assert!(perm.can_read());
        assert!(!perm.can_write());
        assert!(!perm.can_delete());
        assert!(!perm.can_manage());

        let perm = Permission::Write;
        assert!(perm.can_read());
        assert!(perm.can_write());
        assert!(!perm.can_delete());
        assert!(!perm.can_manage());

        let perm = Permission::Delete;
        assert!(perm.can_read());
        assert!(perm.can_write());
        assert!(perm.can_delete());
        assert!(!perm.can_manage());

        let perm = Permission::Manage;
        assert!(perm.can_read());
        assert!(perm.can_write());
        assert!(perm.can_delete());
        assert!(perm.can_manage());

        // Check string-to-permission conversion.
        assert_eq!(
            "".parse::<Permission>().unwrap_err(),
            ApiKeyError::InvalidStringToPermissionCast
        );
        assert_eq!("read".parse::<Permission>().unwrap(), Permission::Read);
        assert_eq!("write".parse::<Permission>().unwrap(), Permission::Write);
        assert_eq!("delete".parse::<Permission>().unwrap(), Permission::Delete);
        assert_eq!("manage".parse::<Permission>().unwrap(), Permission::Manage);
        assert_eq!(
            "wrong_string".parse::<Permission>().unwrap_err(),
            ApiKeyError::InvalidStringToPermissionCast
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
