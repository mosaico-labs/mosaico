//! [`JsonMetadataBlob`] is the [`MetadataBlob`] impl used for arbitrary
//! user-supplied JSON (`user_metadata`, the ontology filter DSL), which
//! round-trips through both the binary protobuf wire and the on-disk
//! metadata files (see [`crate::JsonTopicMetadata`]).

use mosaicod_core::types::{self, MetadataBlob};
use serde::{Deserialize, Serialize};

type Error = types::MetadataError;

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.contains("--")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b" _-".contains(&b))
}

fn find_invalid_keys(v: &serde_json::Value) -> Option<&str> {
    match v {
        serde_json::Value::Object(obj) => {
            // 1. Check if any current key is invalid
            if let Some(invalid_key) = obj.keys().find(|key| !valid_key(key)) {
                return Some(invalid_key);
            }

            // 2. Recursively check the values of this object
            obj.values().find_map(find_invalid_keys)
        }
        serde_json::Value::Array(arr) => {
            // 3. Recursively check elements in the array
            arr.iter().find_map(find_invalid_keys)
        }
        // Base case: Scalars (Null, Bool, Number, String) cannot contain invalid keys
        _ => None,
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonMetadataBlob(serde_json::Value);

impl MetadataBlob for JsonMetadataBlob {
    fn try_to_string(&self) -> Result<String, Error> {
        Ok(serde_json::to_string(&self.0).map_err(|e| Error::SerializationError(e.to_string())))?
    }

    #[allow(refining_impl_trait)]
    fn try_from_str(v: &str) -> Result<JsonMetadataBlob, Error> {
        let json =
            serde_json::from_str(v).map_err(|e| Error::DeserializationError(e.to_string()))?;

        if let Some(invalid_key) = find_invalid_keys(&json) {
            return Err(Error::InvalidJsonKey(invalid_key.to_owned()));
        }

        Ok(JsonMetadataBlob(json))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }

    #[allow(refining_impl_trait)]
    fn try_from_slice(bytes: &[u8]) -> Result<JsonMetadataBlob, Error> {
        let json = serde_json::from_slice(bytes)
            .map_err(|e| Error::DeserializationError(e.to_string()))?;

        if let Some(invalid_key) = find_invalid_keys(&json) {
            return Err(Error::InvalidJsonKey(invalid_key.to_owned()));
        }

        Ok(JsonMetadataBlob(json))
    }
}

impl From<JsonMetadataBlob> for serde_json::Value {
    fn from(value: JsonMetadataBlob) -> Self {
        value.0
    }
}

impl From<serde_json::Value> for JsonMetadataBlob {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

/// `user_metadata` fields carry arbitrary user-supplied JSON as raw bytes.
/// Empty bytes means `None`.
pub fn user_metadata_to_bytes(value: Option<JsonMetadataBlob>) -> Result<Vec<u8>, Error> {
    match value {
        Some(metadata) => metadata.to_bytes(),
        None => Ok(vec![]),
    }
}

/// `user_metadata` fields carry arbitrary user-supplied JSON as raw bytes.
/// Empty bytes means `None`.
pub fn user_metadata_from_bytes(bytes: Vec<u8>) -> Result<Option<JsonMetadataBlob>, Error> {
    if bytes.is_empty() {
        return Ok(None);
    }

    let json: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| Error::DeserializationError(e.to_string()))?;

    Ok(Some(JsonMetadataBlob::from(json)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_find_invalid_keys_with_valid_json_structures() {
        // Base case scalars shouldn't trigger anything
        assert_eq!(find_invalid_keys(&json!("just a string")), None);
        assert_eq!(find_invalid_keys(&json!(42)), None);
        assert_eq!(find_invalid_keys(&json!(true)), None);
        assert_eq!(find_invalid_keys(&json!(null)), None);

        // Valid simple objects and arrays
        assert_eq!(find_invalid_keys(&json!({"valid_key": "value"})), None);
        assert_eq!(find_invalid_keys(&json!({"valid-key 123": "value"})), None);
        assert_eq!(find_invalid_keys(&json!({"valid-key-123": "value"})), None);
        assert_eq!(find_invalid_keys(&json!({"valid key 123": "value"})), None);
        assert_eq!(find_invalid_keys(&json!({" valid-key ": "value"})), None);
        assert_eq!(find_invalid_keys(&json!(["value1", "value2"])), None);

        // Deeply nested valid complex payload
        let valid_nested = json!({
            "user_info": {
                "first-name": "John",
                "last name": "Doe"
            },
            "tags": ["admin", "verified"],
            "metadata": [
                { "item-id": 1 },
                { "item-id": 2 },
                { "item-id": {
                    "subitem_id": 3
                }}
            ]
        });
        assert_eq!(find_invalid_keys(&valid_nested), None);
    }

    #[test]
    fn test_find_invalid_keys_with_invalid_key_at_root() {
        // Contains forbidden double dash "--"
        let invalid_root = json!({
            "valid_key": 1,
            "bad--key": 2
        });
        assert_eq!(find_invalid_keys(&invalid_root), Some("bad--key"));

        let some_invalid_chars = [
            '$', '+', '=', '*', '%', '^', '@', '#', '/', '\\', '(', '[', '{', '}', ']', ')',
            '\u{2728}',
        ];

        assert!(some_invalid_chars.iter().all(|c| {
            let invalid_key = format!("invalid{c}key");
            let invalid_json = json!({
                invalid_key: "value"
            });
            find_invalid_keys(&invalid_json).is_some()
        }));
    }

    #[test]
    fn test_find_invalid_keys_with_invalid_key_nested_in_object() {
        let nested_invalid = json!({
            "level1": {
                "level2": {
                    "invalid--here": "busted"
                }
            }
        });
        assert_eq!(find_invalid_keys(&nested_invalid), Some("invalid--here"));
    }

    #[test]
    fn test_find_invalid_keys_with_invalid_key_inside_array() {
        let array_invalid = json!({
            "list": [
                { "ok": 1 },
                { "not--ok": 2 },
                { "also_ok": 3 }
            ]
        });
        assert_eq!(find_invalid_keys(&array_invalid), Some("not--ok"));
    }

    #[test]
    fn test_find_invalid_keys_with_invalid_key_in_root_level_array() {
        let root_array = json!([
            "just a string element",
            { "valid": true },
            { "nested": { "bad--key": false } }
        ]);
        assert_eq!(find_invalid_keys(&root_array), Some("bad--key"));
    }

    #[test]
    fn test_find_invalid_keys_with_empty_structures() {
        assert_eq!(find_invalid_keys(&json!({})), None);
        assert_eq!(find_invalid_keys(&json!([])), None);

        // Empty keys are not allowed
        assert_eq!(find_invalid_keys(&json!({"": "empty key value"})), Some(""));
    }

    #[test]
    fn test_json_metadata_blob_try_from_str_success() {
        let valid_json_str = r#"{
            "user_id": 123,
            "display-name": "Alice",
            "meta data": {
                "nested_key": true
            }
        }"#;

        let result = JsonMetadataBlob::try_from_str(valid_json_str);

        assert!(
            result.is_ok(),
            "Expected valid JSON string to parse successfully"
        );

        // Verify the inner value matches what we passed in
        let blob = result.unwrap();
        assert_eq!(blob.0["user_id"], json!(123));
        assert_eq!(blob.0["meta data"]["nested_key"], json!(true));
    }

    #[test]
    fn test_json_metadata_blob_try_from_str_invalid_json_syntax() {
        // Malformed JSON syntax (missing closing brace)
        let malformed_str = r#"{"key": "value""#;

        let result = JsonMetadataBlob::try_from_str(malformed_str);

        assert!(result.is_err(), "Expected malformed syntax to fail parsing");
        if let Err(Error::DeserializationError(msg)) = result {
            // Assert it's a native serde_json parsing error message
            assert!(
                msg.contains("EOF"),
                "Error message should originate from serde_json parsing failure"
            );
        } else {
            panic!("Expected Error::DeserializationError");
        }
    }

    #[test]
    fn test_json_metadata_blob_try_from_str_fails_on_invalid_key() {
        // Syntactically valid JSON, but contains a forbidden key ("--")
        let invalid_key_str = r#"{
            "fine_key": 1,
            "nested_array": [
                { "broken--key": "value" }
            ]
        }"#;

        let result = JsonMetadataBlob::try_from_str(invalid_key_str);

        assert!(
            result.is_err(),
            "Expected validation to catch the forbidden double-dash key"
        );

        // Verify our custom validation error message is firing correctly
        match result {
            Err(Error::InvalidJsonKey(msg)) => {
                assert_eq!(msg, "broken--key");
            }
            _ => panic!("Expected Error::InvalidJsonKey with our custom validation message"),
        }
    }

    #[test]
    fn test_json_metadata_blob_try_from_str_non_object_json() {
        // Valid JSON can also just be a string literal, number, or bare array
        // Since they don't have object keys, they should pass right through
        let array_str = r#"["value1", "value2", 345]"#;
        let string_str = r#""just a primitive string""#;

        assert!(JsonMetadataBlob::try_from_str(array_str).is_ok());
        assert!(JsonMetadataBlob::try_from_str(string_str).is_ok());
    }
}
