use super::Format;
use mosaicod_core::types::{self, MetadataBlob, MetadataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type Error = MetadataError;

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

#[derive(Serialize, Deserialize)]
pub struct JsonSequenceMetadata {
    pub user_metadata: JsonMetadataBlob,
}

impl TryFrom<Vec<u8>> for JsonSequenceMetadata {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(serde_json::from_slice(&bytes).map_err(|e| Error::DeserializationError(e.to_string())))?
    }
}

impl TryInto<Vec<u8>> for JsonSequenceMetadata {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }
}

impl JsonSequenceMetadata {
    /// Converts the metadata into a flattened [`HashMap`] representation.
    pub fn to_flat_hashmap(self) -> Result<HashMap<String, String>, MetadataError> {
        Ok(HashMap::from([
            (
                "mosaico:context".to_owned(), //
                "sequence".into(),
            ),
            (
                "mosaico:user_metadata".to_owned(),
                self.user_metadata.try_to_string()?,
            ),
        ]))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicOntologyProperties {
    pub serialization_format: Format,
    pub ontology_tag: String,
}

impl From<types::TopicOntologyProperties> for JsonTopicOntologyProperties {
    fn from(value: types::TopicOntologyProperties) -> Self {
        Self {
            ontology_tag: value.ontology_tag,
            serialization_format: value.serialization_format.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicOntologyMetadata {
    pub properties: JsonTopicOntologyProperties,
    pub user_metadata: JsonMetadataBlob,
}

impl From<types::TopicOntologyMetadata<JsonMetadataBlob>> for JsonTopicOntologyMetadata {
    fn from(value: types::TopicOntologyMetadata<JsonMetadataBlob>) -> Self {
        Self {
            user_metadata: value
                .user_metadata
                .unwrap_or(JsonMetadataBlob(serde_json::Value::Null)),
            properties: JsonTopicOntologyProperties::from(value.properties),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicIntervalProperties {
    pub message_count: usize,
    pub timestamp_ns_min: i64,
    pub timestamp_ns_max: i64,
}

impl From<types::TopicIntervalProperties> for JsonTopicIntervalProperties {
    fn from(value: types::TopicIntervalProperties) -> Self {
        Self {
            message_count: value.message_count,
            timestamp_ns_min: value.timestamp_range.start.as_i64(),
            timestamp_ns_max: value.timestamp_range.end.as_i64(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicMetadata {
    pub properties: JsonTopicProperties,
    pub ontology_metadata: JsonTopicOntologyMetadata,
    pub interval_props: Option<JsonTopicIntervalProperties>,
}

impl JsonTopicMetadata {
    pub fn to_flat_hashmap(self) -> Result<HashMap<String, String>, MetadataError> {
        let mut json_props = serde_json::to_value(&self.ontology_metadata.properties)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        if let Some(interval_props) = &self.interval_props {
            let json_interval_props = serde_json::to_value(interval_props)
                .map_err(|e| Error::SerializationError(e.to_string()))?;

            // Ensure both are actually JSON objects before merging
            if let (Some(obj1), Some(obj2)) =
                (json_props.as_object_mut(), json_interval_props.as_object())
            {
                // Extend the first object with the keys and values of the second
                obj1.extend(obj2.clone());
            }
        }

        Ok(HashMap::from([
            ("mosaico:context".to_owned(), "topic".to_owned()),
            ("mosaico:properties".to_owned(), json_props.to_string()),
            (
                "mosaico:user_metadata".to_owned(),
                self.ontology_metadata.user_metadata.try_to_string()?,
            ),
        ]))
    }
}

impl From<types::TopicMetadata<JsonMetadataBlob>> for JsonTopicMetadata {
    fn from(value: types::TopicMetadata<JsonMetadataBlob>) -> Self {
        Self {
            ontology_metadata: value.ontology_metadata.into(),
            properties: value.properties.into(),
            interval_props: value.interval_props.map(Into::into),
        }
    }
}

impl TryFrom<Vec<u8>> for JsonTopicMetadata {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(serde_json::from_slice(&bytes).map_err(|e| Error::DeserializationError(e.to_string())))?
    }
}

impl TryInto<Vec<u8>> for JsonTopicMetadata {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Ok(serde_json::to_vec(&self).map_err(|e| Error::SerializationError(e.to_string())))?
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonTopicProperties {
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub session_locator: String,
    pub resource_locator: String,
}

impl From<types::TopicMetadataProperties> for JsonTopicProperties {
    fn from(value: types::TopicMetadataProperties) -> Self {
        Self {
            created_at: value.created_at.as_i64(),
            completed_at: value.completed_at.map(Into::into),
            session_locator: value.session_locator.to_string(),
            resource_locator: value.resource_locator.to_string(),
        }
    }
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
