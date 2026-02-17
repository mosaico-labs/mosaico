use crate as repo;
use mosaicod_core::types;

#[derive(Debug)]
pub struct LayerRecord {
    pub layer_id: i32,
    pub layer_name: String,
    pub layer_description: String,
}

impl LayerRecord {
    pub fn new(name: String, description: String) -> Self {
        Self {
            layer_id: repo::UNREGISTERED,
            layer_name: name,
            layer_description: description,
        }
    }
}

impl From<LayerRecord> for types::Layer {
    fn from(value: LayerRecord) -> Self {
        Self::new(
            types::LayerLocator::from(value.layer_name.as_str()),
            value.layer_description,
        )
    }
}
