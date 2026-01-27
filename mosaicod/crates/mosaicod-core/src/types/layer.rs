#[derive(Clone)]
pub struct LayerLocator(String);

impl LayerLocator {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for LayerLocator {
    fn from(value: &str) -> Self {
        Self(value.trim().to_owned())
    }
}

impl std::fmt::Display for LayerLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[layer|{}]", self.0)
    }
}

impl From<LayerLocator> for String {
    fn from(value: LayerLocator) -> Self {
        value.0
    }
}

pub struct Layer {
    pub locator: LayerLocator,
    pub description: String,
}

impl Layer {
    pub fn new(locator: LayerLocator, desc: String) -> Self {
        Self {
            locator,
            description: desc,
        }
    }
}
