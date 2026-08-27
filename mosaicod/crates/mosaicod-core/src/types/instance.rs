use thiserror::Error;

#[derive(Debug, Error)]
pub enum InstanceKindError {
    #[error("unknown instance kind `{0}`")]
    UnknownKind(String),
}

impl InstanceKindError {
    pub fn unknown_kind(kind_name: &str) -> Self {
        Self::UnknownKind(kind_name.to_owned())
    }
}

/// Identifies the kind of long-running `mosaicod` process registered in the instance registry
/// (see `mosaicod ps`).
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum InstanceKind {
    /// A `mosaicod server` process.
    Server,
    /// A `mosaicod cleanup` process.
    Cleanup,
}

impl InstanceKind {
    fn name(&self) -> &'static str {
        match self {
            InstanceKind::Server => "server",
            InstanceKind::Cleanup => "cleanup",
        }
    }
}

impl std::str::FromStr for InstanceKind {
    type Err = InstanceKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "server" => Ok(Self::Server),
            "cleanup" => Ok(Self::Cleanup),
            _ => Err(InstanceKindError::unknown_kind(value)),
        }
    }
}

impl std::fmt::Display for InstanceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
