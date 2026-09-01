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
    /// A `mosaicod store-optimizer` process.
    StoreOptimizer,
}

impl InstanceKind {
    fn name(&self) -> &'static str {
        match self {
            InstanceKind::Server => "server",
            InstanceKind::Cleanup => "cleanup",
            InstanceKind::StoreOptimizer => "store-optimizer",
        }
    }
}

impl std::str::FromStr for InstanceKind {
    type Err = InstanceKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "server" => Ok(Self::Server),
            "cleanup" => Ok(Self::Cleanup),
            "store-optimizer" => Ok(Self::StoreOptimizer),
            _ => Err(InstanceKindError::unknown_kind(value)),
        }
    }
}

impl std::fmt::Display for InstanceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// The derived liveness of a registered `mosaicod` process (see `mosaicod ps`), based on how
/// long it's been since the process last sent a heartbeat.
pub enum InstanceStatus {
    /// Heartbeat received recently; the process is presumed running.
    Alive,
    /// No heartbeat for a while; the process may be down, or may just be slow to check in.
    Stale,
    /// No heartbeat for a long time; the process is presumed dead.
    Dead,
}
