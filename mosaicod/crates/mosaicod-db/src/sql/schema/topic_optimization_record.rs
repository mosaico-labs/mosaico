use mosaicod_core::types;

#[derive(Debug, Clone)]
pub struct TopicOptimizationRecord {
    pub topic_id: i32,

    /// Path inside Object store where to find optimized data and files.
    pub(crate) opt_path_in_store: Option<String>,

    /// UNIX timestamp in milliseconds indicating when the optimization started.
    pub(crate) start_unix_tstamp: Option<i64>,
}

impl TopicOptimizationRecord {
    pub fn opt_path_in_store(&self) -> Option<types::TopicPathInStore> {
        self.opt_path_in_store.clone().map(Into::into)
    }

    pub fn start_timestamp(&self) -> Option<types::Timestamp> {
        self.start_unix_tstamp.map(Into::into)
    }
}
