use mosaicod_core::types;

/// A registered hook. See `mosaicod hook`.
#[derive(Debug)]
pub struct HookRecord {
    pub hook_id: i32,
    pub url: String,
    pub(crate) creation_unix_tstamp: i64,
}

impl From<HookRecord> for types::Hook {
    fn from(value: HookRecord) -> Self {
        Self {
            id: value.hook_id,
            url: value.url,
            created_at: value.creation_unix_tstamp.into(),
        }
    }
}
