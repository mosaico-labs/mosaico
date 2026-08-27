use mosaicod_core::types;
use tracing::error;

/// A registered `mosaicod` process (server, cleanup, ...). See `mosaicod ps`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceRegistryRecord {
    pub instance_id: i32,
    pub(crate) kind: String,
    pub hostname: String,
    pub pid: i32,
    pub(crate) started_unix_tstamp_secs: i64,
    pub(crate) last_heartbeat_unix_tstamp_secs: i64,
    /// True for a routine that performs a single run and exits, rather than looping until shut
    /// down (e.g. `mosaicod cleanup` with the default `--time-interval 0`). Always `false` for
    /// `server`, which has no such concept.
    pub one_shot: bool,
}

impl InstanceRegistryRecord {
    pub fn kind(&self) -> Option<types::InstanceKind> {
        self.kind
            .parse()
            .inspect_err(|e| error!("BUG: invalid instance kind in database: {}", e))
            .ok()
    }

    pub fn started_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(self.started_unix_tstamp_secs, 0).unwrap_or_else(|| {
            panic!(
                "Error converting instance {} start UNIX timestamp {} to DateTime",
                self.instance_id, self.started_unix_tstamp_secs
            )
        })
    }

    pub fn last_heartbeat_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(self.last_heartbeat_unix_tstamp_secs, 0).unwrap_or_else(
            || {
                panic!(
                    "Error converting instance {} last heartbeat UNIX timestamp {} to DateTime",
                    self.instance_id, self.last_heartbeat_unix_tstamp_secs
                )
            },
        )
    }

    /// Returns `true` if, relative to `now`, this instance's last heartbeat is recent enough to
    /// be considered alive (i.e. more recent than `now - staleness_threshold`).
    pub fn is_alive(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        staleness_threshold: types::Duration,
    ) -> bool {
        now.timestamp() - self.last_heartbeat_unix_tstamp_secs < staleness_threshold.num_seconds()
    }
}
