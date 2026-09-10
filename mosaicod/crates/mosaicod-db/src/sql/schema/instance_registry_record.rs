use mosaicod_core::{params, types};
use tracing::error;

/// After this many seconds without a heartbeat, `mosaicod ps` considers an instance possibly
/// down or stopped. Set to a few missed heartbeats so a single transient DB hiccup doesn't
/// falsely flag a live instance.
///
/// TODO: the 3x multiplier is an unvalidated guess, not derived from observed heartbeat
/// jitter/latency. Revisit once we have real data.
const INSTANCE_STALE_THRESHOLD_SECS: u32 = 3 * params::INSTANCE_HEARTBEAT_INTERVAL_SECS;

/// After this many seconds without a heartbeat, `mosaicod ps` considers an instance dead.
///
/// TODO: the 10x multiplier is an unvalidated guess, not derived from observed heartbeat
/// jitter/latency. Revisit once we have real data.
const INSTANCE_DEAD_THRESHOLD_SECS: u32 = 10 * params::INSTANCE_HEARTBEAT_INTERVAL_SECS;

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

    /// Derives the instance's liveness from how long it's been since its last heartbeat,
    /// relative to `INSTANCE_STALE_THRESHOLD_SECS` and `INSTANCE_DEAD_THRESHOLD_SECS`.
    ///
    /// `last_heartbeat_unix_tstamp_secs` is stamped with the instance's own clock, and compared
    /// here against the caller's clock, so this assumes reasonably synced clocks across hosts.
    pub fn status(&self) -> types::InstanceStatus {
        let delta =
            (chrono::Utc::now().timestamp() - self.last_heartbeat_unix_tstamp_secs).max(0) as u64;
        if delta < INSTANCE_STALE_THRESHOLD_SECS as u64 {
            types::InstanceStatus::Alive
        } else if delta < INSTANCE_DEAD_THRESHOLD_SECS as u64 {
            types::InstanceStatus::Stale
        } else {
            types::InstanceStatus::Dead
        }
    }
}
