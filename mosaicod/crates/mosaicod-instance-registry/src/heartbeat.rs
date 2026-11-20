//! Shared helper for registering a long-running `mosaicod` process (server, cleanup, store
//! optimizer, ...) in the instance registry (see `mosaicod ps`).
//!
//! Callers are expected to register via [`mosaicod_db::instance_registry_create`], spawn
//! [`InstanceHeartbeat::run`] to keep that registration alive, and deregister via
//! [`mosaicod_db::instance_registry_delete`] once done; this crate only provides the piece
//! that would otherwise be duplicated across every process kind.

use std::time::Duration;

use mosaicod_core::types;
use mosaicod_db as db;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Periodically refreshes an instance's heartbeat in the instance registry, and opportunistically
/// purges long-expired entries.
///
/// This is intentionally its own small loop (rather than folded into a process's own work loop):
/// a process's own schedule may be driven by a `time_interval` as sparse as once a day, while the
/// heartbeat needs a much tighter, fixed cadence to be a useful liveness signal.
///
/// Each heartbeat doubles as a database connectivity check, published through
/// [`InstanceHeartbeat::db_reachable`].
pub struct InstanceHeartbeat {
    db: db::Database,
    instance_id: i32,
    interval: Duration,
    db_reachable: watch::Sender<bool>,
}

impl InstanceHeartbeat {
    /// Creates a heartbeat for `instance_id`, sent every
    /// [`INSTANCE_HEARTBEAT_INTERVAL_SECS`](types::INSTANCE_HEARTBEAT_INTERVAL_SECS).
    ///
    /// The database starts out as reachable, since the caller has just registered the instance.
    pub fn new(db: db::Database, instance_id: i32) -> Self {
        Self {
            db,
            instance_id,
            interval: Duration::from_secs(types::INSTANCE_HEARTBEAT_INTERVAL_SECS as u64),
            db_reachable: watch::Sender::new(true),
        }
    }

    /// Overrides the interval between heartbeats, so that tests don't wait on the default one.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Returns a receiver tracking whether the last heartbeat reached the database. It is only
    /// notified when the value flips, and closed once [`InstanceHeartbeat::run`] returns.
    pub fn db_reachable(&self) -> watch::Receiver<bool> {
        self.db_reachable.subscribe()
    }

    /// Sends heartbeats until `shutdown` is canceled.
    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {}
                _ = shutdown.cancelled() => break,
            }

            let now = chrono::Utc::now().timestamp();

            let res =
                db::instance_registry_heartbeat(&mut self.db.connection(), self.instance_id, now)
                    .await;

            let reachable = res.is_ok();
            self.db_reachable.send_if_modified(|prev| {
                let flipped = *prev != reachable;
                *prev = reachable;
                flipped
            });

            match res {
                Ok(true) => {}
                Ok(false) => warn!(
                    "instance {} heartbeat found no matching registry entry, it may have been purged as expired",
                    self.instance_id
                ),
                Err(e) => {
                    warn!(
                        "failed to send heartbeat for instance {}: {}",
                        self.instance_id, e
                    );
                    // The purge below would fail the same way.
                    continue;
                }
            }

            let expiry_threshold = now - types::INSTANCE_REGISTRY_EXPIRY_THRESHOLD_SECS as i64;

            if let Err(e) = db::instance_registry_delete_expired(
                &mut self.db.connection(),
                expiry_threshold,
                types::allow_data_loss(),
            )
            .await
            {
                warn!("failed to purge expired instance registry entries: {}", e);
            }
        }
    }
}
