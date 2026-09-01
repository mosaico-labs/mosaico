//! Shared helper for registering a long-running `mosaicod` process (server, cleanup, store
//! optimizer, ...) in the instance registry (see `mosaicod ps`).
//!
//! Callers are expected to register via [`mosaicod_db::instance_registry_create`], spawn
//! [`instance_heartbeat_loop`] to keep that registration alive, and deregister via
//! [`mosaicod_db::instance_registry_delete`] once done; this crate only provides the piece
//! that would otherwise be duplicated across every process kind.

use mosaicod_core::{params, types};
use mosaicod_db as db;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Periodically refreshes `instance_id`'s heartbeat in the instance registry, and opportunistically
/// purges long-expired entries, until `shutdown` is canceled.
///
/// This is intentionally its own small loop (rather than folded into a process's own work loop):
/// a process's own schedule may be driven by a `time_interval` as sparse as once a day, while the
/// heartbeat needs a much tighter, fixed cadence to be a useful liveness signal.
pub async fn instance_heartbeat_loop(
    db: db::Database,
    instance_id: i32,
    shutdown: CancellationToken,
) {
    let interval = std::time::Duration::from_secs(params::INSTANCE_HEARTBEAT_INTERVAL_SECS as u64);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.cancelled() => break,
        }

        let now = chrono::Utc::now().timestamp();

        let updated = db::instance_registry_heartbeat(&mut db.connection(), instance_id, now)
            .await
            .inspect_err(|e| {
                warn!(
                    "failed to send heartbeat for instance {}: {}",
                    instance_id, e
                )
            })
            .unwrap_or(true);

        if !updated {
            warn!(
                "instance {} heartbeat found no matching registry entry, it may have been purged as expired",
                instance_id
            );
        }

        let expiry_threshold = now - params::INSTANCE_REGISTRY_EXPIRY_THRESHOLD_SECS as i64;

        if let Err(e) = db::instance_registry_delete_expired(
            &mut db.connection(),
            expiry_threshold,
            types::allow_data_loss(),
        )
        .await
        {
            warn!("failed to purge expired instance registry entries: {}", e);
        }
    }
}
