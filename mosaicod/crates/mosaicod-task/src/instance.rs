//! Shared helpers for registering a background routine in the instance registry (see
//! `mosaicod ps`), used by both [`crate::cleanup::Cleanup`] and
//! [`crate::store_optimizer::StoreOptimizer`].

use mosaicod_core::{params, types};
use mosaicod_db as db;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Reads the local hostname, falling back to `"unknown"` if it can't be determined.
pub(crate) fn local_hostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|e| {
            warn!("unable to determine local hostname: {}", e);
            "unknown".to_owned()
        })
}

/// Periodically refreshes `instance_id`'s heartbeat in the instance registry, and opportunistically
/// purges long-expired entries, until `shutdown` is cancelled.
///
/// This is intentionally its own small loop (rather than folded into the routine's own loop): a
/// routine's own schedule is driven by its `time_interval` (which can be as sparse as once a
/// day), while the heartbeat needs a much tighter, fixed cadence to be a useful liveness signal.
pub(crate) async fn instance_heartbeat_loop(
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
