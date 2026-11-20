//! The standard gRPC health checking protocol (`grpc.health.v1.Health`), served on the same
//! port as the Flight service so that orchestrators can probe it natively (e.g. Kubernetes
//! `grpc:` probes). The auth middleware lets its calls through without an API key.

use tokio::{sync::watch, task::JoinHandle};
use tonic_health::{
    ServingStatus,
    pb::health_server::HealthServer,
    server::{HealthReporter, HealthService},
};
use tracing::{info, warn};

/// Health service name for liveness: `SERVING` as long as the server is up.
///
/// It deliberately ignores PostgreSQL and the store: restarting the process doesn't fix their
/// outages, and failing liveness on them would restart every replica at once.
pub const LIVENESS_SERVICE: &str = "";

/// Health service name for readiness: `NOT_SERVING` while PostgreSQL is unreachable, as seen by
/// the last instance heartbeat.
pub const READINESS_SERVICE: &str = "mosaicod";

/// Keeps the health statuses up to date while the server runs.
pub(crate) struct Health {
    reporter: HealthReporter,
    track_readiness: JoinHandle<()>,
}

impl Health {
    /// Registers both health services, with readiness following `db_reachable` from now on.
    /// Returns the gRPC service serving them.
    pub(crate) async fn start(
        mut db_reachable: watch::Receiver<bool>,
    ) -> (Self, HealthServer<HealthService>) {
        // The reporter registers `LIVENESS_SERVICE` as `SERVING` on creation.
        let reporter = HealthReporter::new();
        let service = HealthServer::new(HealthService::from_health_reporter(reporter.clone()));

        // Registered before serving, so that probes never find the readiness service missing.
        let reachable = *db_reachable.borrow_and_update();
        set_readiness(&reporter, reachable).await;

        let track_readiness = tokio::spawn({
            let reporter = reporter.clone();
            async move {
                while db_reachable.changed().await.is_ok() {
                    let reachable = *db_reachable.borrow_and_update();
                    if reachable {
                        info!("database reachable again, reporting ready");
                    } else {
                        warn!("database unreachable, reporting not ready");
                    }
                    set_readiness(&reporter, reachable).await;
                }
            }
        });

        let health = Self {
            reporter,
            track_readiness,
        };

        (health, service)
    }

    /// Reports both services as `NOT_SERVING`, then ends their `Watch` streams, which would
    /// otherwise hold up a graceful shutdown forever.
    pub(crate) async fn shutdown(mut self) {
        self.track_readiness.abort();

        for service in [LIVENESS_SERVICE, READINESS_SERVICE] {
            self.reporter
                .set_service_status(service, ServingStatus::NotServing)
                .await;
            // Drops the status sender, so that streams end after sending `NOT_SERVING`.
            self.reporter.clear_service_status(service).await;
        }
    }
}

impl Drop for Health {
    fn drop(&mut self) {
        self.track_readiness.abort();
    }
}

async fn set_readiness(reporter: &HealthReporter, db_reachable: bool) {
    let status = if db_reachable {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    reporter.set_service_status(READINESS_SERVICE, status).await;
}
