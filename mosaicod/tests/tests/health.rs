#![allow(unused_crate_dependencies)]

use std::time::Duration;

use mosaicod_db as db;
use mosaicod_grpc::health::{LIVENESS_SERVICE, READINESS_SERVICE};
use tests::{actions, common, proxy::TcpProxy};
use tonic::{Streaming, transport::Channel};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
    health_client::HealthClient,
};

/// How long the server gets to reflect a change, e.g. readiness catching up with the database.
/// Generous, since it only takes a couple of heartbeats.
const TIMEOUT: Duration = Duration::from_secs(10);

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

async fn health_client(port: u16) -> HealthClient<Channel> {
    let url = common::format_endpoint(&common::HOST.to_string(), port, false);
    let channel = Channel::from_shared(url)
        .unwrap()
        .connect()
        .await
        .expect("unable to connect to the health service");
    HealthClient::new(channel)
}

async fn check(client: &mut HealthClient<Channel>, service: &str) -> ServingStatus {
    client
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await
        .expect("health check failed")
        .into_inner()
        .status()
}

async fn watch(
    client: &mut HealthClient<Channel>,
    service: &str,
) -> Streaming<HealthCheckResponse> {
    client
        .watch(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await
        .expect("health watch failed")
        .into_inner()
}

/// Returns the next status sent on a `Watch` stream, or `None` if the stream ended.
async fn next_status(stream: &mut Streaming<HealthCheckResponse>) -> Option<ServingStatus> {
    tokio::time::timeout(TIMEOUT, stream.message())
        .await
        .expect("no status received on the watch stream")
        .expect("watch stream failed")
        .map(|res| res.status())
}

/// Both health services answer without an API key, even when API keys are enforced, while the
/// Flight service still requires one.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_health_without_api_key(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool)
        .enable_api_key()
        .build()
        .await;

    let mut health = health_client(server.port()).await;

    assert_eq!(
        check(&mut health, LIVENESS_SERVICE).await,
        ServingStatus::Serving
    );
    assert_eq!(
        check(&mut health, READINESS_SERVICE).await,
        ServingStatus::Serving
    );

    let mut readiness = watch(&mut health, READINESS_SERVICE).await;
    assert_eq!(
        next_status(&mut readiness).await,
        Some(ServingStatus::Serving)
    );
    drop(readiness);

    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    let res = actions::sequence_create(&mut client, "no_api_key_seq", "").await;
    assert_eq!(res.unwrap_err().code(), tonic::Code::PermissionDenied);

    server.shutdown().await;
}

/// Readiness turns `NOT_SERVING` when PostgreSQL becomes unreachable and back to `SERVING` once
/// it's reachable again, while liveness stays `SERVING` throughout.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_readiness_follows_database(pool: sqlx::Pool<db::DatabaseType>) {
    let upstream = pool.connect_options();
    let proxy = TcpProxy::start((upstream.get_host().to_owned(), upstream.get_port())).await;

    // Fails fast while the proxy is cut.
    let proxied_pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(
            (*upstream)
                .clone()
                .host(&common::HOST.to_string())
                .port(proxy.port()),
        )
        .await
        .expect("unable to connect to the database through the proxy");

    let server = common::ServerBuilder::new(common::HOST, proxied_pool.clone())
        .with_heartbeat(HEARTBEAT_INTERVAL)
        .build()
        .await;

    let mut health = health_client(server.port()).await;
    let mut readiness = watch(&mut health, READINESS_SERVICE).await;
    assert_eq!(
        next_status(&mut readiness).await,
        Some(ServingStatus::Serving)
    );

    proxy.cut();

    assert_eq!(
        next_status(&mut readiness).await,
        Some(ServingStatus::NotServing)
    );
    assert_eq!(
        check(&mut health, LIVENESS_SERVICE).await,
        ServingStatus::Serving
    );

    proxy.restore();

    assert_eq!(
        next_status(&mut readiness).await,
        Some(ServingStatus::Serving)
    );

    drop(readiness);
    server.shutdown().await;
    // Otherwise the test database can't be dropped.
    proxied_pool.close().await;
}

/// Open `Watch` streams don't hold up a graceful shutdown: they're told `NOT_SERVING`, then end.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_shutdown_ends_watch_streams(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let mut health = health_client(server.port()).await;
    let mut streams = Vec::new();
    for service in [LIVENESS_SERVICE, READINESS_SERVICE] {
        let mut stream = watch(&mut health, service).await;
        assert_eq!(next_status(&mut stream).await, Some(ServingStatus::Serving));
        streams.push(stream);
    }

    tokio::time::timeout(TIMEOUT, server.shutdown())
        .await
        .expect("server didn't shut down with watch streams open");

    for mut stream in streams {
        assert_eq!(
            next_status(&mut stream).await,
            Some(ServingStatus::NotServing)
        );
        assert_eq!(next_status(&mut stream).await, None);
    }
}
