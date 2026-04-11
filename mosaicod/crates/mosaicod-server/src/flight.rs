use super::{
    error::{PublicErrorGrpcExt, Result, ToStatusExt},
    middleware,
};
use crate::endpoint;
use arrow_flight::{
    Action as FlightAction, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightDataDecoder, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use log::{debug, error, info, warn};
use mosaicod_core::{self as core, params, types};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use mosaicod_store as store;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::{Request, Response, Status, Streaming, codec::CompressionEncoding, transport::Server};

/// To stop the server use the following command on
/// `ShutdownNotifier`
#[derive(Clone)]
pub struct ShutdownNotifier(Arc<Notify>);

impl ShutdownNotifier {
    // Notifies the server to be shut down
    pub fn shutdown(&self) {
        self.0.notify_waiters();
    }

    pub async fn wait_for_shutdown(&self) {
        self.0.notified().await;
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        Self(Arc::new(Notify::new()))
    }
}

#[derive(Clone)]
pub struct TlsConfig {
    pub certificate_file: std::path::PathBuf,
    pub private_key_file: std::path::PathBuf,
}

#[derive(Clone)]
pub struct Config {
    pub host: String,

    /// Default port
    pub port: u16,

    /// If this option is `Some` the server will try to enable TLS
    tls: Option<TlsConfig>,

    /// If this option is true the server will require API keys for every operation
    enable_api_key_management: bool,

    /// Enable gzip encoding in gRPC
    gzip: bool,
}

impl Config {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            tls: None,
            enable_api_key_management: false,
            gzip: false,
        }
    }

    /// Enable TLS
    pub fn tls(&mut self, tls: TlsConfig) {
        self.tls = Some(tls);
    }

    /// Enables gzip compression for both incoming and outgoing gRPC messages.
    pub fn gzip(&mut self, enable: bool) {
        self.gzip = enable;
    }

    /// Enable API key management
    pub fn enable_api_key_management(&mut self) {
        self.enable_api_key_management = true;
    }
}

/// Start mosaico Apache Arrow Flight service
pub async fn start(
    config: Config,
    store: store::StoreRef,
    db: db::Database,
    shutdown: Option<ShutdownNotifier>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", config.host, config.port).parse()?;

    let mut flight_service = MosaicodFlight::try_new(store, db.clone())?;

    if config.enable_api_key_management {
        flight_service.enable_api_key_manegement();
    }

    let mut svc = FlightServiceServer::new(flight_service);

    let mut auth_layer = middleware::AuthLayer::new(db);

    // If API key management is disabled define a custom permission with all permissions
    // and enable permissions passthrough in the auth middleware
    if !config.enable_api_key_management {
        auth_layer = auth_layer.with_permission_passthrough(types::auth::Permission::Manage);
    }
    let layer = tower::ServiceBuilder::new().layer(auth_layer).into_inner();

    let mut builder = Server::builder();

    let mut tls_enabled = false;

    if let Some(tls) = config.tls {
        builder = builder.tls_config(ext::tonic::load_tls_config(
            &tls.certificate_file,
            &tls.private_key_file,
        )?)?;
        tls_enabled = true;
    }

    if !tls_enabled {
        warn!("TLS is currently disabled. Traffic is being sent unencrypted.");
    }
    if !config.enable_api_key_management {
        warn!("API key management is currently disabled.");
    } else if !tls_enabled {
        warn!(
            "API key management is currently enabled but TLS is disabled. Sensitive credential are sent unencrypted and could be intercepted."
        );
    }

    svc = svc
        .max_decoding_message_size(params::params().max_grpc_message_size)
        .max_encoding_message_size(params::params().max_grpc_message_size);

    if config.gzip {
        svc = svc
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        info!("gzip compression for gRPC requests is enabled");
    }

    let server = builder.layer(layer).add_service(svc);

    if let Some(shutdown_notifier) = shutdown {
        server
            .serve_with_shutdown(addr, async {
                shutdown_notifier.wait_for_shutdown().await;
                debug!("received shutdown notification");
            })
            .await?;
    } else {
        server.serve(addr).await?;
    }

    Ok(())
}

struct MosaicodFlight {
    store: store::StoreRef,
    db: db::Database,
    ts_gw: query::TimeseriesEngineRef,

    api_key_management: bool,

    /// Semaphore used to controll the maximum number of concurrent writers
    concurrent_writes_semaphore: Arc<tokio::sync::Semaphore>,
}

impl MosaicodFlight {
    pub fn try_new(store: store::StoreRef, db: db::Database) -> std::result::Result<Self, String> {
        let ts_gw = Arc::new(
            query::TimeseriesEngine::try_new(
                store.clone(),
                params::params().query_engine_memory_pool_size,
            )
            .map_err(|e| e.to_string())?,
        );

        Ok(MosaicodFlight {
            store,
            db,
            ts_gw,
            api_key_management: false,
            concurrent_writes_semaphore: Arc::new(tokio::sync::Semaphore::new(
                params::params().max_concurrent_writes,
            )),
        })
    }

    pub fn enable_api_key_manegement(&mut self) {
        self.api_key_management = true;
    }

    pub fn context(&self) -> facade::Context {
        facade::Context::new(self.store.clone(), self.db.clone(), self.ts_gw.clone())
    }
}

type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

impl MosaicodFlight {
    async fn impl_get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let auth_ctx = auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized())?;
        }

        let desc = request.into_inner();

        let info = endpoint::get_flight_info(&self.context(), desc).await?;

        Ok(Response::new(info))
    }

    async fn impl_list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<ListFlightsStream>> {
        let auth_ctx = auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized())?;
        }

        let criteria = request.into_inner();

        let stream = endpoint::list_flights(&self.context(), criteria).await?;

        // Convert the returned stream inner result error to tonis::Status
        let stream = stream.map(|item| item.log_to_status());

        Ok(Response::new(Box::pin(stream)))
    }

    async fn impl_do_get(&self, request: Request<Ticket>) -> Result<Response<DoGetStream>> {
        let auth_ctx = auth_context(&request)?;
        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized())?;
        }

        let ticket = request.into_inner();

        let data_stream = endpoint::do_get(&self.context(), ticket).await?;

        // map data stream error (flight error) to a tonic one
        let out_stream = data_stream
            .inspect_err(|e| error!("flight encoding error: {}", e))
            .map_err(|e| Status::internal(format!("flight encoding error: {}", e)));

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn impl_do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<DoPutStream>> {
        let auth_ctx = auth_context(&request)?;
        if !auth_ctx.permissions().can_write() {
            Err(core::Error::unauthorized())?;
        }

        let stream = request.into_inner();
        let mut decoder = FlightDataDecoder::new(stream.map_err(Into::into));

        let ctx = endpoint::DoPutContext {
            inner: self.context(),
            concurrent_writes_semaphore: self.concurrent_writes_semaphore.clone(),
        };

        endpoint::do_put(ctx, &mut decoder).await?;

        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn impl_do_action(
        &self,
        request: Request<FlightAction>,
    ) -> Result<Response<DoActionStream>> {
        let auth_ctx = auth_context(&request)?;

        let action = request.into_inner();
        let action = marshal::ActionRequest::try_new(action.r#type.as_str(), &action.body)?;

        let response = endpoint::do_action(&self.context(), action, auth_ctx.permissions()).await?;

        let bytes = response.bytes()?;

        // Create the stream from the flight result
        let stream = futures::stream::iter(vec![Ok(arrow_flight::Result::new(bytes))]);

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Map impl methods to FlightService
#[tonic::async_trait]
impl FlightService for MosaicodFlight {
    type HandshakeStream = HandshakeStream;
    type ListFlightsStream = ListFlightsStream;
    type DoGetStream = DoGetStream;
    type DoPutStream = DoPutStream;
    type DoActionStream = DoActionStream;
    type ListActionsStream = ListActionsStream;
    type DoExchangeStream = DoExchangeStream;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(core::Error::unimplemented()
            .to_public_error()
            .log_to_status())
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        let resp = self.impl_list_flights(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let resp = self.impl_get_flight_info(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(core::Error::unimplemented()
            .to_public_error()
            .log_to_status())
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(core::Error::unimplemented()
            .to_public_error()
            .log_to_status())
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let resp = self.impl_do_get(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let resp = self.impl_do_put(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn do_action(
        &self,
        request: Request<FlightAction>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let resp = self.impl_do_action(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "list_actions is currently unimplemented",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is currently unimplemented",
        ))
    }
}

fn auth_context<T>(req: &Request<T>) -> Result<middleware::AuthContext> {
    req.extensions()
        .get::<middleware::AuthContext>()
        .cloned()
        .ok_or_else(|| core::Error::unauthenticated().into())
}
