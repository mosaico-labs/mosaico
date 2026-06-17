use crate::endpoint;
use arrow_flight::{
    Action as FlightAction, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightDataDecoder, flight_service_server::FlightService,
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use log::error;
use mosaicod_core::{self as core, params};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_grpc_common::{self as grpc_common, PublicErrorGrpcExt, ToStatusExt, middleware};
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use mosaicod_store as store;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

pub struct Service {
    store: store::StoreRef,
    db: db::Database,
    ts_gw: query::TimeseriesEngineRef,
    api_key_management: bool,
    /// Semaphore used to controll the maximum number of concurrent writers
    concurrent_writes_semaphore: Arc<tokio::sync::Semaphore>,
}

impl Service {
    pub fn try_new(store: store::StoreRef, db: db::Database) -> std::result::Result<Self, String> {
        let ts_gw = Arc::new(
            query::TimeseriesEngine::try_new(
                store.clone(),
                params::params().query_engine_memory_pool_size.value,
            )
            .map_err(|e| e.to_string())?,
        );

        Ok(Service {
            store,
            db,
            ts_gw,
            api_key_management: false,
            concurrent_writes_semaphore: Arc::new(tokio::sync::Semaphore::new(
                params::params().max_concurrent_writes.value,
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
pub type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

pub trait IntoStream {
    fn into_stream(self) -> grpc_common::Result<DoActionStream>;
}

impl IntoStream for marshal::ActionResponse {
    /// Wraps a single ActionResponse into a one-item
    /// DoActionStream, as expected by Arrow Flight's do_action endpoint.
    ///
    /// Use this when the handler produces a single payload rather than a
    /// stream of results.
    fn into_stream(self) -> grpc_common::Result<DoActionStream> {
        let bytes = self.bytes()?;
        Ok(Box::pin(futures::stream::once(async move {
            Ok(arrow_flight::Result::new(bytes))
        })))
    }
}

impl Service {
    async fn impl_get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> grpc_common::Result<Response<FlightInfo>> {
        let auth_ctx = middleware::auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let desc = request.into_inner();
        let info = endpoint::get_flight_info(&self.context(), desc).await?;
        Ok(Response::new(info))
    }

    async fn impl_list_flights(
        &self,
        request: Request<Criteria>,
    ) -> grpc_common::Result<Response<ListFlightsStream>> {
        let auth_ctx = middleware::auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let criteria = request.into_inner();
        let stream = endpoint::list_flights(&self.context(), criteria).await?;

        // Convert the returned stream inner result error to tonis::Status
        let stream = stream.map(|item| item.log_to_status());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn impl_do_get(
        &self,
        request: Request<Ticket>,
    ) -> grpc_common::Result<Response<DoGetStream>> {
        let auth_ctx = middleware::auth_context(&request)?;
        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let ticket = request.into_inner();
        let data_stream = endpoint::do_get(&self.context(), ticket).await?;

        // Map data stream error (flight error) to a tonic one
        let out_stream = data_stream
            .inspect_err(|e| error!("flight encoding error: {}", e))
            .map_err(|e| Status::internal(format!("flight encoding error: {}", e)));

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn impl_do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> grpc_common::Result<Response<DoPutStream>> {
        let auth_ctx = middleware::auth_context(&request)?;
        if !auth_ctx.permissions().can_write() {
            Err(core::Error::unauthorized(
                "provided API key does not have WRITE permissions.".to_string(),
            ))?;
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
    ) -> grpc_common::Result<Response<DoActionStream>> {
        let auth_ctx = middleware::auth_context(&request)?;

        let action = request.into_inner();
        let action = marshal::ActionRequest::try_new(action.r#type.as_str(), &action.body)?;

        let stream = endpoint::do_action(&self.context(), action, auth_ctx.permissions()).await?;

        // Create the stream from the flight result
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Map impl methods to FlightService
#[tonic::async_trait]
impl FlightService for Service {
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
