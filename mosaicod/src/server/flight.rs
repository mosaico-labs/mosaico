use crate::server::endpoints;
use crate::server::errors::ServerError;
use crate::{marshal, params, query, repo, store};
use arrow_flight::decode::FlightDataDecoder;
use arrow_flight::{
    Action as FlightAction, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use log::{error, trace};
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// To stop the server use the following command on
/// `ShutdownNotifier`
/// To trigger the server shutdown use [`notify_waiters()`] function.
pub type ShutdownNotifier = Arc<Notify>;

pub struct Config {
    pub host: String,
    pub port: u16,
}

/// Start mosaico Apache Arrow Flight service
pub async fn start(
    config: Config,
    store: store::StoreRef,
    repo: repo::Repository,
    shutdown: Option<ShutdownNotifier>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", config.host, config.port).parse()?;

    let service = MosaicoFlightService::try_new(store, repo)?;

    let svc = FlightServiceServer::new(service);

    let server = Server::builder().add_service(
        svc.max_decoding_message_size(params::configurables().max_message_size_in_bytes)
            .max_encoding_message_size(params::configurables().max_message_size_in_bytes),
    );

    if let Some(shutdown_notifier) = shutdown {
        server
            .serve_with_shutdown(addr, async {
                shutdown_notifier.notified().await;
                trace!("received shutdown notification");
            })
            .await?;
    } else {
        server.serve(addr).await?;
    }

    Ok(())
}

struct MosaicoFlightService {
    store: store::StoreRef,
    repo: repo::Repository,
    ts_gw: query::TimeseriesRef,
}

impl MosaicoFlightService {
    pub fn try_new(store: store::StoreRef, repo: repo::Repository) -> Result<Self, String> {
        let ts_gw = Arc::new(query::Timeseries::try_new(store.clone()).map_err(|e| e.to_string())?);

        Ok(MosaicoFlightService { store, repo, ts_gw })
    }

    pub fn context(&self) -> endpoints::Context {
        endpoints::Context::new(self.store.clone(), self.repo.clone(), self.ts_gw.clone())
    }
}
#[tonic::async_trait]
impl FlightService for MosaicoFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "handshake is currently unimplemented",
        ))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let criteria = request.into_inner();

        let stream = endpoints::list_flights(self.context(), criteria)
            .await
            .inspect_err(log_server_error)?;

        Ok(Response::new(stream))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();

        let info = endpoints::get_flight_info(self.context(), desc)
            .await
            .inspect_err(log_server_error)?;

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "poll_flight_info is currently unimplemented",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "get_schema is currently unimplemented",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let data_stream = endpoints::do_get(self.context(), ticket)
            .await
            .inspect_err(log_server_error)?;

        // map data stream error (flight error) to a tonic one
        let out_stream = data_stream
            .inspect_err(|e| error!("flight encoding error: {}", e))
            .map_err(|e| Status::internal(format!("flight encoding error: {}", e)));

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();
        let mut decoder = FlightDataDecoder::new(stream.map_err(Into::into));

        endpoints::do_put(self.context(), &mut decoder)
            .await
            .inspect_err(log_server_error)?;

        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn do_action(
        &self,
        request: Request<FlightAction>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let action = marshal::ActionRequest::try_new(action.r#type.as_str(), &action.body)
            .map_err(ServerError::from)
            .inspect_err(log_server_error)?;

        let response = endpoints::do_action(self.context(), action)
            .await
            .inspect_err(log_server_error)?;

        let bytes = response
            .bytes()
            .map_err(ServerError::from)
            .inspect_err(log_server_error)?;

        // Create the stream from the flight result
        let stream = futures::stream::iter(vec![Ok(arrow_flight::Result::new(bytes))]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "list_actions is currently unimplemented",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is currently unimplemented",
        ))
    }
}

/// Log `ServerError` to terminal
///
/// Use this function with `.inspect_err`
fn log_server_error(e: &ServerError) {
    match e {
        ServerError::BadTicket(inner) => {
            error!("{} - {}", e, inner);
        }
        _ => error!("{}", e),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn error_logging() {
        fn my_function() -> Result<(), ServerError> {
            Err(ServerError::Unimplemented)
        }
        let _ = my_function().inspect_err(log_server_error);
    }
}
