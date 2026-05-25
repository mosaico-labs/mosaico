//! Topic-related actions.
use crate::{
    error::{Error, Result},
    flight::DoActionStream,
};
use log::{info, trace, warn};
use mosaicod_core::{
    self as core,
    types::{self, MetadataBlob},
};
use mosaicod_ext;
use mosaicod_facade::{self as facade};
use mosaicod_marshal::{
    self as marshal, ActionResponse, ClusterTimestampRange, Ontology, flight::FilterTimestampRange,
    responses,
};

use arrow::error::ArrowError;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const MAX_BUFFER_CHANNEL_SIZE: usize = 128;

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &facade::Context,
    name: String,
    session_uuid: String,
    serialization_format: types::Format,
    ontology_tag: String,
    user_metadata_str: &str,
) -> Result<ActionResponse> {
    info!("requested resource {} creation", name);

    let user_mdata = marshal::JsonMetadataBlob::try_from_str(user_metadata_str)?;

    let received_uuid: types::Uuid = session_uuid
        .parse()
        .map_err(|_| core::Error::bad_uuid(session_uuid))?;

    let ontology_metadata = types::TopicOntologyMetadata::new(
        types::TopicOntologyProperties {
            serialization_format,
            ontology_tag,
        },
        Some(user_mdata),
    );

    let topic_locator = name.parse::<types::TopicLocator>()?;

    let session_handle = facade::session::Handle::try_from_uuid(ctx, &received_uuid).await?;

    let topic_handle =
        facade::topic::try_create(ctx, topic_locator, &session_handle, ontology_metadata).await?;

    trace!(
        "resource `{}` created with uuid {}",
        topic_handle.locator(),
        topic_handle.uuid(),
    );

    Ok(ActionResponse::topic_create(
        topic_handle.uuid().clone().into(),
    ))
}

/// Deletes a topic (it doesn't matter if it's still open or archived).
pub async fn delete(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    warn!("requested deletion of resource `{}`", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator.clone()).await?;

    facade::topic::delete(ctx, topic_handle, types::allow_data_loss()).await?;

    warn!("resource {} deleted", topic_locator);

    Ok(ActionResponse::topic_delete())
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &facade::Context,
    locator: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse> {
    info!("notification for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notification_type = notification_type
        .parse()
        .map_err(|_| Error::invalid_notification_type(&notification_type))?;

    facade::topic::notify(ctx, &topic_handle, notification_type, msg).await?;

    Ok(ActionResponse::topic_notification_create())
}

/// Lists all notifications for a topic.
pub async fn notification_list(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    info!("notification list for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notifications = facade::topic::notification_list(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    warn!("notification purge for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    facade::topic::notification_purge(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_purge())
}

type FlightTx = mpsc::Sender<std::result::Result<arrow_flight::Result, tonic::Status>>;

pub async fn filter_clusterize(
    ctx: &facade::Context,
    locator: String,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<FilterTimestampRange>,
) -> Result<DoActionStream> {
    info!("filter clusterize for {}", locator);

    // 1. Validation and conversion to TimestampRange
    let ts: Option<types::TimestampRange> = match timestamp_range.as_ref() {
        Some(ftr) => {
            ftr.validate()?;
            Some(ftr.into())
        }
        None => None,
    };

    // 2. Check ontology parameter
    if ontology.len() > 1 || ontology.is_empty() {
        return Err(core::Error::bad_request(format!(
            "Only 1 filtering condition is allowed, found {}",
            ontology.len()
        )))?;
    }

    // 3. Check clustering_dt_ns
    let dt_ns = if clustering_dt_ns == 0 {
        u64::MAX
    } else {
        clustering_dt_ns
    };

    // 4. Setup query
    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;
    let timestamp_column = core::params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP.to_owned();
    let ontology_filter = ontology.try_into()?;

    // 5. RecordBatch stram filtered by timestamp if any and ontology
    let batch_stream = facade::topic::query_by_timestamp(ctx, &topic_handle, ts, ontology_filter)
        .await?
        .map(|item| item.map_err(|e| ArrowError::ExternalError(Box::new(e))));

    // 6. Channel setup
    let (tx, rx) = mpsc::channel::<std::result::Result<arrow_flight::Result, tonic::Status>>(
        MAX_BUFFER_CHANNEL_SIZE,
    );
    let (cluster_tx, cluster_rx) =
        mpsc::channel::<mosaicod_ext::arrow_filter::Cluster>(MAX_BUFFER_CHANNEL_SIZE);

    // 7. Spawn task
    spawn_clusterize_task(
        batch_stream,
        dt_ns,
        timestamp_column,
        cluster_tx,
        tx.clone(),
    );
    spawn_cluster_to_flight_bridge(cluster_rx, tx);

    Ok(Box::pin(ReceiverStream::new(rx)))
}

fn spawn_clusterize_task<S>(
    batch_stream: S,
    clustering_dt_ns: u64,
    timestamp_column: String,
    cluster_tx: mpsc::Sender<mosaicod_ext::arrow_filter::Cluster>,
    flight_tx: FlightTx,
) where
    S: futures::Stream<Item = std::result::Result<arrow::record_batch::RecordBatch, ArrowError>>
        + Send
        + Unpin
        + 'static,
{
    tokio::spawn(async move {
        let result = mosaicod_ext::arrow_filter::topic_filter_clusterize(
            batch_stream,
            clustering_dt_ns,
            &timestamp_column,
            cluster_tx,
        )
        .await;
        if let Err(e) = result {
            let _ = flight_tx
                .send(Err(tonic::Status::internal(format!(
                    "clustering error: {e}"
                ))))
                .await;
        }
    });
}

fn spawn_cluster_to_flight_bridge(
    mut cluster_rx: mpsc::Receiver<mosaicod_ext::arrow_filter::Cluster>,
    flight_tx: FlightTx,
) {
    tokio::spawn(async move {
        while let Some(cluster) = cluster_rx.recv().await {
            let msg = cluster_to_flight_result(cluster);
            if flight_tx.send(msg).await.is_err() {
                warn!("client disconnected, aborting clusterize stream");
                return;
            }
        }
    });
}

fn cluster_to_flight_result(
    cluster: mosaicod_ext::arrow_filter::Cluster,
) -> std::result::Result<arrow_flight::Result, tonic::Status> {
    let res = responses::TopicFilterClusterize {
        ts: ClusterTimestampRange {
            start_ns: cluster.start_ns,
            end_ns: cluster.end_ns,
        },
        id: cluster.id,
    };

    let bytes = ActionResponse::topic_filter_clusterize(res)
        .bytes()
        .map_err(|e| tonic::Status::internal(e.to_string()))?;

    let mut payload = bytes.to_vec();
    payload.push(b'\n');
    Ok(arrow_flight::Result::new(payload))
}
