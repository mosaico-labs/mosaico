//! Topic-related actions.
use arrow::error::ArrowError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use log::{info, trace, warn};
use mosaicod_core::{
    self as core,
    types::{self, MetadataBlob},
};
use mosaicod_ext;
use mosaicod_facade::{self as facade};
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{
    self as marshal, ActionResponse, ClusterTimestampRange, Ontology, flight::FilterTimestampRange,
    responses,
};
use mosaicod_query as query;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::flight::DoActionStream;

const MAX_BUFFER_CHANNEL_SIZE: usize = 128;

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &facade::Context,
    name: String,
    session_uuid: String,
    serialization_format: types::Format,
    ontology_tag: String,
    user_metadata_str: &str,
) -> grpc_common::Result<ActionResponse> {
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
pub async fn delete(ctx: &facade::Context, locator: String) -> grpc_common::Result<ActionResponse> {
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
) -> grpc_common::Result<ActionResponse> {
    info!("notification for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notification_type = notification_type
        .parse()
        .map_err(|_| grpc_common::Error::invalid_notification_type(&notification_type))?;

    facade::topic::notify(ctx, &topic_handle, notification_type, msg).await?;

    Ok(ActionResponse::topic_notification_create())
}

/// Lists all notifications for a topic.
pub async fn notification_list(
    ctx: &facade::Context,
    locator: String,
) -> grpc_common::Result<ActionResponse> {
    info!("notification list for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;
    let notifications = facade::topic::notification_list(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(
    ctx: &facade::Context,
    locator: String,
) -> grpc_common::Result<ActionResponse> {
    warn!("notification purge for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    facade::topic::notification_purge(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_purge())
}

/// Builds a filtered streaming query over a topic.
///
/// Reads the topic's Parquet data with the provided ontology_filter and
/// optional ts window applied as predicate pushdown, returning a lazy
/// SendableRecordBatchStream of matching record batches.
pub async fn query_by_timestamp(
    context: &facade::Context,
    handle: &facade::topic::Handle,
    ts: Option<types::TimestampRange>,
    ontology_filter: query::OntologyFilter,
) -> grpc_common::Result<SendableRecordBatchStream> {
    let meta = facade::topic::metadata(context, handle).await?;
    let format = meta.ontology_metadata.properties.serialization_format;
    let topic_tag = &meta.ontology_metadata.properties.ontology_tag;

    // Check if filter tag is registered before execute query
    for filter_tag in ontology_filter.ontology_tags() {
        if filter_tag != topic_tag {
            return Err(core::Error::bad_request(format!(
                "wrong ontology tag {filter_tag}, topic uses {topic_tag}"
            )))?;
        }
    }

    let batch_size = facade::topic::compute_optimal_batch_size(context, handle)
        .await
        .ok();

    let path_in_store = handle
        .path_in_store()
        .ok_or(facade::Error::MissingDbData(handle.locator().to_string()))?;

    let mut result = context
        .timeseries_querier
        .read(path_in_store.data_folder_path(), format, batch_size)
        .await?;

    if let Some(ts_range) = ts {
        result = result.filter_by_timestamp_range(ts_range)?;
    }

    result = result.filter(ontology_filter.into_expr_group())?;

    Ok(result.stream().await?)
}

pub async fn filter_clusterize(
    ctx: &facade::Context,
    locator: String,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<FilterTimestampRange>,
) -> grpc_common::Result<DoActionStream> {
    info!("filter clusterize for {}", locator);

    // Validation and conversion to TimestampRange
    let ts: Option<types::TimestampRange> = match timestamp_range.as_ref() {
        Some(ftr) => {
            ftr.validate()?;
            Some(ftr.into())
        }
        None => None,
    };

    // Check ontology parameter
    if ontology.len() > 1 || ontology.is_empty() {
        return Err(core::Error::bad_request(format!(
            "Only 1 filtering condition is allowed, found {}",
            ontology.len()
        )))?;
    }

    // Check clustering_dt_ns
    let dt_ns = if clustering_dt_ns == 0 {
        u64::MAX
    } else {
        clustering_dt_ns
    };

    // Setup query
    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;
    let timestamp_column = core::params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP.to_owned();
    let ontology_filter = ontology.try_into()?;

    // RecordBatch stram filtered by timestamp if any and ontology
    let batch_stream = query_by_timestamp(ctx, &topic_handle, ts, ontology_filter)
        .await?
        .map(|item| item.map_err(|e| ArrowError::ExternalError(Box::new(e))));

    // Channel Setup
    // Bridges the background clustering task with the gRPC response stream.
    // The channel carries ['Result<Cluster, ClusteringError>`]: the task pushes
    // successful clusters and streaming-time errors, in the order they occur.
    // The downstream `map` converts each variant into the corresponding Flight
    // payload or [`tonic::Status`], so the client sees errors interleaved with
    // data at the exact position where they happened.
    let (tx, rx) = mpsc::channel::<
        std::result::Result<
            mosaicod_ext::arrow_filter::Cluster,
            mosaicod_ext::arrow_filter::ClusteringError,
        >,
    >(MAX_BUFFER_CHANNEL_SIZE);

    tokio::spawn(async move {
        let _ = mosaicod_ext::arrow_filter::topic_filter_clusterize(
            batch_stream,
            dt_ns,
            &timestamp_column,
            tx,
        )
        .await;
    });

    let stream = ReceiverStream::new(rx).map(|res| match res {
        Ok(cluster) => cluster_to_flight_result(cluster),
        Err(e) => Err(e.to_status()),
    });

    Ok(Box::pin(stream))
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
