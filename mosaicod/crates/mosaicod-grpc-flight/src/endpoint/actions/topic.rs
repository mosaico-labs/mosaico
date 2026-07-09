//! Topic-related actions.

use ext::arrow_filter::{Cluster, ClusteringError};

use arrow::error::ArrowError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use log::{info, trace, warn};
use mosaicod_core::{
    self as core,
    types::{self, MetadataBlob, TopicLocator},
};
use mosaicod_ext as ext;
use mosaicod_facade::{self as facade};
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{
    self as marshal, ActionResponse, ClusterTimestampRange, Ontology, flight::FilterTimestampRange,
    requests, responses,
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

    let received_session_uuid: types::Uuid = session_uuid
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
    let topic_uuid = facade::topic::try_create(
        ctx,
        &topic_locator,
        &received_session_uuid,
        ontology_metadata,
    )
    .await?;

    trace!(
        "resource `{}` created with uuid {}",
        topic_locator, topic_uuid,
    );

    Ok(ActionResponse::topic_create(topic_uuid.into()))
}

/// Deletes a topic (it doesn't matter if it's still open or archived).
pub async fn delete(ctx: &facade::Context, locator: String) -> grpc_common::Result<ActionResponse> {
    warn!("requested deletion of resource `{}`", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;
    facade::topic::delete(ctx, &topic_locator, types::allow_data_loss()).await?;

    warn!("resource {} deleted", topic_locator);

    Ok(ActionResponse::topic_delete())
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &facade::Context,
    locator: &str,
    notification_type: &str,
    msg: &str,
) -> grpc_common::Result<ActionResponse> {
    info!("notification for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let notification_type = notification_type
        .parse()
        .map_err(|_| grpc_common::Error::invalid_notification_type(notification_type))?;

    facade::topic::notify(ctx, &topic_locator, notification_type, msg).await?;

    Ok(ActionResponse::topic_notification_create())
}

/// Lists all notifications for a topic.
pub async fn notification_list(
    ctx: &facade::Context,
    locator: String,
) -> grpc_common::Result<ActionResponse> {
    info!("notification list for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let notifications = facade::topic::notification_list(ctx, &topic_locator).await?;

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
    facade::topic::notification_purge(ctx, &topic_locator).await?;

    Ok(ActionResponse::topic_notification_purge())
}

/// Builds a filtered streaming query over a topic.
///
/// Reads the topic's Parquet data with the provided ontology_filter and
/// optional ts window applied as predicate pushdown, returning a lazy
/// SendableRecordBatchStream of matching record batches.
pub async fn query_by_timestamp(
    context: &facade::Context,
    locator: &types::TopicLocator,
    ts: Option<types::TimestampRange>,
    ontology_filter: query::OntologyFilter,
) -> grpc_common::Result<SendableRecordBatchStream> {
    let params = facade::topic::streaming_read_prepare(context, locator).await?;
    let format = params
        .metadata
        .ontology_metadata
        .properties
        .serialization_format;
    let topic_tag = &params.metadata.ontology_metadata.properties.ontology_tag;

    // Check if filter tag is registered before execute query
    for filter_tag in ontology_filter.ontology_tags() {
        if filter_tag != topic_tag {
            Err(core::Error::unsupported_ontology_type(format!(
                "wrong ontology tag {filter_tag}, topic uses {topic_tag}"
            )))?;
        }
    }

    let mut result = context
        .timeseries_querier
        .read(
            params.data_folder_path,
            format,
            Some(params.optimal_batch_size),
        )
        .await?;

    if let Some(ts_range) = ts {
        result = result.filter_by_timestamp_range(ts_range)?;
    }

    result = result.filter(ontology_filter.into_expr_group())?;

    Ok(result.stream().await?)
}

type ClusteringResult =
    std::result::Result<ext::arrow_filter::Cluster, ext::arrow_filter::ClusteringError>;

pub async fn filter_clusterize(
    ctx: &facade::Context,
    locator: String,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<FilterTimestampRange>,
) -> grpc_common::Result<DoActionStream> {
    info!("filter clusterize for {}", locator);

    let rx =
        spawn_cluster_stream(ctx, locator, clustering_dt_ns, ontology, timestamp_range).await?;

    let stream = rx.map(|res| match res {
        Ok(cluster) => cluster_to_flight_result(cluster, ActionResponse::topic_filter_clusterize),
        Err(e) => Err(e.to_status()),
    });

    Ok(Box::pin(stream))
}

/// Sets up the full pipeline for a single topic: validates input, opens the
/// filtered RecordBatch stream, spawns the clustering task, and returns the
/// receiver end of the cluster channel.
async fn spawn_cluster_stream(
    ctx: &facade::Context,
    locator: String,
    clustering_dt_ns: u64,
    ontology: Ontology,
    timestamp_range: Option<FilterTimestampRange>,
) -> grpc_common::Result<ReceiverStream<ClusteringResult>> {
    // Check at least one ontology filter is present
    if ontology.is_empty() {
        Err(core::Error::bad_request(format!(
            "At least 1 filtering condition is required, found {}",
            ontology.len()
        )))?;
    }

    // Validation and conversion to TimestampRange
    let ts: Option<types::TimestampRange> = match timestamp_range.as_ref() {
        Some(ftr) => {
            ftr.validate()?;
            Some(ftr.into())
        }
        None => None,
    };

    // Check clustering_dt_ns
    let dt_ns = if clustering_dt_ns == 0 {
        u64::MAX
    } else {
        clustering_dt_ns
    };

    // Setup query
    let topic_locator = locator.parse::<types::TopicLocator>()?;
    let timestamp_column = core::params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP.to_owned();
    let ontology_filter = ontology.try_into()?;

    // RecordBatch stram filtered by timestamp if any and ontology
    let batch_stream = query_by_timestamp(ctx, &topic_locator, ts, ontology_filter)
        .await?
        .map(|item| item.map_err(|e| ArrowError::ExternalError(Box::new(e))));

    // Channel Setup
    // Bridges the background clustering task with the gRPC response stream.
    // The channel carries ['Result<Cluster, ClusteringError>`]: the task pushes
    // successful clusters and streaming-time errors, in the order they occur.
    // The downstream `map` converts each variant into the corresponding Flight
    // payload or [`tonic::Status`], so the client sees errors interleaved with
    // data at the exact position where they happened.
    let (tx, rx) = mpsc::channel::<ClusteringResult>(MAX_BUFFER_CHANNEL_SIZE);

    tokio::spawn(async move {
        let _ =
            ext::arrow_filter::topic_filter_clusterize(batch_stream, dt_ns, &timestamp_column, tx)
                .await;
    });

    Ok(ReceiverStream::new(rx))
}

/// Converts a [`Cluster`] into an [`arrow_flight::Result`] payload.
///
/// `F` is a one-shot closure that selects the correct [`ActionResponse`] variant
/// for the operation being performed, either a clusterize or an intersect result.
fn cluster_to_flight_result<F>(
    cluster: ext::arrow_filter::Cluster,
    action_builder: F,
) -> std::result::Result<arrow_flight::Result, tonic::Status>
where
    F: FnOnce(responses::TopicFilterClusterize) -> ActionResponse,
{
    let res = responses::TopicFilterClusterize {
        ts: ClusterTimestampRange {
            start_ns: cluster.start_ns,
            end_ns: cluster.end_ns,
        },
        id: cluster.id,
    };

    let bytes = action_builder(res)
        .bytes()
        .map_err(|e| tonic::Status::internal(e.to_string()))?;

    let mut payload = bytes.to_vec();
    payload.push(b'\n');
    Ok(arrow_flight::Result::new(payload))
}

pub async fn filter_intersect(
    ctx: &facade::Context,
    topics: Vec<requests::TopicClusterizeParams>,
    intersect_dt_ns: u64,
) -> grpc_common::Result<DoActionStream> {
    info!("filter intersect for {} topics", topics.len());

    if topics.len() < 2 {
        Err(core::Error::bad_request(
            "at least 2 topics are required".to_string(),
        ))?;
    }

    let locators = topics
        .iter()
        .map(|t| {
            t.locator
                .parse::<TopicLocator>()
                .map_err(|_| core::Error::bad_request("invalid topic locator".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // All topics must belong to the same sequence.
    let first_seq = &locators[0].sequence;
    for loc in &locators[1..] {
        if loc.sequence != *first_seq {
            Err(core::Error::bad_request(format!(
                "all topics must belong to sequence {first_seq}, but {loc} belongs to {}",
                loc.sequence
            )))?;
        }
    }

    // One clustering task per topic
    let mut receivers = Vec::with_capacity(topics.len());
    for tfc in topics {
        let rx = spawn_cluster_stream(
            ctx,
            tfc.locator,
            tfc.clustering_dt_ns,
            tfc.ontology,
            tfc.timestamp_range,
        )
        .await?;
        receivers.push(rx);
    }

    let (out_tx, out_rx) = mpsc::channel::<ClusteringResult>(MAX_BUFFER_CHANNEL_SIZE);

    tokio::spawn(async move {
        let _ = intersect_cluster_streams(receivers, intersect_dt_ns, out_tx).await;
    });

    let stream = ReceiverStream::new(out_rx).map(|res| match res {
        Ok(cluster) => cluster_to_flight_result(cluster, ActionResponse::topic_filter_intersect),
        Err(e) => Err(e.to_status()),
    });

    Ok(Box::pin(stream))
}

/// At each step the stream with the smallest end_ns (min_end) is always
/// advanced. If all active clusters overlap within intersect_dt_ns
/// (max_start <= min_end + dt) an intersection is emitted before advancing.
///
/// Natural exhaustion of any stream stops the algorithm: with fewer streams
/// than originally requested, intersections are no longer meaningful.
/// A stream error terminates the intersection immediately: the error is
/// forwarded to the client as the last item and the function returns.
async fn intersect_cluster_streams(
    streams: Vec<ReceiverStream<ClusteringResult>>,
    intersect_dt_ns: u64,
    out: mpsc::Sender<ClusteringResult>,
) -> std::result::Result<(), ext::arrow_filter::ClusteringError> {
    let mut current_cluster: Vec<Cluster> = Vec::new();
    let mut active_streams: Vec<ReceiverStream<ClusteringResult>> = Vec::new();

    for mut stream in streams {
        match advance_one(&mut stream).await {
            Ok(Some(c)) => {
                current_cluster.push(c);
                active_streams.push(stream);
            }
            Ok(None) => {} // stream immediately empty, skip it
            Err(e) => {
                out.send(Err(e))
                    .await
                    .map_err(|_| ClusteringError::ChannelClosed)?;
                return Ok(());
            }
        }
    }

    let mut cluster_id: u64 = 0;

    loop {
        if current_cluster.is_empty() {
            break;
        }

        let mut max_start = u64::MIN;
        let mut min_end = u64::MAX;
        let mut idx = 0;

        for (i, c) in current_cluster.iter().enumerate() {
            if min_end > c.end_ns {
                min_end = c.end_ns;
                idx = i;
            }

            if max_start < c.start_ns {
                max_start = c.start_ns;
            }
        }

        if max_start <= min_end.saturating_add(intersect_dt_ns) {
            let (start_ns, end_ns) = if max_start <= min_end {
                (max_start, min_end)
            } else {
                // Split intersect_dt_ns symmetrically but round the second half
                // up (ceiling) so that lo + hi == intersect_dt_ns exactly.
                // Without this, odd values truncate both halves to the same
                // floor and produce start_ns > end_ns when gap == intersect_dt_ns.
                let lo = intersect_dt_ns / 2;
                let hi = intersect_dt_ns - lo;
                (max_start.saturating_sub(lo), min_end.saturating_add(hi))
            };
            out.send(Ok(Cluster {
                start_ns,
                end_ns,
                id: cluster_id,
            }))
            .await
            .map_err(|_| ClusteringError::ChannelClosed)?;
            cluster_id += 1;
        }

        match advance_one(&mut active_streams[idx]).await {
            Ok(Some(c)) => current_cluster[idx] = c,
            Ok(None) => break, // empty stream
            Err(e) => {
                out.send(Err(e))
                    .await
                    .map_err(|_| ClusteringError::ChannelClosed)?;
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Reads the next item from stream. Returns Some(cluster) on success,
/// None on natural exhaustion, Err(e) on a stream error.
#[inline]
async fn advance_one(
    stream: &mut ReceiverStream<ClusteringResult>,
) -> std::result::Result<Option<ext::arrow_filter::Cluster>, ext::arrow_filter::ClusteringError> {
    match stream.next().await {
        Some(Ok(c)) => Ok(Some(c)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}
