use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum ClusteringError {
    #[error("timestamp column `{0}` not found")]
    ColumnNotFound(String),

    #[error("timestamp column `{0}` has null values")]
    HasNulls(String),

    #[error("output channel closed")]
    ChannelClosed,

    #[error("clustering_dt_ns must be greater than 0")]
    UnsupportedClusteringDt,

    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

impl ClusteringError {
    pub fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match self {
            ClusteringError::ColumnNotFound(col) => {
                Status::failed_precondition(format!("timestamp column `{col}` not found"))
            }
            ClusteringError::HasNulls(col) => {
                Status::data_loss(format!("timestamp column `{col}` has null values"))
            }
            ClusteringError::UnsupportedClusteringDt => {
                Status::invalid_argument("clustering_dt_ns must be greater than 0")
            }
            ClusteringError::Arrow(arr) => Status::internal(format!("arrow error: {arr}")),
            ClusteringError::ChannelClosed => Status::cancelled("client disconnected"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cluster {
    pub start_ns: u64,
    pub end_ns: u64,
    pub id: u64,
}

/// Sends a clustering error through the output channel.
///
/// Returns Ok(()) if the error was delivered (which is the only "success" path
/// from the caller's perspective: it means the consumer will see the error).
/// Returns `Err(ChannelClosed)` if the channel is closed.
async fn forward_err(
    out: mpsc::Sender<std::result::Result<Cluster, ClusteringError>>,
    e: ClusteringError,
) -> std::result::Result<(), ClusteringError> {
    out.send(Err(e))
        .await
        .map_err(|_| ClusteringError::ChannelClosed)
}

/// Clusters strictly monotonically increasing timestamps from a single topic's
/// filtered [`RecordBatch`] stream, forwarding each closed cluster on `out`.
///
/// Two consecutive timestamps `t_i, t_{i+1}` belong to the same cluster iff
/// `t_{i+1} - t_i <= clustering_dt_ns`. Clusters are emitted as soon as they
/// close; the final open one is emitted on stream end. State is `O(1)` and
/// back-pressure is propagated through `out`.
///
/// # Preconditions
///
/// * Timestamps must be strictly monotonically increasing within and across
///   batches (not validated; violations yield undefined clusters).
/// * `clustering_dt_ns >= 1`.
/// # Arguments
///
/// * `batch_stream`: stream of `Result<RecordBatch, ArrowError>` produced upstream.
/// * `clustering_dt_ns`: inclusive gap threshold in nanoseconds.
/// * `timestamp_column`: name of the `UInt64` column carrying timestamps.
/// * `out`: channel for emitted [`Cluster`]s. If the receiver is dropped, the
///   next send fails, the function returns
///   [`ClusteringError::ChannelClosed`] without consuming the remainder of
///   `input`, and any open cluster is discarded.
///
pub async fn topic_filter_clusterize<S>(
    mut batch_stream: S,
    clustering_dt_ns: u64,
    timestamp_column: &str,
    out: mpsc::Sender<Result<Cluster, ClusteringError>>,
) -> Result<(), ClusteringError>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>> + Unpin,
{
    let mut current: Option<Cluster> = None;
    let mut prev_ts: Option<u64> = None;
    let mut id: u64 = 0;

    if clustering_dt_ns == 0 {
        return forward_err(out, ClusteringError::UnsupportedClusteringDt).await;
    }

    while let Some(batch) = batch_stream.next().await {
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return forward_err(out, ClusteringError::Arrow(e)).await,
        };

        let ts = match extract_timestamps(&batch, timestamp_column) {
            Ok(ts) => ts,
            Err(e) => return forward_err(out, e).await,
        };

        if ts.is_empty() {
            continue;
        }

        for &t in ts {
            match (current.as_mut(), prev_ts) {
                (None, _) => {
                    current = Some(Cluster {
                        start_ns: t,
                        end_ns: t,
                        id,
                    });
                }
                (Some(curr), Some(prev)) => {
                    if (t - prev) <= clustering_dt_ns {
                        curr.end_ns = t;
                    } else {
                        out.send(Ok(*curr))
                            .await
                            .map_err(|_| ClusteringError::ChannelClosed)?;
                        id += 1;
                        // t is the first point of the next cluster, not just the gap-closing one
                        current = Some(Cluster {
                            start_ns: t,
                            end_ns: t,
                            id,
                        });
                    }
                }
                (Some(_), None) => unreachable!("prev_ts is Some whenever current is Some"),
            }

            prev_ts = Some(t);
        }
    }

    if let Some(cluster) = current {
        out.send(Ok(cluster))
            .await
            .map_err(|_| ClusteringError::ChannelClosed)?;
    }

    Ok(())
}

fn extract_timestamps<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<&'a [u64], ClusteringError> {
    let timestamp_array = batch
        .column_by_name(column)
        .ok_or_else(|| ClusteringError::ColumnNotFound(column.to_string()))?;

    if timestamp_array.null_count() > 0 {
        return Err(ClusteringError::HasNulls(column.to_string()));
    }

    // unwrap here is safe because the timestamp's datatype is enforced in do_put session.
    let timestamp_array = timestamp_array
        .as_primitive_opt::<Int64Type>()
        .unwrap()
        .values();

    // SAFETY: reinterpreting &[i64] as &[u64] is sound here because:
    //   - i64 and u64 have the same size (8 bytes) and alignment, so the
    //     pointer cast preserves the memory layout of the slice;
    //   - the timestamp column is enforced to be Int64 at ingest time
    //     (do_put session), so we never reach this code with a different type;
    //   - timestamps represent nanoseconds since the Unix epoch and are always
    //     non-negative, hence every value fits in the positive range of i64
    //     and maps one-to-one to u64 without any change in numeric meaning;
    let ts: &[u64] = unsafe {
        std::slice::from_raw_parts(
            timestamp_array.as_ptr() as *const u64,
            timestamp_array.len(),
        )
    };
    Ok(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::array::UInt64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::{Field, Schema};
    use futures::stream;
    use std::sync::Arc;

    fn batch(ts: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let ts: Vec<i64> = ts.iter().map(|&v| v as i64).collect();
        let array = Int64Array::from(ts);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    async fn run_raw(
        batches: Vec<RecordBatch>,
        clustering_dt: u64,
    ) -> Vec<std::result::Result<Cluster, ClusteringError>> {
        let s = stream::iter(batches.into_iter().map(Ok::<_, ArrowError>));
        let (tx, mut rx) = mpsc::channel::<std::result::Result<Cluster, ClusteringError>>(64);

        let handle =
            tokio::spawn(async move { topic_filter_clusterize(s, clustering_dt, "ts", tx).await });

        let mut out = Vec::new();
        while let Some(c) = rx.recv().await {
            out.push(c);
        }

        handle
            .await
            .expect("task panicked")
            .expect("task returned ChannelClosed unexpectedly");
        out
    }

    async fn run(batches: Vec<RecordBatch>, clustering_dt: u64) -> Vec<Cluster> {
        run_raw(batches, clustering_dt)
            .await
            .into_iter()
            .map(|item| item.expect("clustering returned an error"))
            .collect()
    }

    async fn run_err(
        batches: Vec<RecordBatch>,
        clustering_dt: u64,
        column: &'static str,
    ) -> ClusteringError {
        let s = stream::iter(batches.into_iter().map(Ok::<_, ArrowError>));
        let (tx, mut rx) = mpsc::channel::<std::result::Result<Cluster, ClusteringError>>(64);

        let handle =
            tokio::spawn(
                async move { topic_filter_clusterize(s, clustering_dt, column, tx).await },
            );

        let mut captured = None;
        while let Some(item) = rx.recv().await {
            if let Err(e) = item {
                captured = Some(e);
            }
        }

        handle
            .await
            .expect("task panicked")
            .expect("task returned ChannelClosed unexpectedly");
        captured.expect("expected clustering error in channel, found none")
    }
    #[tokio::test]
    async fn empty_stream_emits_nothing() {
        let clusters = run(vec![], 100).await;
        assert!(clusters.is_empty());
    }

    #[tokio::test]
    async fn empty_batches_are_skipped() {
        let clusters = run(vec![batch(&[]), batch(&[10, 11]), batch(&[])], 5).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 11,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn single_timestamp_yields_singleton_cluster() {
        let clusters = run(vec![batch(&[42])], 100).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 42,
                end_ns: 42,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn all_within_threshold_yields_single_cluster() {
        let clusters = run(vec![batch(&[10, 11, 12, 13, 14])], 5).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 14,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn all_above_threshold_yields_singleton_clusters() {
        let clusters = run(vec![batch(&[10, 100, 1_000])], 5).await;
        assert_eq!(
            clusters,
            vec![
                Cluster {
                    start_ns: 10,
                    end_ns: 10,
                    id: 0
                },
                Cluster {
                    start_ns: 100,
                    end_ns: 100,
                    id: 1
                },
                Cluster {
                    start_ns: 1_000,
                    end_ns: 1_000,
                    id: 2
                },
            ]
        );
    }

    #[tokio::test]
    async fn mixed_clusters_in_single_batch() {
        let clusters = run(vec![batch(&[0, 2, 4, 6, 106, 109, 112])], 5).await;
        assert_eq!(
            clusters,
            vec![
                Cluster {
                    start_ns: 0,
                    end_ns: 6,
                    id: 0
                },
                Cluster {
                    start_ns: 106,
                    end_ns: 112,
                    id: 1
                },
            ]
        );
    }

    #[tokio::test]
    async fn cluster_spans_batch_boundary() {
        let clusters = run(vec![batch(&[10, 11]), batch(&[12, 13])], 5).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 13,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn gap_across_batch_boundary_splits_clusters() {
        let clusters = run(vec![batch(&[10, 11]), batch(&[200, 201])], 5).await;
        assert_eq!(
            clusters,
            vec![
                Cluster {
                    start_ns: 10,
                    end_ns: 11,
                    id: 0
                },
                Cluster {
                    start_ns: 200,
                    end_ns: 201,
                    id: 1
                },
            ]
        );
    }

    #[tokio::test]
    async fn closing_timestamp_starts_new_cluster() {
        let clusters = run(vec![batch(&[10, 11, 100, 101])], 5).await;
        assert_eq!(
            clusters,
            vec![
                Cluster {
                    start_ns: 10,
                    end_ns: 11,
                    id: 0
                },
                Cluster {
                    start_ns: 100,
                    end_ns: 101,
                    id: 1
                },
            ]
        );
    }

    #[tokio::test]
    async fn gap_equal_to_threshold_keeps_same_cluster() {
        let clusters = run(vec![batch(&[10, 15, 20])], 5).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 20,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn gap_just_above_threshold_splits() {
        let clusters = run(vec![batch(&[10, 16])], 5).await;
        assert_eq!(
            clusters,
            vec![
                Cluster {
                    start_ns: 10,
                    end_ns: 10,
                    id: 0
                },
                Cluster {
                    start_ns: 16,
                    end_ns: 16,
                    id: 1
                },
            ]
        );
    }

    #[tokio::test]
    async fn threshold_larger_than_total_span_yields_single_cluster() {
        let clusters = run(vec![batch(&[10, 20, 30])], 1_000_000).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 30,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn cluster_ids_increment_from_zero() {
        let clusters = run(vec![batch(&[0, 100, 200, 300])], 5).await;
        let ids: Vec<u64> = clusters.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn missing_column_returns_error() {
        let err = run_err(vec![batch(&[1, 2, 3])], 5, "does_not_exist").await;
        assert!(matches!(err, ClusteringError::ColumnNotFound(name) if name == "does_not_exist"));
    }

    #[tokio::test]
    async fn nulls_in_column_return_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::UInt64, true)]));
        let arr = UInt64Array::from(vec![Some(1_u64), None, Some(3)]);
        let b = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let err = run_err(vec![b], 5, "ts").await;
        assert!(matches!(err, ClusteringError::HasNulls(name) if name == "ts"));
    }

    #[tokio::test]
    async fn cluster_spans_four_batches() {
        let clusters = run(
            vec![
                batch(&[10, 11]),
                batch(&[12, 13]),
                batch(&[14, 15]),
                batch(&[16, 17]),
            ],
            5,
        )
        .await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 17,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn empty_batch_in_middle_of_cluster_does_not_break_it() {
        let clusters = run(vec![batch(&[10, 11]), batch(&[]), batch(&[12, 13])], 5).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 13,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn leading_and_trailing_empty_batches_are_ignored() {
        let clusters = run(
            vec![
                batch(&[]),
                batch(&[]),
                batch(&[10, 11]),
                batch(&[]),
                batch(&[]),
            ],
            5,
        )
        .await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 10,
                end_ns: 11,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn stream_error_is_propagated_and_open_cluster_is_lost() {
        let s = stream::iter(vec![
            Ok::<_, ArrowError>(batch(&[10, 11])),
            Err(ArrowError::ComputeError("upstream failed".into())),
        ]);
        let (tx, mut rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move { topic_filter_clusterize(s, 5, "ts", tx).await });

        let mut received = Vec::new();
        while let Some(c) = rx.recv().await {
            received.push(c);
        }

        handle
            .await
            .expect("task panicked")
            .expect("task returned ChannelClosed unexpectedly");

        assert_eq!(received.len(), 1);
        assert!(matches!(received[0], Err(ClusteringError::Arrow(_))));
    }

    #[tokio::test]
    async fn back_pressure_with_capacity_one_channel_still_delivers_every_cluster() {
        let s = stream::iter(
            vec![batch(&[0, 100, 200, 300, 400, 500, 600, 700])]
                .into_iter()
                .map(Ok::<_, ArrowError>),
        );
        let (tx, mut rx) = mpsc::channel::<std::result::Result<Cluster, ClusteringError>>(1);

        let handle = tokio::spawn(async move { topic_filter_clusterize(s, 5, "ts", tx).await });

        let mut got: Vec<std::result::Result<Cluster, ClusteringError>> = Vec::new();
        while let Some(c) = rx.recv().await {
            got.push(c);
        }
        handle
            .await
            .expect("task panicked")
            .expect("clustering failed");

        assert_eq!(got.len(), 8);
        for (i, c) in got.iter().enumerate() {
            let ts = (i as u64) * 100;
            assert_eq!(
                *c.as_ref().unwrap(),
                Cluster {
                    start_ns: ts,
                    end_ns: ts,
                    id: i as u64,
                }
            );
        }
    }

    #[tokio::test]
    async fn timestamps_near_u64_max_do_not_overflow() {
        let near_max = u64::MAX - 10;
        let clusters = run(vec![batch(&[near_max, u64::MAX - 5, u64::MAX])], 100).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: near_max,
                end_ns: u64::MAX,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn max_threshold_collapses_everything_into_one_cluster() {
        let clusters = run(vec![batch(&[0, u64::MAX / 2, u64::MAX])], u64::MAX).await;
        assert_eq!(
            clusters,
            vec![Cluster {
                start_ns: 0,
                end_ns: u64::MAX,
                id: 0
            }]
        );
    }

    #[tokio::test]
    async fn many_singleton_clusters_have_sequential_ids() {
        let ts: Vec<u64> = (0..1_000).map(|i| i * 1_000).collect();
        let clusters = run(vec![batch(&ts)], 5).await;

        assert_eq!(clusters.len(), 1_000);
        for (i, c) in clusters.iter().enumerate() {
            assert_eq!(c.id, i as u64);
            assert_eq!(c.start_ns, c.end_ns);
            assert_eq!(c.start_ns, (i as u64) * 1_000);
        }
    }

    #[tokio::test]
    async fn error_arrives_in_order_after_successful_clusters() {
        let good = batch(&[1, 100]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "not_ts",
            DataType::Int64,
            false,
        )]));
        let arr = Int64Array::from(vec![200i64]);
        let bad = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let items = run_raw(vec![good, bad], 10).await;

        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].as_ref().unwrap(),
            &Cluster {
                start_ns: 1,
                end_ns: 1,
                id: 0
            }
        );
        assert!(matches!(items[1], Err(ClusteringError::ColumnNotFound(_))));
    }

    #[tokio::test]
    async fn dropping_receiver_returns_channel_closed() {
        let s = stream::iter(
            vec![batch(&[0, 100, 200])]
                .into_iter()
                .map(Ok::<_, ArrowError>),
        );
        let (tx, rx) = mpsc::channel::<std::result::Result<Cluster, ClusteringError>>(1);

        drop(rx);

        let result = topic_filter_clusterize(s, 5, "ts", tx).await;
        assert!(matches!(result, Err(ClusteringError::ChannelClosed)));
    }

    #[tokio::test]
    async fn dt_zero_is_reported_through_channel() {
        let items = run_raw(vec![batch(&[1, 2, 3])], 0).await;
        assert_eq!(items.len(), 1);
        assert!(matches!(
            items[0],
            Err(ClusteringError::UnsupportedClusteringDt)
        ));
    }
}
