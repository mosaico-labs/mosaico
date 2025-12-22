use super::FacadeError;
use crate::{params, query, repo, types};
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, trace};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

/// Facade used to perform queries in the system, it will handle the dependencies
/// between different components (mainly `query` and `repo` modules).
///
/// All complex query logics needs to be implemented inside this facade.
pub struct FacadeQuery {}

impl FacadeQuery {
    pub async fn query(
        filter: query::Filter,
        ts_gw: query::TimeseriesGwRef,
        repo: repo::Repository,
    ) -> Result<types::SequenceTopicGroups, FacadeError> {
        let mut result: Option<types::SequenceTopicGroups> = None;

        let (seq_filt, top_filt, on_filt) = filter.into_parts();

        let no_topic_filter = (seq_filt.is_none() || seq_filt.as_ref().unwrap().is_empty())
            && (top_filt.is_none() || top_filt.as_ref().unwrap().is_empty());

        // This holds the set of topic that the user requested with topic and sequence filters
        let on_topics = {
            let mut cx = repo.connection();
            repo::topic_from_query_filter(&mut cx, seq_filt, top_filt).await?
        };
        let on_topics = Arc::new(on_topics);

        if no_topic_filter {
            trace!("search unrestricted (no prior topics)");
        } else {
            trace!("restricting search on #{} topics", on_topics.len());
        }

        // Here we split the ontology filter elements and group them by `ontology_tag`.
        // Each group becomes a query to find the corresponding chunks, this is done
        // since there is a mutual mapping between ontology and chunks (a chunk holds data of a
        // single ontology model).
        // Each expression group is processed concurrently using unordered futures, and returns a
        // `SequenceTopicGroups`.
        // At the end sequence topic groups are merged (sequences are interseted and topic are
        // joined) before return.
        if let Some(ontology_filter) = on_filt {
            let start = Instant::now();

            let ontology_tag_expr_groups =
                ontology_filter.into_expr_group().split_by_ontology_tag();
            let expression_groups_count = ontology_tag_expr_groups.len();

            for ontology_tag_exprs in ontology_tag_expr_groups {
                if ontology_tag_exprs.group.is_empty() {
                    continue;
                }
                trace!(
                    "starting search for ontology tag `{}` (expression groups: {})",
                    // grp is ensured to contain at least one element (previous check)
                    ontology_tag_exprs.group[0].ontology_field().ontology_tag(),
                    &expression_groups_count,
                );

                let ts_engine = ts_gw.clone();
                let max_concurrent = params::configurables().max_concurrent_chunk_queries;
                let semaphore = Arc::new(Semaphore::new(max_concurrent));
                let mut search_jobs = FuturesUnordered::new();

                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    FacadeError::ConcurrencyError(format!("semaphore acquire failed: {e}"))
                })?;

                let repo_clone = repo.clone();
                let on_topics = on_topics.clone();

                search_jobs.push(async move {
                    let _permit = permit; // sentinel lock

                    let mut cx = repo_clone.connection();
                    let chunks = repo::chunks_from_filters(
                        &mut cx,
                        ontology_tag_exprs.clone(),
                        Some(&on_topics),
                    )
                    .await?;
                    trace!("found {} chunks for provided filter", chunks.len());

                    // Extract a lookup structure holding all the topics for the current chunk set
                    let on_topics = if no_topic_filter {
                        None
                    } else {
                        Some(&on_topics)
                    };
                    let topics_map = pre_fetch_topics(&mut cx, &chunks, on_topics).await?;

                    // Store which topic had a positive data file search
                    let mut topics_with_data: HashSet<i32> = HashSet::new();

                    for chunk in chunks {
                        let topic = topics_map.get(&chunk.topic_id);
                        if topic.is_none() {
                            debug!(
                                "can't find a topic associated with chunk `{}`, skipping",
                                chunk.chunk_uuid
                            );
                            return Ok::<_, FacadeError>(types::SequenceTopicGroups::empty());
                        }
                        let topic = topic.unwrap();

                        trace!(
                            "searching data file `{}`",
                            chunk.data_file().to_string_lossy()
                        );

                        let serialization_format =
                            topic.serialization_format().ok_or_else(|| {
                                FacadeError::MissingSerializationFormat(
                                    topic.locator_name.to_owned(),
                                )
                            })?;

                        let qr = ts_engine
                            .read(chunk.data_file(), serialization_format, None)
                            .await?;

                        let qr = qr.filter(ontology_tag_exprs.to_owned())?;

                        if qr.has_rows().await? {
                            trace!("found matching records in chunk");
                            topics_with_data.insert(topic.topic_id);
                        } else {
                            trace!("discarding chunk `{}` for no query match", chunk.chunk_uuid);
                        }
                    }

                    trace!("topics with positive match: {:?}", topics_with_data);
                    let topics = topics_map
                        .values()
                        .filter(|e| topics_with_data.contains(&e.topic_id));
                    let group = repo::sequences_group_from_topics(&mut cx, topics).await?;

                    Ok::<_, FacadeError>(group.into())
                });

                while let Some(groups) = search_jobs.next().await {
                    if let Some(r) = result {
                        result = Some(r.merge(groups?));
                    } else {
                        result = Some(groups?);
                    }
                }

                let elapsed = start.elapsed();

                debug!(
                    "expression groups search required {}us ({:.2}us/group, {} concurrent)",
                    elapsed.as_micros(),
                    elapsed.as_micros() as f64 / expression_groups_count.max(1) as f64,
                    max_concurrent
                );
            }
        } else {
            // No ontology filter branch, simply retrieve
            let mut cx = repo.connection();
            let group = repo::sequences_group_from_topics(&mut cx, on_topics.iter()).await?;
            result = Some(group.into());
        }

        Ok(result.unwrap_or_default())
    }
}

/// A map holding pairs of (topic_id, topic_record) for easy lookup
type TopicMap = HashMap<i32, repo::TopicRecord>;

/// Pre-fetch all topics needed for chunks to avoid `N+1` queries
async fn pre_fetch_topics(
    cx: &mut repo::Cx<'_>,
    chunks: &[repo::Chunk],
    on_topics: Option<&Arc<Vec<repo::TopicRecord>>>,
) -> Result<Arc<TopicMap>, FacadeError> {
    let topic_map = if let Some(topics) = on_topics {
        topics.iter().map(|t| (t.topic_id, t.clone())).collect()
    } else {
        let chunk_topic_ids: Vec<i32> = chunks.iter().map(|c| c.topic_id).collect();
        let fetched_topics = repo::topic_find_by_ids(cx, &chunk_topic_ids).await?;
        fetched_topics
            .into_iter()
            .map(|t| (t.topic_id, t))
            .collect()
    };

    Ok(Arc::new(topic_map))
}
