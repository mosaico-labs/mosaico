use bytes::Bytes;

use crate::marshal;
use crate::{repo, rw, server::errors::ServerError, store, types};
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;
use futures::TryStreamExt;
use log::{debug, info, trace};

pub async fn do_put(
    store: store::StoreRef,
    repo: repo::Repository,
    decoder: &mut FlightDataDecoder,
) -> Result<(), ServerError> {
    let (cmd, schema) = extract_command_and_schema_from_header_message(decoder).await?;
    do_put_topic_data(store, repo, decoder, schema, cmd).await
}

async fn extract_command_and_schema_from_header_message(
    decoder: &mut FlightDataDecoder,
) -> Result<(types::flight::DoPutCmd, SchemaRef), ServerError> {
    if let Some(data) = decoder
        .try_next()
        .await
        .map_err(|e| ServerError::StreamError(e.to_string()))?
    {
        let cmd = extract_command_from_flight_data(&data)?;
        let schema = extract_schema_from_flight_data(&data)?;
        return Ok((cmd, schema));
    }
    Err(ServerError::MissingDoPutHeaderMessage)
}

fn extract_schema_from_flight_data(data: &DecodedFlightData) -> Result<SchemaRef, ServerError> {
    if let DecodedPayload::Schema(schema) = &data.payload {
        return Ok(schema.clone());
    }
    Err(ServerError::MissingSchema)
}

/// Extract descriptor tag from flight decoded data
fn extract_command_from_flight_data(
    data: &DecodedFlightData,
) -> Result<types::flight::DoPutCmd, ServerError> {
    let desc = data
        .inner
        .flight_descriptor
        .as_ref()
        .ok_or_else(|| ServerError::MissingDescriptior)?;

    // Check if the descriptor if supported
    if desc.r#type() == DescriptorType::Path {
        return Err(ServerError::UnsupportedDescriptor);
    }

    let decoded = marshal::flight::do_put_cmd(&desc.cmd)?;

    Ok(decoded)
}

async fn do_put_topic_data(
    store: store::StoreRef,
    repo: repo::Repository,
    decoder: &mut FlightDataDecoder,
    schema: SchemaRef,
    cmd: types::flight::DoPutCmd,
) -> Result<(), ServerError> {
    let locator = cmd.resource_locator;
    let key = &cmd.key;

    info!(
        "client trying to upload topic '{}' using key `{}`",
        locator, key
    );

    crate::arrow::check_schema(&schema)?;

    let handle = repo::FacadeTopic::new(locator, store.clone(), repo.clone());

    // perform the match between received key and topic id
    let r_id = handle.resource_id().await?;
    let received_uuid: uuid::Uuid = key.parse()?;
    if received_uuid != r_id.uuid {
        return Err(ServerError::BadKey);
    }

    let mdata = handle.metadata().await?;

    // Setup the callback that will be used to create the repository record for the data catalog
    // and prepare variables that will be moved in the closure
    let ontology_tag = mdata.properties.ontology_tag;
    let serialization_format = mdata.properties.serialization_format;
    let topic_id = r_id.id;

    let mut writer = handle.writer(serialization_format).on_chunk_created(
        move |target_path, parquet_bytes, cols_stats, chunk_metadata| {
            let topic_id = topic_id;
            let repo_clone = repo.clone();
            let ontology_tag = ontology_tag.clone();

            async move {
                trace!(
                    "calling chunk creation callback for `{}` {:?}",
                    target_path.to_string_lossy(),
                    cols_stats
                );

                Ok(on_chunk_created(
                    repo_clone,
                    topic_id,
                    &ontology_tag,
                    target_path,
                    parquet_bytes,
                    cols_stats,
                    chunk_metadata,
                )
                .await?)
            }
        },
    );

    // Consume all batches
    while let Some(data) = decoder
        .try_next()
        .await
        .map_err(|e| ServerError::StreamError(e.to_string()))?
    {
        match data.payload {
            DecodedPayload::RecordBatch(batch) => {
                debug!(
                    "processing batch (cols: {}, memory_size: {}",
                    batch.columns().len(),
                    batch.get_array_memory_size()
                );
                writer.write(&batch).await?;
            }
            DecodedPayload::Schema(_) => {
                return Err(ServerError::DuplicateSchemaInPayload);
            }
            DecodedPayload::None => {
                return Err(ServerError::NoData);
            }
        }
    }

    // If the finalize fails (e.g. problems during stats computation) the topic will not be locked,
    // this allows the reindexing (currently not implemented) of
    // the topic
    trace!("finializing data write");
    writer.finalize().await?;

    trace!("resource {} locked", handle.locator);
    handle.lock().await?;

    Ok(())
}

/// Creates chunk metadata and inserts parquet data into the outbox atomically.
///
/// This implements the transactional outbox pattern:
/// 1. Creates chunk metadata in the database
/// 2. Inserts column statistics
/// 3. Inserts parquet bytes into the outbox for background S3 upload
///
/// All operations are performed in a single transaction, ensuring no orphan files.
async fn on_chunk_created(
    repo: repo::Repository,
    topic_id: i32,
    ontology_tag: &str,
    target_path: impl AsRef<std::path::Path>,
    parquet_bytes: Bytes,
    cstats: types::ColumnsStats,
    chunk_metadata: rw::ChunkMetadata,
) -> Result<(), ServerError> {
    let mut tx = repo.transaction().await?;

    // 1. Create chunk record
    let chunk = repo::chunk_create(
        &mut tx,
        &repo::Chunk::new(
            topic_id,
            &target_path,
            chunk_metadata.size_bytes as i64,
            chunk_metadata.row_count as i64,
        ),
    )
    .await?;

    // 2. Insert column statistics
    // Collect all stats and batch insert them
    let mut numeric_batch: Vec<repo::ColumnChunkNumeric> = Vec::new();
    let mut literal_batch: Vec<repo::ColumnChunkLiteral> = Vec::new();

    for (field, stats) in cstats.stats {
        if stats.is_unsupported() {
            continue;
        }

        let column = repo::column_get_or_create(&mut tx, &field, ontology_tag).await?;

        match stats {
            types::Stats::Text(stats) => {
                let (min, max, has_null) = stats.into_owned();
                literal_batch.push(repo::ColumnChunkLiteral::try_new(
                    column.column_id,
                    chunk.chunk_id,
                    min,
                    max,
                    has_null,
                )?);
            }
            types::Stats::Numeric(stats) => {
                numeric_batch.push(repo::ColumnChunkNumeric::new(
                    column.column_id,
                    chunk.chunk_id,
                    stats.min,
                    stats.max,
                    stats.has_null,
                    stats.has_nan,
                ));
            }
            types::Stats::Unsupported => {}
        }
    }

    repo::column_chunk_numeric_create_batch(&mut tx, &numeric_batch).await?;
    repo::column_chunk_literal_create_batch(&mut tx, &literal_batch).await?;

    // 3. Insert outbox entry (atomic with chunk metadata)
    let outbox_entry =
        repo::ChunkOutboxEntry::new(chunk.chunk_id, &target_path, parquet_bytes.to_vec());
    repo::outbox_create(&mut tx, &outbox_entry).await?;

    // Commit all in one transaction: chunk + stats + outbox
    tx.commit().await?;

    debug!(
        "chunk {} created with outbox entry for path: {}",
        chunk.chunk_id,
        target_path.as_ref().display()
    );

    Ok(())
}
