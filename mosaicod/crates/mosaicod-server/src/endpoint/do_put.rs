use super::Context;
use crate::errors::ServerError;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;
use futures::TryStreamExt;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_rw as rw;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

pub struct DoPutContext {
    pub inner: Context,
    pub concurrent_writes_semaphore: Arc<tokio::sync::Semaphore>,
}

impl std::ops::Deref for DoPutContext {
    type Target = Context;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub async fn do_put(ctx: DoPutContext, decoder: &mut FlightDataDecoder) -> Result<(), ServerError> {
    let (cmd, schema) = extract_command_and_schema_from_header_message(decoder).await?;
    do_put_topic_data(ctx, decoder, schema, cmd).await
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
        .ok_or_else(|| ServerError::MissingDescriptor)?;

    // Check if the descriptor if supported
    if desc.r#type() == DescriptorType::Path {
        return Err(ServerError::UnsupportedDescriptor);
    }

    let decoded = marshal::flight::do_put_cmd(&desc.cmd)?;

    Ok(decoded)
}

async fn do_put_topic_data(
    ctx: DoPutContext,
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

    mosaicod_ext::arrow::check_schema(&schema)?;

    let handle =
        facade::Topic::try_from_locator(locator.into(), ctx.store.clone(), ctx.db.clone()).await?;

    // Perform the match between received key and topic id
    let r_id = handle.resource_id().await?;
    let received_uuid: types::Uuid = key.parse()?;
    if received_uuid != r_id.uuid {
        return Err(ServerError::BadKey);
    }

    let mdata = handle.manifest().await?;

    // Setup the callback that will be used to create the database record for the data catalog
    // and prepare variables that will be moved in the closure
    let ontology_tag = mdata.ontology_metadata.properties.ontology_tag;
    let serialization_format = mdata.ontology_metadata.properties.serialization_format;
    let topic_id = r_id.id;

    let mut writer = handle.writer(
        ctx.timeseries_querier.clone(),
        serialization_format,
        schema.clone(),
    );

    // Consume all batches
    debug!("ready to receive batches");
    while let Some(data) = decoder
        .try_next()
        .await
        .map_err(|e| ServerError::StreamError(e.to_string()))?
    {
        match data.payload {
            DecodedPayload::RecordBatch(batch) => {
                debug!(
                    "received batch - cols: {}, rows: {}, msg_body_size: {} MB, batch_physical_size: {} MB",
                    batch.columns().len(),
                    batch.num_rows(),
                    data.inner.data_body.len() / 1000_000,
                    batch.get_array_memory_size() / 1000_000,
                );

                // Trying to acquire a semaphore to limit the total amount of concurrent writes
                // run by this instance. This is done in order to bound memory consumption and
                // to limit CPU-bound operations.
                //
                // Since the `.write()` will encode-and-serialize in a single operation it is safe
                // to acquire the semaphore without causing deadlocks.
                let permit = ctx
                    .concurrent_writes_semaphore
                    .acquire()
                    .await
                    .map_err(|e| {
                        ServerError::internal_error(&format!(
                            "unable to acquire semaphore: {}",
                            e.to_string()
                        ))
                    })?;
                let serialized_chunk = writer.write(batch).await?;
                drop(permit);

                on_chunk_created(
                    &ctx.db,
                    topic_id,
                    &ontology_tag,
                    serialized_chunk.path,
                    serialized_chunk.ontology_stats,
                    serialized_chunk.metadata,
                )
                .await?;
            }
            DecodedPayload::Schema(_) => {
                return Err(ServerError::DuplicateSchemaInPayload);
            }
            DecodedPayload::None => {
                return Err(ServerError::NoData);
            }
        }
    }

    let time = Instant::now();
    writer.finalize().await?;
    debug!(
        target = "topic finalization",
        finalize_ms = time.elapsed().as_millis()
    );

    Ok(())
}

async fn on_chunk_created(
    db: &db::Database,
    topic_id: i32,
    ontology_tag: &str,
    target_path: impl AsRef<std::path::Path>,
    cstats: types::OntologyModelStats,
    chunk_metadata: rw::ChunkMetadata,
) -> Result<(), ServerError> {
    let mut handle = facade::Chunk::create(
        topic_id,
        &target_path,
        chunk_metadata.size_bytes as i64,
        chunk_metadata.row_count as i64,
        db,
    )
    .await?;

    // Use batch insert for better performance (single INSERT per type instead of N)
    handle
        .push_ontology_model_stats(ontology_tag, cstats)
        .await?;

    handle.finalize().await?;

    Ok(())
}
