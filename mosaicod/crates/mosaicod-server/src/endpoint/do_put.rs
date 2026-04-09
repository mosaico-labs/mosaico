use crate::errors::ServerError;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;
use futures::TryStreamExt;
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_rw as rw;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

pub struct DoPutContext {
    pub inner: facade::Context,
    pub concurrent_writes_semaphore: Arc<tokio::sync::Semaphore>,
}

impl std::ops::Deref for DoPutContext {
    type Target = facade::Context;
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
    let uuid_str = &cmd.key;

    info!(
        target = "uploading topic",
        locator = locator,
        uuid = uuid_str,
    );

    mosaicod_ext::arrow::check_schema(&schema)?;

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(&ctx, topic_locator).await?;

    // perform the match between received uuid string and topic uuid
    let topic_uuid = topic_handle.uuid().clone();
    let received_uuid: types::Uuid = uuid_str.parse()?;
    if received_uuid != topic_uuid {
        return Err(ServerError::BadKey);
    }

    let mdata = facade::topic::manifest(&ctx, &topic_handle).await?;

    // Setup the callback that will be used to create the database record for the data catalog
    // and prepare variables that will be moved in the closure
    let ontology_tag = mdata.ontology_metadata.properties.ontology_tag;
    let serialization_format = mdata.ontology_metadata.properties.serialization_format;

    let mut writer = facade::topic::writer(ctx.clone(), topic_handle, serialization_format, schema);

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
                    target = "received batch",
                    cols = batch.columns().len(),
                    rows = batch.num_rows(),
                    msg_body_size = data.inner.data_body.len() / 1_000_000,
                    batch_physical_size = batch.get_array_memory_size() / 1_000_000,
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
                        ServerError::internal_error(&format!("unable to acquire semaphore: {}", e))
                    })?;
                let serialized_chunk = writer.write(batch).await?;
                drop(permit);

                on_chunk_created(
                    &ctx,
                    &topic_uuid,
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
    ctx: &DoPutContext,
    topic_uuid: &types::Uuid,
    ontology_tag: &str,
    target_path: impl AsRef<std::path::Path>,
    cstats: types::OntologyModelStats,
    chunk_metadata: rw::ChunkMetadata,
) -> Result<(), ServerError> {
    let mut handle = facade::Chunk::create(
        topic_uuid,
        &target_path,
        chunk_metadata.size_bytes as i64,
        chunk_metadata.row_count as i64,
        &ctx.inner,
    )
    .await?;

    handle
        .push_ontology_model_stats(ontology_tag, cstats)
        .await?;

    handle.finalize().await?;

    Ok(())
}
