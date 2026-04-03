use super::Context;
use crate::{endpoint, errors::ServerError};
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;
use futures::TryStreamExt;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_rw as rw;
use tracing::{debug, info};

pub async fn do_put(ctx: Context, decoder: &mut FlightDataDecoder) -> Result<(), ServerError> {
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
    ctx: endpoint::Context,
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

    let mut writer = handle.writer(ctx.timeseries_querier, serialization_format, schema.clone());

    // Build the callback that will be called at each chunk serialization
    let on_chunk_creation = move |path: std::path::PathBuf, cols_stats, mdata| {
        let topic_id = topic_id;
        let db_clone = ctx.db.clone();
        let ontology_tag = ontology_tag.clone();

        async move {
            debug!(
                "calling chunk creation callback for `{}` {:?}",
                path.to_string_lossy(),
                cols_stats
            );

            on_chunk_created(db_clone, topic_id, &ontology_tag, path, cols_stats, mdata).await
        }
    };

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
                    "received batch - cols: {}, rows: {}, msg_body_size: {} MiB, batch_physical_size: {} MiB",
                    batch.columns().len(),
                    batch.num_rows(),
                    data.inner.data_body.len() / 1024,
                    batch.get_array_memory_size() / 1024,
                );

                let serialized_chunk = writer.write(batch).await?;

                on_chunk_creation(
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

    debug!("finalizing writer");
    writer.finalize().await?;

    Ok(())
}

async fn on_chunk_created(
    db: db::Database,
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
        &db,
    )
    .await?;

    // Use batch insert for better performance (single INSERT per type instead of N)
    handle
        .push_ontology_model_stats(ontology_tag, cstats)
        .await?;

    handle.finalize().await?;

    Ok(())
}
