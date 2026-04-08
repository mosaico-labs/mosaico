use crate::errors::ServerError;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;
use futures::TryStreamExt;
use log::{info, trace};
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_rw as rw;

pub async fn do_put(
    ctx: facade::Context,
    decoder: &mut FlightDataDecoder,
) -> Result<(), ServerError> {
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
    ctx: facade::Context,
    decoder: &mut FlightDataDecoder,
    schema: SchemaRef,
    cmd: types::flight::DoPutCmd,
) -> Result<(), ServerError> {
    let locator = cmd.resource_locator;
    let uuid_str = &cmd.key;

    info!(
        "client trying to upload topic '{}' using uuid `{}`",
        locator, uuid_str
    );

    mosaicod_ext::arrow::check_schema(&schema)?;

    let topic_locator = types::TopicResourceLocator::from(locator);

    let mut topic_handle = facade::topic::Handle::try_from_locator(&ctx, topic_locator).await?;

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

    trace!("creating topic writer");
    let mut writer = facade::topic::writer(ctx.clone(), &mut topic_handle, serialization_format);

    writer.on_chunk_created(move |target_path, cols_stats, chunk_metadata| {
        let topic_uuid = topic_uuid.clone();
        let ctx_clone = ctx.clone();
        let ontology_tag = ontology_tag.clone();

        async move {
            trace!(
                "calling chunk creation callback for `{}` {:?}",
                target_path.to_string_lossy(),
                cols_stats
            );

            Ok(on_chunk_created(
                &ctx_clone,
                topic_uuid,
                &ontology_tag,
                target_path,
                cols_stats,
                chunk_metadata,
            )
            .await?)
        }
    });

    // Consume all batches
    trace!("ready to consume batches");
    while let Some(data) = decoder
        .try_next()
        .await
        .map_err(|e| ServerError::StreamError(e.to_string()))?
    {
        match data.payload {
            DecodedPayload::RecordBatch(batch) => {
                trace!(
                    "received batch (cols: {}, rows: {}, memory_size: {})",
                    batch.columns().len(),
                    batch.num_rows(),
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
    // this allows the reindexing (currently not implemented) of the topic
    trace!("finializing data write");
    writer.finalize().await?;

    Ok(())
}

async fn on_chunk_created(
    context: &facade::Context,
    topic_uuid: types::Uuid,
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
        context,
    )
    .await?;

    // Use batch insert for better performance (single INSERT per type instead of N)
    handle
        .push_ontology_model_stats(ontology_tag, cstats)
        .await?;

    handle.finalize().await?;

    Ok(())
}
