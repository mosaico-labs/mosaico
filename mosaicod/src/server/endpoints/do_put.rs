use arrow::datatypes::SchemaRef;
use futures::TryStreamExt;

use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::flight_descriptor::DescriptorType;

use log::{debug, info, trace};
use serde::Deserialize;

use crate::{repo, rw, server::errors::ServerError, store, types};

#[derive(Deserialize, Debug)]
struct DoPutTopic {
    name: String,
    key: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
enum DoPutCommand {
    Topic(DoPutTopic),
}

pub async fn do_put(
    store: store::StoreRef,
    repo: repo::Repository,
    decoder: &mut FlightDataDecoder,
) -> Result<(), ServerError> {
    let (cmd, schema) = extract_command_and_schema_from_header_message(decoder).await?;

    match cmd {
        DoPutCommand::Topic(cmd) => {
            return do_put_topic_data(store, repo, decoder, schema, cmd).await;
        }
    }
}

async fn extract_command_and_schema_from_header_message(
    decoder: &mut FlightDataDecoder,
) -> Result<(DoPutCommand, SchemaRef), ServerError> {
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
fn extract_command_from_flight_data(data: &DecodedFlightData) -> Result<DoPutCommand, ServerError> {
    let desc = data
        .inner
        .flight_descriptor
        .as_ref()
        .ok_or_else(|| ServerError::MissingDescriptior)?;

    // Check if the descriptor if supported
    if desc.r#type() == DescriptorType::Path {
        return Err(ServerError::UnsupportedDescriptor);
    }

    // decode
    Ok(serde_json::from_slice::<DoPutCommand>(&desc.cmd)?)
}

async fn do_put_topic_data(
    store: store::StoreRef,
    repo: repo::Repository,
    decoder: &mut FlightDataDecoder,
    schema: SchemaRef,
    cmd: DoPutTopic,
) -> Result<(), ServerError> {
    let name = cmd.name;
    let key = &cmd.key;

    info!(
        "client trying to upload topic '{}' using key `{}`",
        name, key
    );

    crate::arrow::check_schema(&schema)?;

    let handle = repo::FacadeTopic::new(name, store.clone(), repo.clone());

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
        move |target_path, cols_stats, chunk_metadata| {
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

async fn on_chunk_created(
    repo: repo::Repository,
    topic_id: i32,
    ontology_tag: &str,
    target_path: impl AsRef<std::path::Path>,
    cstats: types::ColumnsStats,
    chunk_metadata: rw::ChunkMetadata,
) -> Result<(), ServerError> {
    let mut handle = repo::FacadeChunk::create(
        topic_id,
        &target_path,
        chunk_metadata.size_bytes as i64,
        chunk_metadata.row_count as i64,
        &repo,
    )
    .await?;

    for (field, stats) in cstats.stats {
        handle.push_stats(ontology_tag, &field, stats).await?;
    }

    handle.finalize().await?;

    Ok(())
}
