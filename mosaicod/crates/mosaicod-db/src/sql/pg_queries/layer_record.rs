use crate::{Error, core::AsExec, sql::schema};
use log::info;
use mosaicod_core::{
    params::{DEFAULT_LAYER_DESCRIPTION, DEFAULT_LAYER_NAME},
    types,
};

/// Initializes the database layer structure.
///
/// This function ensures that the default layer is always defined.
pub async fn layer_bootstrap(exec: &mut impl AsExec) -> Result<(), Error> {
    let default_loc = types::LayerLocator::from(DEFAULT_LAYER_NAME);

    let layer = layer_find_by_locator(exec, &default_loc).await;
    if let Err(err) = layer {
        if let Error::BackendError(err) = err {
            match err {
                sqlx::Error::RowNotFound => {
                    info!("creating default layer");
                    layer_create(
                        exec,
                        types::Layer::new(default_loc, DEFAULT_LAYER_DESCRIPTION.to_owned()),
                    )
                    .await?;
                }
                _ => return Err(Error::BackendError(err)),
            }
        } else {
            return Err(err);
        }
    }

    Ok(())
}

/// Creates a new layer in the database
pub async fn layer_create(
    exec: &mut impl AsExec,
    layer: types::Layer,
) -> Result<schema::LayerRecord, Error> {
    let res = sqlx::query_as!(
        schema::LayerRecord,
        r#"INSERT INTO layer_t
            (layer_name, layer_description)
          VALUES
            ($1, $2)
          RETURNING *"#,
        layer.locator.name(),
        layer.description
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

/// Deletes a new layer in the database, the layer can be deleted only if there are no indexes
/// associated with him
pub async fn layer_delete(exec: &mut impl AsExec, layer_id: i32) -> Result<(), Error> {
    sqlx::query!("DELETE FROM layer_t WHERE layer_id=$1", layer_id)
        .execute(exec.as_exec())
        .await?;
    Ok(())
}

/// Update an existing layer with new data
pub async fn layer_update(
    exec: &mut impl AsExec,
    prev_loc: &types::LayerLocator,
    curr_loc: &types::LayerLocator,
    curr_description: &str,
) -> Result<schema::LayerRecord, Error> {
    let res = sqlx::query_as!(
        schema::LayerRecord,
        r#"
          UPDATE layer_t
          SET
            layer_name=$1, layer_description=$2
          WHERE
            layer_name=$3
          RETURNING
            *
    "#,
        curr_loc.name(),
        curr_description,
        prev_loc.name(),
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn layer_find_by_locator(
    exe: &mut impl AsExec,
    loc: &types::LayerLocator,
) -> Result<schema::LayerRecord, Error> {
    let res = sqlx::query_as!(
        schema::LayerRecord,
        r#"
        SELECT *
        FROM layer_t
        WHERE layer_name=$1
    "#,
        loc.name(),
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}
/// Return all layers
pub async fn layer_find_all(exe: &mut impl AsExec) -> Result<Vec<schema::LayerRecord>, Error> {
    Ok(
        sqlx::query_as!(schema::LayerRecord, "SELECT * FROM layer_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}
