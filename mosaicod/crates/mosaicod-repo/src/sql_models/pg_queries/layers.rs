use crate::{self as repo, Error, sql_models};
use log::info;
use mosaicod_core::{
    params::{DEFAULT_LAYER_DESCRIPTION, DEFAULT_LAYER_NAME},
    types,
};

/// Initializes the repository layer structure.
///
/// This function ensures that the default layer is always defined.
pub async fn layer_bootstrap(exec: &mut impl repo::AsExec) -> Result<(), repo::Error> {
    let default_loc = types::LayerLocator::from(DEFAULT_LAYER_NAME);

    let layer = layer_find_by_locator(exec, &default_loc).await;
    if let Err(err) = layer {
        if let repo::Error::BackendError(err) = err {
            match err {
                sqlx::Error::RowNotFound => {
                    info!("creating default layer");
                    repo::layer_create(
                        exec,
                        types::Layer::new(default_loc, DEFAULT_LAYER_DESCRIPTION.to_owned()),
                    )
                    .await?;
                }
                _ => return Err(repo::Error::BackendError(err)),
            }
        } else {
            return Err(err);
        }
    }

    Ok(())
}

/// Creates a new layer in the repository
pub async fn layer_create(
    exec: &mut impl repo::AsExec,
    layer: types::Layer,
) -> Result<sql_models::Layer, Error> {
    let res = sqlx::query_as!(
        sql_models::Layer,
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

/// Deletes a new layer in the repository, the layer can be deleted only if there are no indexes
/// associated with him
pub async fn layer_delete(exec: &mut impl repo::AsExec, layer_id: i32) -> Result<(), repo::Error> {
    sqlx::query!("DELETE FROM layer_t WHERE layer_id=$1", layer_id)
        .execute(exec.as_exec())
        .await?;
    Ok(())
}

/// Update an existing layer with new data
pub async fn layer_update(
    exec: &mut impl repo::AsExec,
    prev_loc: &types::LayerLocator,
    curr_loc: &types::LayerLocator,
    curr_description: &str,
) -> Result<sql_models::Layer, repo::Error> {
    let res = sqlx::query_as!(
        sql_models::Layer,
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
    exe: &mut impl repo::AsExec,
    loc: &types::LayerLocator,
) -> Result<sql_models::Layer, repo::Error> {
    let res = sqlx::query_as!(
        sql_models::Layer,
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
pub async fn layer_find_all(
    exe: &mut impl repo::AsExec,
) -> Result<Vec<sql_models::Layer>, repo::Error> {
    Ok(sqlx::query_as!(sql_models::Layer, "SELECT * FROM layer_t")
        .fetch_all(exe.as_exec())
        .await?)
}
