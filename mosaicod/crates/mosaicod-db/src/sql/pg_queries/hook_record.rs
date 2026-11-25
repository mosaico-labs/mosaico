//! Hooks: HTTP endpoints that mosaicod sends events to. See `mosaicod hook`.

use tracing::trace;

use crate::{Error, core::AsExec, sql::schema};

/// Registers a hook.
///
/// Returns [`Error::AlreadyExists`] if `url` is already registered.
pub async fn hook_create(
    exe: &mut impl AsExec,
    url: &str,
    creation_unix_tstamp: i64,
) -> Result<schema::HookRecord, Error> {
    trace!("registering hook `{}`", url);

    Ok(sqlx::query_as!(
        schema::HookRecord,
        r#"
            INSERT INTO hook_t (url, creation_unix_tstamp)
            VALUES ($1, $2)
            RETURNING *
        "#,
        url,
        creation_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?)
}

/// Lists every hook, ordered by id.
pub async fn hook_find_all(exe: &mut impl AsExec) -> Result<Vec<schema::HookRecord>, Error> {
    trace!("listing hooks");

    Ok(sqlx::query_as!(
        schema::HookRecord,
        r#"SELECT * FROM hook_t ORDER BY hook_id ASC"#,
    )
    .fetch_all(exe.as_exec())
    .await?)
}

/// Deletes a hook.
///
/// Returns `false` if no hook has this id.
pub async fn hook_delete(exe: &mut impl AsExec, hook_id: i32) -> Result<bool, Error> {
    trace!("deleting hook {}", hook_id);

    let res = sqlx::query!(r#"DELETE FROM hook_t WHERE hook_id = $1"#, hook_id)
        .execute(exe.as_exec())
        .await?;

    Ok(res.rows_affected() != 0)
}
