use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;

fn convert(record: schema::ApiKeyRecord) -> Result<types::ApiKey, Error> {
    record
        .try_into()
        .map_err(|e: types::auth::ApiKeyError| Error::BadData(e.to_string()))
}

pub async fn api_key_create(
    exe: &mut impl AsExec,
    policy: types::ApiKey,
) -> Result<types::ApiKey, Error> {
    let key: schema::ApiKeyRecord = policy.into();

    let res = sqlx::query_as!(
        schema::ApiKeyRecord,
        r#"
        INSERT INTO api_key_t
            (
                fingerprint, 
                payload, 
                permissions,
                description,
                creation_unix_timestamp,
                expiration_unix_timestamp
            )
        VALUES
            ($1, $2, $3, $4, $5, $6)
        RETURNING
            *
        "#,
        key.fingerprint,
        key.payload,
        key.permissions,
        key.description,
        key.creation_unix_timestamp,
        key.expiration_unix_timestamp
    )
    .fetch_one(exe.as_exec())
    .await?;

    convert(res)
}

pub async fn api_key_find_by_fingerprint(
    exe: &mut impl AsExec,
    fingerprint: &str,
) -> Result<types::ApiKey, Error> {
    let res = sqlx::query_as!(
        schema::ApiKeyRecord,
        r#"
        SELECT *
        FROM api_key_t AS api_key 
        WHERE api_key.fingerprint = $1
        "#,
        fingerprint.as_bytes()
    )
    .fetch_one(exe.as_exec())
    .await?;

    convert(res)
}

pub async fn api_key_delete(exe: &mut impl AsExec, fingerprint: &str) -> Result<(), Error> {
    sqlx::query!(
        "DELETE FROM api_key_t WHERE fingerprint=$1",
        fingerprint.as_bytes()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

pub async fn api_key_find_all(exe: &mut impl AsExec) -> Result<Vec<types::ApiKey>, Error> {
    let keys = sqlx::query_as!(schema::ApiKeyRecord, "SELECT * FROM api_key_t")
        .fetch_all(exe.as_exec())
        .await?;

    let keys = keys
        .into_iter()
        .map(convert)
        .collect::<Result<Vec<types::ApiKey>, Error>>()?;

    Ok(keys)
}
