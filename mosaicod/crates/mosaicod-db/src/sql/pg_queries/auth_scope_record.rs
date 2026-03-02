use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;

fn convert(record: schema::AuthScopeRecord) -> Result<types::AuthScope, Error> {
    Ok(record
        .try_into()
        .map_err(|e: types::ApiKeyError| Error::BadData(e.to_string()))?)
}

pub async fn auth_scope_create(
    exe: &mut impl AsExec,
    scope: types::AuthScope,
) -> Result<types::AuthScope, Error> {
    let scope: schema::AuthScopeRecord = scope.into();

    let res = sqlx::query_as!(
        schema::AuthScopeRecord,
        r#"
        INSERT INTO auth_scope_t
            (
                api_key_fingerprint, 
                api_key_payload, 
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
        scope.api_key_fingerprint,
        scope.api_key_payload,
        scope.permissions,
        scope.description,
        scope.creation_unix_timestamp,
        scope.expiration_unix_timestamp
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(convert(res)?)
}

pub async fn auth_scope_find_by_fingerprint(
    exe: &mut impl AsExec,
    fingerprint: &str,
) -> Result<types::AuthScope, Error> {
    let res = sqlx::query_as!(
        schema::AuthScopeRecord,
        r#"
        SELECT *
        FROM auth_scope_t AS auth_scope
        WHERE auth_scope.api_key_fingerprint = $1
        "#,
        fingerprint.as_bytes()
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(convert(res)?)
}

pub async fn auth_scope_delete(exe: &mut impl AsExec, fingerprint: &str) -> Result<(), Error> {
    sqlx::query!(
        "DELETE FROM auth_scope_t WHERE api_key_fingerprint=$1",
        fingerprint.as_bytes()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

pub async fn auth_scope_find_all(exe: &mut impl AsExec) -> Result<Vec<types::AuthScope>, Error> {
    let scopes = sqlx::query_as!(schema::AuthScopeRecord, "SELECT * FROM auth_scope_t")
        .fetch_all(exe.as_exec())
        .await?;

    let scopes = scopes
        .into_iter()
        .map(|e| convert(e))
        .collect::<Result<Vec<types::AuthScope>, Error>>()?;

    Ok(scopes)
}
