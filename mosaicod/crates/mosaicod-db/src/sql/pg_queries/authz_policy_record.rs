use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;

fn convert(record: schema::AuthzPolicyRecord) -> Result<types::AuthorizationPolicy, Error> {
    record
        .try_into()
        .map_err(|e: types::ApiKeyError| Error::BadData(e.to_string()))
}

pub async fn authz_policy_create(
    exe: &mut impl AsExec,
    policy: types::AuthorizationPolicy,
) -> Result<types::AuthorizationPolicy, Error> {
    let policy: schema::AuthzPolicyRecord = policy.into();

    let res = sqlx::query_as!(
        schema::AuthzPolicyRecord,
        r#"
        INSERT INTO authz_policy_t
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
        policy.api_key_fingerprint,
        policy.api_key_payload,
        policy.permissions,
        policy.description,
        policy.creation_unix_timestamp,
        policy.expiration_unix_timestamp
    )
    .fetch_one(exe.as_exec())
    .await?;

    convert(res)
}

pub async fn authz_policy_find_by_fingerprint(
    exe: &mut impl AsExec,
    fingerprint: &str,
) -> Result<types::AuthorizationPolicy, Error> {
    let res = sqlx::query_as!(
        schema::AuthzPolicyRecord,
        r#"
        SELECT *
        FROM authz_policy_t AS authz_policy
        WHERE authz_policy.api_key_fingerprint = $1
        "#,
        fingerprint.as_bytes()
    )
    .fetch_one(exe.as_exec())
    .await?;

    convert(res)
}

pub async fn authz_policy_delete(exe: &mut impl AsExec, fingerprint: &str) -> Result<(), Error> {
    sqlx::query!(
        "DELETE FROM authz_policy_t WHERE api_key_fingerprint=$1",
        fingerprint.as_bytes()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

pub async fn authz_policy_find_all(
    exe: &mut impl AsExec,
) -> Result<Vec<types::AuthorizationPolicy>, Error> {
    let policies = sqlx::query_as!(schema::AuthzPolicyRecord, "SELECT * FROM authz_policy_t")
        .fetch_all(exe.as_exec())
        .await?;

    let policies = policies
        .into_iter()
        .map(convert)
        .collect::<Result<Vec<types::AuthorizationPolicy>, Error>>()?;

    Ok(policies)
}
