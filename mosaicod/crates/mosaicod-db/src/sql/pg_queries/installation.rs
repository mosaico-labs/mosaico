//! The installation ID: one per database, sent with every usage report.

use mosaicod_core::types;

use crate::{Error, core::AsExec};

/// Returns the installation ID, creating it if this database has none yet.
///
/// The insert and the read are two statements, not one: when two processes start at once,
/// the one whose insert loses the race reads the winner's row with a fresh snapshot, which a
/// single statement couldn't see.
pub async fn installation_get_or_create(exe: &mut impl AsExec) -> Result<types::Uuid, Error> {
    sqlx::query!(
        r#"
            INSERT INTO installation_t (installation_uuid, creation_unix_tstamp)
            VALUES ($1, $2)
            ON CONFLICT (singleton) DO NOTHING
        "#,
        uuid::Uuid::from(types::Uuid::new()),
        types::Timestamp::now().as_i64(),
    )
    .execute(exe.as_exec())
    .await?;

    let uuid = sqlx::query_scalar!("SELECT installation_uuid FROM installation_t")
        .fetch_one(exe.as_exec())
        .await?;

    Ok(uuid.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[sqlx::test(migrator = "testing::MIGRATOR")]
    async fn later_calls_return_the_first_id(pool: sqlx::Pool<crate::DatabaseType>) {
        let db = testing::Database::new(pool);

        let first = installation_get_or_create(&mut db.connection())
            .await
            .unwrap();
        let later = installation_get_or_create(&mut db.connection())
            .await
            .unwrap();

        assert_eq!(later, first);
    }
}
