use crate::{self as repo, Error, sql_models};
use log::trace;
use mosaicod_core::types::{self, Resource};

/// Find a sequence given its id.
pub async fn sequence_find_by_id(
    exe: &mut impl repo::AsExec,
    id: i32,
) -> Result<sql_models::SequenceRecord, Error> {
    trace!("searching sequence by id `{}`", id);
    let res = sqlx::query_as!(
        sql_models::SequenceRecord,
        "SELECT * FROM sequence_t WHERE sequence_id=$1",
        id
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its uuid.
pub async fn sequence_find_by_uuid(
    exe: &mut impl repo::AsExec,
    uuid: &uuid::Uuid,
) -> Result<sql_models::SequenceRecord, Error> {
    trace!("searching sequence by uuid `{}`", uuid);
    let res = sqlx::query_as!(
        sql_models::SequenceRecord,
        "SELECT * FROM sequence_t WHERE sequence_uuid=$1",
        uuid
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its name.
pub async fn sequence_find_by_locator(
    exe: &mut impl repo::AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<sql_models::SequenceRecord, Error> {
    trace!("searching by name `{}`", loc);
    let res = sqlx::query_as!(
        sql_models::SequenceRecord,
        "SELECT * FROM sequence_t WHERE locator_name=$1",
        loc.name(),
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn sequence_find_all_topic_names(
    exe: &mut impl repo::AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<Vec<types::TopicResourceLocator>, Error> {
    trace!("searching topic locators by `{}`", loc);
    let res = sqlx::query_scalar!(
        r#"
        SELECT topic.locator_name
        FROM topic_t AS topic
        JOIN sequence_t AS sequence ON topic.sequence_id = sequence.sequence_id
        WHERE sequence.locator_name = $1
        "#,
        loc.name()
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res
        .into_iter()
        .map(types::TopicResourceLocator::from)
        .collect())
}

/// Return all sequences
pub async fn sequence_find_all(
    exe: &mut impl repo::AsExec,
) -> Result<Vec<sql_models::SequenceRecord>, Error> {
    trace!("retrieving all sequences");
    Ok(
        sqlx::query_as!(sql_models::SequenceRecord, "SELECT * FROM sequence_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}

/// Deletes a sequence record from the repository **only if it is unlocked**.
///
/// If the sequence is locked or does not exist, the operation has no effect.
pub async fn sequence_delete_unlocked(
    exe: &mut impl repo::AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<(), repo::Error> {
    trace!("deleting unlocked `{}`", loc);
    sqlx::query!(
        "DELETE FROM sequence_t WHERE locator_name=$1 AND locked=FALSE",
        loc.name()
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

/// Deletes a sequence record from the repository by its name, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record
/// from the database without checking whether it is locked or referenced
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn sequence_delete(
    exe: &mut impl repo::AsExec,
    loc: &types::SequenceResourceLocator,
    _: types::DataLossToken,
) -> Result<(), repo::Error> {
    trace!("(data loss) deleting `{}`", loc);
    sqlx::query!("DELETE FROM sequence_t WHERE locator_name=$1", loc.name())
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

pub async fn sequence_create(
    exe: &mut impl repo::AsExec,
    record: &sql_models::SequenceRecord,
) -> Result<sql_models::SequenceRecord, Error> {
    trace!("creating a new sequence record {:?}", record);
    let res = sqlx::query_as!(
        sql_models::SequenceRecord,
        r#"
            INSERT INTO sequence_t
                (sequence_uuid, locator_name, locked, creation_unix_tstamp, user_metadata) 
            VALUES 
                ($1, $2, $3, $4, $5) 
            RETURNING 
                *
    "#,
        record.sequence_uuid,
        record.locator_name,
        record.locked,
        record.creation_unix_tstamp,
        record.user_metadata
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn sequence_lock(
    exe: &mut impl repo::AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<(), Error> {
    trace!("locking `{}`", loc);
    sqlx::query!(
        r#"
            UPDATE sequence_t
            SET locked = TRUE 
            WHERE locator_name = $1
    "#,
        loc.name()
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing;
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_create(pool: Pool<repo::Database>) -> sqlx::Result<()> {
        let record = sql_models::SequenceRecord::new("/my/path");
        let repo = testing::Repository::new(pool);
        let rrecord = sequence_create(&mut repo.connection(), &record)
            .await
            .unwrap();

        assert_eq!(record.sequence_uuid, rrecord.sequence_uuid);
        assert_eq!(record.locator_name, rrecord.locator_name);
        assert_eq!(record.creation_unix_tstamp, rrecord.creation_unix_tstamp);
        assert_eq!(record.locked, rrecord.locked);

        Ok(())
    }

    // (cabba) TODO: extend tests
}
