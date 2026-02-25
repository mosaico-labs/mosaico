use crate::{core::AsExec, Error, sql::schema};
use log::trace;
use mosaicod_core::types::{self, Resource};

/// Find a sequence given its id.
pub async fn sequence_find_by_id(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<schema::SequenceRecord, Error> {
    trace!("searching sequence by id `{}`", id);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        "SELECT * FROM sequence_t WHERE sequence_id=$1",
        id
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its uuid.
pub async fn sequence_find_by_uuid(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
) -> Result<schema::SequenceRecord, Error> {
    trace!("searching sequence by uuid `{}`", uuid);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        "SELECT * FROM sequence_t WHERE sequence_uuid=$1",
        uuid.as_ref()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its name.
pub async fn sequence_find_by_locator(
    exe: &mut impl AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<schema::SequenceRecord, Error> {
    trace!("searching by locator `{}`", loc);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        "SELECT * FROM sequence_t WHERE locator_name=$1",
        loc.name(),
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence by resource lookup
pub async fn sequence_lookup(
    exec: &mut impl AsExec,
    resource_lookup: &types::ResourceLookup,
) -> Result<schema::SequenceRecord, Error> {
    match resource_lookup {
        types::ResourceLookup::Id(id) => sequence_find_by_id(exec, *id).await,
        types::ResourceLookup::Uuid(uuid) => sequence_find_by_uuid(exec, uuid).await,
        types::ResourceLookup::Locator(locator) => {
            // (cabba) FIXME: we need to find a way to avoid locator copy
            let locator = locator.to_owned().into();
            sequence_find_by_locator(exec, &locator).await
        }
    }
}

pub async fn sequence_find_all_topic_names(
    exe: &mut impl AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<Vec<types::TopicResourceLocator>, Error> {
    trace!("searching topic locators by sequence `{}`", loc);
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
    exe: &mut impl AsExec,
) -> Result<Vec<schema::SequenceRecord>, Error> {
    trace!("retrieving all sequences");
    Ok(
        sqlx::query_as!(schema::SequenceRecord, "SELECT * FROM sequence_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}

/// Deletes a sequence record from the database by its name, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record
/// from the database without checking whether it is locked or referenced
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn sequence_delete(
    exe: &mut impl AsExec,
    loc: &types::SequenceResourceLocator,
    _: types::DataLossToken,
) -> Result<(), Error> {
    trace!("(data loss) deleting `{}`", loc);
    sqlx::query!("DELETE FROM sequence_t WHERE locator_name=$1", loc.name())
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

pub async fn sequence_create(
    exe: &mut impl AsExec,
    record: &schema::SequenceRecord,
) -> Result<schema::SequenceRecord, Error> {
    trace!("creating a new sequence record {:?}", record);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        r#"
            INSERT INTO sequence_t
                (sequence_uuid, locator_name, creation_unix_tstamp, user_metadata) 
            VALUES 
                ($1, $2, $3, $4) 
            RETURNING 
                *
    "#,
        record.sequence_uuid,
        record.locator_name,
        record.creation_unix_tstamp,
        record.user_metadata
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{testing, DatabaseType};
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_create(pool: Pool<DatabaseType>) -> sqlx::Result<()> {
        let record = schema::SequenceRecord::new("/my/path");
        let database = testing::Database::new(pool);
        let rrecord = sequence_create(&mut database.connection(), &record)
            .await
            .unwrap();

        assert_eq!(record.sequence_uuid, rrecord.sequence_uuid);
        assert_eq!(record.locator_name, rrecord.locator_name);
        assert_eq!(record.creation_unix_tstamp, rrecord.creation_unix_tstamp);

        Ok(())
    }

    // (cabba) TODO: extend tests
}
