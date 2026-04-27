use crate::{Error, core::AsExec, sql::schema};
use log::{trace, warn};
use mosaicod_core::types;

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
    loc: &types::SequenceLocator,
) -> Result<schema::SequenceRecord, Error> {
    trace!("searching by locator `{}`", loc);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        "SELECT * FROM sequence_t WHERE locator_name=$1",
        loc as &str,
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn sequence_find_all_topics(
    exe: &mut impl AsExec,
    loc: &types::SequenceLocator,
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics for sequence `{}`", loc);
    Ok(sqlx::query_as!(
        schema::TopicRecord,
        r#"
        SELECT topic.*
        FROM topic_t AS topic
        JOIN sequence_t AS sequence ON topic.sequence_id = sequence.sequence_id
        WHERE sequence.locator_name = $1
        "#,
        loc as &str
    )
    .fetch_all(exe.as_exec())
    .await?)
}

pub async fn sequence_find_all_sessions(
    exe: &mut impl AsExec,
    loc: &types::SequenceLocator,
) -> Result<Vec<schema::SessionRecord>, Error> {
    trace!("searching sessions for sequence `{}`", loc);
    Ok(sqlx::query_as!(
        schema::SessionRecord,
        r#"
        SELECT session.*
        FROM session_t AS session 
        JOIN sequence_t AS sequence ON session.sequence_id = sequence.sequence_id
        WHERE sequence.locator_name = $1
        "#,
        loc as &str
    )
    .fetch_all(exe.as_exec())
    .await?)
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

/// Deletes a sequence record from the database by its name.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record
/// from the database without checking if it's referenced elsewhere.
/// Improper use can lead to data inconsistency or loss.
pub async fn sequence_delete_by_locator(
    exe: &mut impl AsExec,
    loc: &types::SequenceLocator,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting sequence `{}`", loc);
    sqlx::query!("DELETE FROM sequence_t WHERE locator_name=$1", loc as &str)
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

/// Deletes a sequence record from the database by its id.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record
/// from the database without checking if it's referenced elsewhere.
/// Improper use can lead to data inconsistency or loss.
pub async fn sequence_delete_by_id(
    exe: &mut impl AsExec,
    sequence_id: i32,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting sequence with id `{}`", sequence_id);
    let result = sqlx::query!("DELETE FROM sequence_t WHERE sequence_id=$1", sequence_id)
        .execute(exe.as_exec())
        .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

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
                (sequence_uuid, locator_name, creation_unix_tstamp, user_metadata, path_in_store)
            VALUES 
                ($1, $2, $3, $4, $5)
            RETURNING 
                *
    "#,
        record.sequence_uuid,
        record.locator_name,
        record.creation_unix_tstamp,
        record.user_metadata,
        record.path_in_store
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DatabaseType, testing};
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_create(pool: Pool<DatabaseType>) -> sqlx::Result<()> {
        let record = schema::SequenceRecord::new(
            "/my_sequence".parse().unwrap(),
            "/my/path/in/store".to_owned().into(),
        );
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
