use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use tracing::{trace, warn};

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
    trace!("searching sequence by locator name `{}`", loc);
    let res = sqlx::query_as!(
        schema::SequenceRecord,
        "SELECT * FROM sequence_t WHERE locator_name=$1",
        loc as &str,
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Search for a sequence with the given path_in_store.
pub async fn sequence_find_path_in_store(
    exe: &mut impl AsExec,
    path_in_store: &str,
) -> Result<bool, Error> {
    trace!(
        "searching if path_in_store `{}` is assigned to a sequence",
        path_in_store
    );
    let found: bool = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM sequence_t WHERE path_in_store=$1) as "found!""#,
        path_in_store
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(found)
}

pub async fn sequence_find_all_topics(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics for sequence with id `{}`", id);
    Ok(sqlx::query_as!(
        schema::TopicRecord,
        r#"SELECT * FROM topic_t WHERE sequence_id = $1"#,
        id
    )
    .fetch_all(exe.as_exec())
    .await?)
}

pub async fn sequence_find_all_sessions(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<Vec<schema::SessionRecord>, Error> {
    trace!("searching sessions for sequence with id `{}`", id);

    let res = sqlx::query_as!(
        schema::SessionRecord,
        r#"SELECT * FROM session_t WHERE sequence_id = $1"#,
        id
    )
    .fetch_all(exe.as_exec())
    .await?;

    Ok(res)
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
    let res = sqlx::query!("DELETE FROM sequence_t WHERE locator_name=$1", loc as &str)
        .execute(exe.as_exec())
        .await?;

    if res.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

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
    let result = sqlx::query!(
        r#"DELETE FROM sequence_t WHERE sequence_id=$1"#,
        sequence_id
    )
    .execute(exe.as_exec())
    .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

    Ok(())
}

pub async fn sequence_create(
    exe: &mut impl AsExec,
    locator: &types::SequenceLocator,
    path_in_store: &types::SequencePathInStore,
    user_metadata: Option<serde_json::Value>,
) -> Result<schema::SequenceRecord, Error> {
    trace!("creating a new sequence {}", locator);
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
        uuid::Uuid::new_v4(),
        locator.to_string(),
        types::Timestamp::now().as_i64(),
        user_metadata,
        path_in_store.to_string()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        UNREGISTERED,
        core::{DatabaseType, testing},
    };
    use mosaicod_core::types::MetadataBlob;
    use mosaicod_marshal as marshal;
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_create_without_metadata(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let record = sequence_create(
            &mut database.connection(),
            &"my_sequence".parse().unwrap(),
            &"/my/path/in/store".to_owned().into(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(record.locator_name, "my_sequence");
        assert!(record.creation_unix_tstamp <= types::Timestamp::now().as_i64());
        assert_eq!(record.path_in_store, "/my/path/in/store");
        assert!(record.user_metadata.is_none());
        assert_ne!(record.sequence_id, UNREGISTERED);
    }

    #[sqlx::test]
    async fn test_create_with_metadata(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let metadata =
            marshal::JsonMetadataBlob::try_from_str(r#"{"key": "value", "key2": 100}"#).unwrap();

        let record = sequence_create(
            &mut database.connection(),
            &"my_sequence".parse().unwrap(),
            &"/my/path/in/store".to_owned().into(),
            Some(metadata.into()),
        )
        .await
        .unwrap();

        assert_eq!(record.locator_name, "my_sequence");
        assert!(record.creation_unix_tstamp <= types::Timestamp::now().as_i64());
        assert_eq!(record.path_in_store, "/my/path/in/store");
        assert!(record.user_metadata.is_some());
        assert_ne!(record.sequence_id, UNREGISTERED);
    }
}
