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

/// Returns a page of sequences in ascending order: the first page in the given order, or
/// the page strictly after a cursor, in the cursor's order.
pub async fn sequence_find_page(
    exe: &mut impl AsExec,
    start: &types::ListStart,
    limit: types::ListLimit,
) -> Result<Vec<types::SequenceListEntry>, Error> {
    trace!("retrieving a page of sequences from {:?}", start);

    let limit = match limit {
        types::ListLimit::Unlimited => None,
        types::ListLimit::Bounded(limit) => Some(i64::from(limit.get())),
    };

    match start {
        types::ListStart::First(types::SequenceOrder::CreatedAt) => {
            page_by_created_at(exe, i64::MIN, i32::MIN, limit).await
        }
        types::ListStart::First(types::SequenceOrder::Name) => page_by_name(exe, "", limit).await,
        types::ListStart::After(types::SequenceCursor::CreatedAt {
            created_at,
            sequence_id,
        }) => page_by_created_at(exe, created_at.as_i64(), *sequence_id, limit).await,
        types::ListStart::After(types::SequenceCursor::Name(name)) => {
            page_by_name(exe, name, limit).await
        }
    }
}

/// Returns the sequences after `(created_at, sequence_id)`, in creation order.
async fn page_by_created_at(
    exe: &mut impl AsExec,
    created_at: i64,
    sequence_id: i32,
    limit: Option<i64>,
) -> Result<Vec<types::SequenceListEntry>, Error> {
    let rows = sqlx::query!(
        r#"
            SELECT creation_unix_tstamp, sequence_id, locator_name
            FROM sequence_t
            WHERE (creation_unix_tstamp, sequence_id) > ($1, $2)
            ORDER BY creation_unix_tstamp, sequence_id
            LIMIT $3
        "#,
        created_at,
        sequence_id,
        limit,
    )
    .fetch_all(exe.as_exec())
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(types::SequenceListEntry {
                locator: parse_locator(row.locator_name)?,
                cursor: types::SequenceCursor::CreatedAt {
                    created_at: row.creation_unix_tstamp.into(),
                    sequence_id: row.sequence_id,
                },
            })
        })
        .collect()
}

/// Returns the sequences whose name sorts after `after`, in name order.
async fn page_by_name(
    exe: &mut impl AsExec,
    after: &str,
    limit: Option<i64>,
) -> Result<Vec<types::SequenceListEntry>, Error> {
    let names = sqlx::query_scalar!(
        r#"
            SELECT locator_name
            FROM sequence_t
            WHERE locator_name > $1
            ORDER BY locator_name
            LIMIT $2
        "#,
        after,
        limit,
    )
    .fetch_all(exe.as_exec())
    .await?;

    names
        .into_iter()
        .map(|name| {
            let locator = parse_locator(name)?;
            Ok(types::SequenceListEntry {
                cursor: types::SequenceCursor::Name(locator.clone()),
                locator,
            })
        })
        .collect()
}

fn parse_locator(name: String) -> Result<types::SequenceLocator, Error> {
    name.parse()
        .map_err(|_| Error::BadData(format!("invalid sequence locator in DB `{name}`")))
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

    // ////////////////////////////////////////////////////////////////////////
    // PAGINATED LISTING
    // ////////////////////////////////////////////////////////////////////////

    use types::{ListLimit, ListStart, SequenceOrder};

    /// Inserts a sequence with an explicit creation timestamp.
    async fn insert(exe: &mut impl AsExec, name: &str, created_at: i64) {
        sqlx::query(
            "INSERT INTO sequence_t (sequence_uuid, locator_name, creation_unix_tstamp, path_in_store)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(name)
        .bind(created_at)
        .bind(format!("sq_{name}"))
        .execute(exe.as_exec())
        .await
        .unwrap();
    }

    fn names(page: &[types::SequenceListEntry]) -> Vec<String> {
        page.iter().map(|e| e.locator.to_string()).collect()
    }

    /// Walks the whole listing page by page, returning each page's names.
    async fn walk(exe: &mut impl AsExec, order: SequenceOrder, page_size: u32) -> Vec<Vec<String>> {
        let mut pages = vec![];
        let mut start = ListStart::First(order);

        loop {
            let page = sequence_find_page(exe, &start, ListLimit::from(page_size))
                .await
                .unwrap();
            pages.push(names(&page));

            if page.len() < page_size as usize {
                return pages;
            }
            start = ListStart::After(page.last().unwrap().cursor.clone());
        }
    }

    /// Creation order and name order differ, to tell the two apart.
    async fn insert_fixture(exe: &mut impl AsExec) {
        insert(exe, "charlie", 100).await;
        insert(exe, "alpha", 200).await;
        insert(exe, "echo", 300).await;
        insert(exe, "bravo", 400).await;
        insert(exe, "delta", 500).await;
    }

    #[sqlx::test]
    async fn page_walk_by_created_at(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        insert_fixture(&mut cx).await;

        let pages = walk(&mut cx, SequenceOrder::CreatedAt, 2).await;

        assert_eq!(
            pages,
            vec![
                vec!["charlie", "alpha"],
                vec!["echo", "bravo"],
                vec!["delta"]
            ]
        );
    }

    #[sqlx::test]
    async fn page_walk_by_name(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        insert_fixture(&mut cx).await;

        let pages = walk(&mut cx, SequenceOrder::Name, 2).await;

        assert_eq!(
            pages,
            vec![
                vec!["alpha", "bravo"],
                vec!["charlie", "delta"],
                vec!["echo"]
            ]
        );
    }

    /// Sequences sharing a creation timestamp are ordered by id, and each is listed once.
    #[sqlx::test]
    async fn created_at_ties_are_split_by_id(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        insert(&mut cx, "first", 100).await;
        insert(&mut cx, "second", 100).await;
        insert(&mut cx, "third", 100).await;

        let pages = walk(&mut cx, SequenceOrder::CreatedAt, 1).await;

        assert_eq!(
            pages,
            vec![vec!["first"], vec!["second"], vec!["third"], vec![]]
        );
    }

    /// A cursor stays valid after the sequence it was built from is deleted.
    #[sqlx::test]
    async fn cursor_of_deleted_sequence(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        insert_fixture(&mut cx).await;

        for order in [SequenceOrder::CreatedAt, SequenceOrder::Name] {
            let first = sequence_find_page(&mut cx, &ListStart::First(order), ListLimit::from(2))
                .await
                .unwrap();
            let last = first.last().unwrap();
            let after = ListStart::After(last.cursor.clone());

            let expected = names(
                &sequence_find_page(&mut cx, &after, ListLimit::Unlimited)
                    .await
                    .unwrap(),
            );

            let mut tx = database.transaction().await.unwrap();
            sequence_delete_by_locator(&mut tx, &last.locator, types::allow_data_loss())
                .await
                .unwrap();

            let next = sequence_find_page(&mut tx, &after, ListLimit::Unlimited)
                .await
                .unwrap();
            assert_eq!(names(&next), expected, "{order:?}");
            // Roll back, so the next order starts from the same fixture.
            drop(tx);
        }
    }

    /// A live listing: sequences created after the cursor show up, those before it don't.
    #[sqlx::test]
    async fn sequences_created_between_pages(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        insert_fixture(&mut cx).await;

        let first = sequence_find_page(
            &mut cx,
            &ListStart::First(SequenceOrder::CreatedAt),
            ListLimit::from(2),
        )
        .await
        .unwrap();
        assert_eq!(names(&first), vec!["charlie", "alpha"]);

        insert(&mut cx, "behind", 150).await;
        insert(&mut cx, "ahead", 600).await;

        let rest = sequence_find_page(
            &mut cx,
            &ListStart::After(first.last().unwrap().cursor.clone()),
            ListLimit::Unlimited,
        )
        .await
        .unwrap();
        assert_eq!(names(&rest), vec!["echo", "bravo", "delta", "ahead"]);
    }
}
