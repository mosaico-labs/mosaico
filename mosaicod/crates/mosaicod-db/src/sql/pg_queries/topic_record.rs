use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use sqlx::{Row, postgres::PgRow};
use tracing::{trace, warn};

fn cast_topic_data(row: PgRow) -> Result<schema::TopicRecord, Error> {
    Ok(schema::TopicRecord {
        topic_id: row.try_get("topic_id")?,
        topic_uuid: row.try_get("topic_uuid")?,
        locator_name: row.try_get("locator_name")?,
        sequence_id: row.try_get("sequence_id")?,
        session_id: row.try_get("session_id")?,
        ontology_tag: row.try_get("ontology_tag")?,
        serialization_format: row.try_get("serialization_format")?,
        user_metadata: row.try_get("user_metadata")?,
        path_in_store: row.try_get("path_in_store")?,
        creation_unix_tstamp: row.try_get("creation_unix_tstamp")?,
        completion_unix_tstamp: row.try_get("completion_unix_tstamp")?,
        start_index_timestamp: row.try_get("start_index_timestamp")?,
        end_index_timestamp: row.try_get("end_index_timestamp")?,
        optimization_end_unix_tstamp: row.try_get("optimization_end_unix_tstamp")?,
    })
}

/// Find a topic given its uuid.
pub async fn topic_find_by_ids(
    exe: &mut impl AsExec,
    ids: &[i32],
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics with the following ids `{:?}`", ids);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE topic_id = ANY($1)",
        ids
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a topic given its name.
pub async fn topic_find_by_locator(
    exe: &mut impl AsExec,
    topic: &types::TopicLocator,
) -> Result<schema::TopicRecord, Error> {
    trace!("searching topic by locator name `{}`", topic);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE locator_name=$1",
        topic.to_string()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a topic given its UUID.
pub async fn topic_find_by_uuid(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
) -> Result<schema::TopicRecord, Error> {
    trace!("searching by resource UUID `{}`", uuid);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE topic_uuid=$1",
        uuid.as_ref()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a topic given its id.
pub async fn topic_find_by_id(
    exe: &mut impl AsExec,
    topic_id: i32,
) -> Result<schema::TopicRecord, Error> {
    trace!("searching topic by id `{}`", topic_id);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE topic_id=$1",
        topic_id
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Search for a topic with the given path_in_store.
pub async fn topic_find_path_in_store(
    exe: &mut impl AsExec,
    path_in_store: &str,
) -> Result<bool, Error> {
    trace!(
        "searching if path_in_store `{}` is assigned to a topic",
        path_in_store
    );
    let found: bool = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM topic_t WHERE path_in_store=$1) as "found!""#,
        path_in_store
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(found)
}

/// Return all topics
pub async fn topic_find_all(exe: &mut impl AsExec) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("retrieving all topics");
    Ok(
        sqlx::query_as!(schema::TopicRecord, "SELECT * FROM topic_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}

/// Deletes a topic record from the database by its id, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] since permanently removes the record
/// from the database without checking whether it is locked or referenced
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn topic_delete(
    exe: &mut impl AsExec,
    locator: &types::TopicLocator,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting topic record {}", locator);
    let result = sqlx::query!(
        "DELETE FROM topic_t WHERE locator_name=$1",
        locator.to_string()
    )
    .execute(exe.as_exec())
    .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

    Ok(())
}

pub async fn topic_create(
    exe: &mut impl AsExec,
    locator: &types::TopicLocator,
    session_uuid: types::Uuid,
    ontology_tag: &str,
    serialization_format: &str,
    path_in_store: Option<types::TopicPathInStore>,
    user_metadata: Option<serde_json::Value>,
) -> Result<schema::TopicRecord, Error> {
    trace!("creating a new topic {}", locator);

    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            INSERT INTO topic_t
                (
                    topic_uuid, sequence_id, session_id, locator_name, creation_unix_tstamp,
                    serialization_format, ontology_tag, user_metadata, path_in_store
                )
            SELECT $1, seq.sequence_id, sess.session_id, $2, $3, $4, $5, $6, $7
            FROM sequence_t as seq JOIN session_t as sess ON sess.sequence_id = seq.sequence_id
            WHERE seq.locator_name = $8 AND sess.session_uuid = $9 AND sess.completion_unix_tstamp IS NULL
            RETURNING topic_t.*
            "#,
        uuid::Uuid::from(types::Uuid::new()),
        locator.to_string(),
        types::Timestamp::now().as_i64(),
        serialization_format,
        ontology_tag,
        user_metadata,
        path_in_store.map(|x| x.to_string()),
        locator.sequence.to_string(),
        uuid::Uuid::from(session_uuid)
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_serialization_format(
    exe: &mut impl AsExec,
    loc: &types::TopicLocator,
    serialization_format: &str,
) -> Result<schema::TopicRecord, Error> {
    trace!(
        "updating serialization_format to `{}` for `{}`",
        serialization_format, loc
    );
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET serialization_format = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        serialization_format,
        loc.to_string()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn topic_update_ontology_tag(
    exe: &mut impl AsExec,
    loc: &types::TopicLocator,
    ontology_tag: &str,
) -> Result<schema::TopicRecord, Error> {
    trace!("updating ontology_tag to `{}` for `{}`", ontology_tag, loc);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET ontology_tag = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        ontology_tag,
        loc.to_string(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_user_metadata(
    exe: &mut impl AsExec,
    loc: &types::TopicLocator,
    user_metadata: marshal::JsonMetadataBlob,
) -> Result<schema::TopicRecord, Error> {
    trace!("updating user_metadata for `{}`", loc);
    let metadata = serde_json::to_value(user_metadata)?;
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET user_metadata = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        metadata,
        loc.to_string(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_index_timestamp_range(
    exe: &mut impl AsExec,
    loc: &types::TopicLocator,
    ts_range: types::TimestampRange,
) -> Result<schema::TopicRecord, Error> {
    trace!(
        "updating index timestamp range to `{:?}` for topic `{}`",
        ts_range, loc
    );
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET start_index_timestamp = $1, end_index_timestamp = $2
            WHERE locator_name = $3
            RETURNING *
    "#,
        ts_range.start.as_i64(),
        ts_range.end.as_i64(),
        loc.to_string(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_completion_tstamp(
    exe: &mut impl AsExec,
    topic_id: i32,
    completion_ts: i64,
) -> Result<(), Error> {
    trace!(
        "updating completion timestamp to `{}` for topic `{}`",
        completion_ts, topic_id
    );
    sqlx::query!(
        r#"
            UPDATE topic_t
            SET completion_unix_tstamp = $1
            WHERE topic_id = $2
    "#,
        completion_ts,
        topic_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

pub async fn topic_update_path_in_store_if_null(
    exe: &mut impl AsExec,
    topic_id: i32,
    path_in_store: types::TopicPathInStore,
) -> Result<bool, Error> {
    trace!(
        "updating path_in_store to `{}` for topic with id {}",
        path_in_store, topic_id
    );

    let res = sqlx::query!(
        r#"
            UPDATE topic_t
            SET path_in_store = $1
            WHERE topic_id = $2 AND path_in_store IS NULL
            "#,
        Some(String::from(path_in_store)),
        topic_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected() != 0)
}

/// Updates optimization completion timestamp and path in store for the given [`topic_id`].
pub async fn topic_optimization_complete(
    exe: &mut impl AsExec,
    topic_id: i32,
    optimization_end_ts: i64,
    path_in_store: types::TopicPathInStore,
) -> Result<(), Error> {
    trace!("store optimization completed for topic `{}`", topic_id);
    sqlx::query!(
        r#"
            UPDATE topic_t
            SET optimization_end_unix_tstamp = $1, path_in_store = $2
            WHERE topic_id = $3
    "#,
        optimization_end_ts,
        Some(String::from(path_in_store)),
        topic_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

pub async fn topic_delete_path_in_store(
    exe: &mut impl AsExec,
    topic_id: i32,
) -> Result<bool, Error> {
    trace!("removing path_in_store for topic with id {}", topic_id);
    let res = sqlx::query!(
        r#"
            UPDATE topic_t
            SET path_in_store = NULL
            WHERE topic_id = $1
    "#,
        topic_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected() != 0)
}

pub async fn topic_from_query_filter(
    exe: &mut impl AsExec,
    filter_seq: Option<query::SequenceFilter>,
    filter_top: Option<query::TopicFilter>,
) -> Result<Vec<schema::TopicRecord>, Error> {
    // Return empty vector if there is nothing to filter
    if filter_seq.is_none() && filter_top.is_none() {
        return Ok(Vec::new());
    }

    let select = r#"
        SELECT topic.*
        FROM topic_t topic
        INNER JOIN sequence_t sequence
            ON topic.sequence_id = sequence.sequence_id
    "#;

    let mut qb = query::ClausesCompiler::new();

    let placeholder = query::Placeholder::new();

    let mut sql_fmt = super::SqlQueryCompiler::new(placeholder.clone());
    let mut json_fmt = super::JsonQueryCompiler::new(placeholder);

    if let Some(seq) = filter_seq {
        if let Some(op) = seq.name {
            qb = qb.expr("sequence.locator_name", op, &mut sql_fmt);
        }

        if let Some(op) = seq.created_at {
            qb = qb.expr("sequence.creation_unix_tstamp", op, &mut sql_fmt);
        }

        let fmt = json_fmt.with_field("sequence.user_metadata".into());

        for (field, op) in seq.user_metadata {
            qb = qb.expr(&field, op, fmt);
        }
    }

    if let Some(top) = filter_top {
        if let Some(op) = top.name {
            // Substring has a +1 for zero based indexing
            //
            // So using sequence locator '/a/b' and topic locator 'a/b/c/d'
            // the query produces '/c/d', slash included
            qb = qb.expr(
                "SUBSTRING(topic.locator_name, LENGTH(sequence.locator_name) + 1)",
                op,
                &mut sql_fmt,
            );
        }

        if let Some(op) = top.created_at {
            qb = qb.expr("topic.creation_unix_tstamp", op, &mut sql_fmt);
        }

        if let Some(op) = top.ontology_tag {
            qb = qb.expr("topic.ontology_tag", op, &mut sql_fmt);
        }

        if let Some(op) = top.serialization_format {
            qb = qb.expr("topic.serialization_format", op, &mut sql_fmt);
        }

        let fmt = json_fmt.with_field("topic.user_metadata".into());

        for (field, op) in top.user_metadata {
            qb = qb.expr(&field, op, fmt);
        }
    }

    let qr = qb.compile()?;

    // If the query has no filters skip, to avoid retuning too much elements
    if qr.is_unfiltered() {
        return Ok(Vec::new());
    }

    // Since we have done an early-return is the query is unfiltered there is always a WHERE clause
    let query = format!("{select} WHERE {}", qr.clauses.join(" AND "));

    trace!("query values: {:?}", qr.values);
    trace!("generated SQL query: {}", query);

    let mut r = sqlx::query(&query);

    for v in qr.values.into_iter() {
        match v {
            query::Value::Integer(v) => r = r.bind(v),
            query::Value::Float(v) => r = r.bind(v),
            query::Value::Text(v) => r = r.bind(v),
            query::Value::Boolean(v) => r = r.bind(v),
            query::Value::IntegerArray(_)
            | query::Value::FloatArray(_)
            | query::Value::TextArray(_)
            | query::Value::BooleanArray(_) => {
                unreachable!("array values are not produced by SQL/JSON query compilers")
            }
        }
    }

    let r = r.map(cast_topic_data).fetch_all(exe.as_exec()).await?;
    trace!("query returned {} results", r.len());
    r.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DatabaseType, testing};
    use crate::{sequence_create, session_create};
    use mosaicod_core::types::MetadataBlob;
    use mosaicod_marshal as marshal;
    use sqlx::Pool;

    async fn setup_fake_db(database: crate::Database) {
        let mut cx = database.connection();

        let metadata = marshal::JsonMetadataBlob::try_from_str(
            r#"{"key1": ["value", "value2", "val3"], "key3": { "key4": "value1" }, "key4": "value2", "key5": 200 }"#,
        )
            .unwrap();

        let sequence_record = sequence_create(
            &mut cx,
            &"my_sequence".parse().unwrap(),
            &"/my/path/in/store".to_owned().into(),
            Some(metadata.into()),
        )
        .await
        .unwrap();

        let session_record = session_create(
            &mut cx,
            &types::SessionLocator::new(sequence_record.locator()),
        )
        .await
        .unwrap();

        let metadata = marshal::JsonMetadataBlob::try_from_str(
            r#"{"key1": "value1", "key2": 100, "key3": { "key4": "value2", "key5": 100 }}"#,
        )
        .unwrap();

        topic_create(
            &mut database.connection(),
            &"my_sequence/topic".parse().unwrap(),
            session_record.uuid(),
            "",
            "",
            Some("/my/path/in/store".to_owned().into()),
            Some(metadata.into()),
        )
        .await
        .unwrap();

        let metadata = marshal::JsonMetadataBlob::try_from_str(
            r#"{"key1": "value1", "key3": { "key4": "value2" }, "key4": "value2", "key5": 100, "key6": true }"#,
        )
            .unwrap();

        topic_create(
            &mut database.connection(),
            &"my_sequence/topic2".parse().unwrap(),
            session_record.uuid(),
            "",
            "",
            Some("/my/path/in/store".to_owned().into()),
            Some(metadata.into()),
        )
        .await
        .unwrap();

        // Create second sequence.
        let metadata = marshal::JsonMetadataBlob::try_from_str(
            r#"{"key1": ["value", "val", "val2"], "key3": { "key4": "value1", "key8": true }, "key4": "value2", "key5": 100 }"#,
        )
            .unwrap();

        let sequence2_record = sequence_create(
            &mut cx,
            &"my_sequence2".parse().unwrap(),
            &"/my/path/in/store".to_owned().into(),
            Some(metadata.into()),
        )
        .await
        .unwrap();

        let session2_record = session_create(
            &mut cx,
            &types::SessionLocator::new(sequence2_record.locator()),
        )
        .await
        .unwrap();

        let metadata = marshal::JsonMetadataBlob::try_from_str(
            r#"{"key1": "value1", "key2": [ {"key1": 4, "key6": 5}, {"key3": false, "key4": "value"} ], "key3": { "key4": "value2", "key5": { "key6": "value6", "key7": [ 1, 3, 6 ], "key1": "value1" } }, "key4": "value2", "key6": 1 }"#,
        )
            .unwrap();

        topic_create(
            &mut database.connection(),
            &"my_sequence2/topic".parse().unwrap(),
            session2_record.uuid(),
            "",
            "",
            Some("/my/path/in/store".to_owned().into()),
            Some(metadata.into()),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_topic_from_query_filter_match_regex(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        setup_fake_db(database.clone()).await;

        let mut cx = database.connection();

        // This returns a match because of the default json path LAX mode (no need for [*]).
        let filter = r#"{"sequence": {"locator": {"$eq": "my_sequence"}, "user_metadata": {"key1": {"$match": "value2"}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].locator_name, "my_sequence/topic");
        assert_eq!(res[1].locator_name, "my_sequence/topic2");

        let filter = r#"{"sequence": {"locator": {"$eq": "my_sequence"}, "user_metadata": {"key1": {"$match": "value, value2"}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();
        assert!(res.is_empty());

        // Test single * to match anything....
        let filter = r#"{"sequence": {"user_metadata": {"key4": {"$match": "*"}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].locator_name, "my_sequence/topic");
        assert_eq!(res[1].locator_name, "my_sequence/topic2");
        assert_eq!(res[2].locator_name, "my_sequence2/topic");
    }

    #[sqlx::test]
    async fn test_topic_from_query_filter_with_glob_pattern(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        setup_fake_db(database.clone()).await;

        let mut cx = database.connection();

        // Search for "key6" at third level existence inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"*.*.key6": {"$ex": null}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].locator_name, "my_sequence2/topic");

        // Search for "key5" with value 100 at second level inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"*.key5": {"$eq": 100}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].locator_name, "my_sequence/topic");

        // Search for list element with "value2" inside sequence's user metadata.
        let filter = r#"{"sequence": {"locator": {"$match": "my_sequence"}, "user_metadata": {"key1[*]": { "$eq": "value2"}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].locator_name, "my_sequence/topic");
        assert_eq!(res[1].locator_name, "my_sequence/topic2");

        // Search for "key8" as list item inside sequence's user metadata. This works beacuse Postgres uses LAX mode by default.
        let filter = r#"{"sequence": {"locator": {"$match": "my_sequence"}, "user_metadata": {"*[*].key8": { "$eq": true}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].locator_name, "my_sequence2/topic");

        // Search for any key with value in [1, 100, 200] at first level inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"*": {"$in": [1, 100, 200]}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].locator_name, "my_sequence/topic");
        assert_eq!(res[1].locator_name, "my_sequence/topic2");
        assert_eq!(res[2].locator_name, "my_sequence2/topic");
    }

    #[sqlx::test]
    async fn test_topic_from_query_filter_with_recursive_glob_pattern(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        setup_fake_db(database.clone()).await;

        let mut cx = database.connection();

        // Triple * is not allowed.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"***.key5": {"$eq": 100}}}}"#;
        let err = marshal::query_filter_from_string(filter).unwrap_err();
        assert!(matches!(err, marshal::Error::DeserializationError(_)));

        // Search for "key5" at every level inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"**.key5": {"$eq": 100}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].locator_name, "my_sequence/topic");
        assert_eq!(res[1].locator_name, "my_sequence/topic2");

        // Search for "key6" inside an array in topics' metadata. This returns 2 results because Postgres operates in LAX mode by default.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"**[*].key6": {"$ex": null}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].locator_name, "my_sequence/topic2");
        assert_eq!(res[1].locator_name, "my_sequence2/topic");

        // Search for any key with value in [1, 100, 200] at any level inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"**": {"$in": ["value5", "value6", true]}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].locator_name, "my_sequence/topic2");
        assert_eq!(res[1].locator_name, "my_sequence2/topic");

        // Search for any key with value in [1, 100, 200] at any level inside topics' user metadata.
        let filter = r#"{"topic": {"locator": {"$match": "topic"}, "user_metadata": {"**[*]": {"$eq": false}}}}"#;
        let filter = marshal::query_filter_from_string(filter).unwrap();
        let res = topic_from_query_filter(&mut cx, filter.sequence, filter.topic)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].locator_name, "my_sequence2/topic");
    }
}
