//! Flight DoAction endpoint implementation.
//!
//! This module implements the main dispatcher for Flight DoAction requests,
//! delegating to specialized handler functions for each action category.

use super::actions::{layer, query as query_action, sequence, topic};
use crate::{endpoints::Context, errors::ServerError};
use mosaicod_marshal::{ActionRequest, ActionResponse};

/// Dispatches a Flight action request to the appropriate handler.
///
/// This function serves as the main entry point for all Flight DoAction requests,
/// routing each action type to its specialized handler function.
pub async fn do_action(ctx: Context, action: ActionRequest) -> Result<ActionResponse, ServerError> {
    match action {
        // Sequence actions
        ActionRequest::SequenceCreate(data) => {
            let user_metadata = data.user_metadata()?;
            sequence::create(&ctx, data.name, user_metadata.as_str()).await
        }
        ActionRequest::SequenceDelete(data) => sequence::delete(&ctx, data.name).await,
        ActionRequest::SequenceAbort(data) => sequence::abort(&ctx, data.name, data.key).await,
        ActionRequest::SequenceFinalize(data) => {
            sequence::finalize(&ctx, data.name, data.key).await
        }
        ActionRequest::SequenceNotifyCreate(data) => {
            sequence::notify_create(&ctx, data.name, data.notify_type, data.msg).await
        }
        ActionRequest::SequenceNotifyList(data) => sequence::notify_list(&ctx, data.name).await,
        ActionRequest::SequenceNotifyPurge(data) => sequence::notify_purge(&ctx, data.name).await,
        ActionRequest::SequenceSystemInfo(data) => sequence::system_info(&ctx, data.name).await,

        // Topic actions
        ActionRequest::TopicCreate(data) => {
            let user_metadata = data.user_metadata()?;
            topic::create(
                &ctx,
                data.name,
                data.sequence_key,
                data.serialization_format.into(),
                data.ontology_tag,
                user_metadata.as_str(),
            )
            .await
        }
        ActionRequest::TopicDelete(data) => topic::delete(&ctx, data.name).await,
        ActionRequest::TopicNotifyCreate(data) => {
            topic::notify_create(&ctx, data.name, data.notify_type, data.msg).await
        }
        ActionRequest::TopicNotifyList(data) => topic::notify_list(&ctx, data.name).await,
        ActionRequest::TopicNotifyPurge(data) => topic::notify_purge(&ctx, data.name).await,
        ActionRequest::TopicSystemInfo(data) => topic::system_info(&ctx, data.name).await,

        // Layer actions
        ActionRequest::LayerCreate(data) => layer::create(&ctx, data.name, data.description).await,
        ActionRequest::LayerDelete(data) => layer::delete(&ctx, data.name).await,
        ActionRequest::LayerUpdate(data) => {
            layer::update(&ctx, data.prev_name, data.curr_name, data.curr_description).await
        }
        ActionRequest::LayerList(_) => layer::list(&ctx).await,

        // Query actions
        ActionRequest::Query(data) => query_action::execute(&ctx, data.query).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types::{self, MetadataBlob};
    use mosaicod_marshal as marshal;
    use mosaicod_query as query;
    use mosaicod_repo::{self as repo, FacadeSequence, FacadeTopic};
    use mosaicod_store as store;
    use std::sync::Arc;

    /// Creates an empty sequence (no data) for testing purposes.
    async fn create_empty_sequence(
        repo: &repo::testing::Repository,
        store: &store::testing::Store,
        name: &str,
    ) -> Result<types::ResourceId, repo::FacadeError> {
        let handle = FacadeSequence::new(name.to_owned(), (*store).clone(), (*repo).clone());

        let metadata = types::SequenceMetadata::new(
            marshal::JsonMetadataBlob::try_from_str(
                r#"{
                    "test_field_1" : "value1",
                    "test_field_2" : "value2"
                }"#,
            )
            .expect("Error parsing user metadata"),
        );

        let record = handle.create(Some(metadata)).await?;

        Ok(record)
    }

    /// Creates an empty topic (no data) for testing purposes.
    async fn create_empty_topic(
        repo: &repo::testing::Repository,
        store: &store::testing::Store,
        sequence: &types::ResourceId,
        name: &str,
    ) -> Result<types::ResourceId, repo::FacadeError> {
        let handle = FacadeTopic::new(name.to_owned(), (*store).clone(), (*repo).clone());
        let props = types::TopicProperties::new(types::Format::Default, "test_tag".to_owned());

        let metadata = types::TopicMetadata::new(
            props,
            marshal::JsonMetadataBlob::try_from_str(
                r#"{
                    "test_field_1" : "test_value_1",
                    "test_field_2" : "test_value_2"
                }"#,
            )
            .expect("Error parsing user metadata json string"),
        );

        let record = handle.create(&sequence.uuid, Some(metadata)).await?;

        Ok(record)
    }

    #[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
    /// This test checks the creation against the repository and compares values to check if
    /// the creation was successful.
    async fn sequence_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let name = "/test_sequence".to_owned();

        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_engine = query::Timeseries::try_new(store.clone()).unwrap();

        #[derive(serde::Serialize, Debug)]
        struct Request {
            name: String,
            user_metadata: serde_json::Value,
        }

        let request = Request {
            name: name.clone(),
            user_metadata: serde_json::from_str(
                r#"{
                    "field1": "value1",
                    "field2": "value2"
                }"#,
            )
            .unwrap(),
        };

        let request_raw = serde_json::to_string(&request).unwrap();

        let action = ActionRequest::try_new("sequence_create", request_raw.as_bytes())
            .expect("Unable to create action from string");

        let ctx = Context::new((*store).clone(), repo.clone(), Arc::new(ts_engine));
        let response = do_action(ctx, action).await.unwrap();

        if let ActionResponse::SequenceCreate(_) = response {
            let handle = repo::FacadeSequence::new(name, (*store).clone(), repo.clone());

            let user_metadata: serde_json::Value =
                handle.metadata().await.unwrap().user_metadata.into();

            // Check that user_metadata are saved correctly
            assert_eq!(request.user_metadata, user_metadata);
        } else {
            panic!("wrong response returned")
        }

        Ok(())
    }

    #[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
    /// Test checking if the creation of an already existing sequence fails.
    async fn sequence_create_existing(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let name = "test_sequence".to_owned();
        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();

        // Create a first sequence to then try to create it again
        create_empty_sequence(&repo, &store, &name).await.unwrap();

        let result = create_empty_sequence(&repo, &store, &name).await;

        if result.is_ok() {
            panic!("sequence creation should have failed");
        }

        Ok(())
    }

    #[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
    /// Test checking if the creation of a topic succeeds.
    async fn topic_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let sequence_name = "test_sequence".to_owned();
        let topic_name = "test_sequence/test_topic".to_owned();

        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();

        // Create a first sequence to then try to create it again
        let sequence = create_empty_sequence(&repo, &store, &sequence_name)
            .await
            .unwrap();

        let _ = create_empty_topic(&repo, &store, &sequence, &topic_name)
            .await
            .unwrap();

        Ok(())
    }

    #[sqlx::test(migrator = "mosaicod_repo::testing::MIGRATOR")]
    /// Test checking if the creation of a topic with unauthorized name fails.
    async fn topic_create_unauthorized(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let sequence_name = "test_sequence".to_owned();
        let topic_name = "test_topic".to_owned();

        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();

        // Create a first sequence to then try to create it again
        let sequence = create_empty_sequence(&repo, &store, &sequence_name)
            .await
            .unwrap();

        // This should fail since the topic name is not a child of the sequence
        let topic = create_empty_topic(&repo, &store, &sequence, &topic_name).await;

        if topic.is_ok() {
            panic!("the topic creation should fail")
        }

        Ok(())
    }
}
