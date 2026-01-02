use log::{info, trace, warn};

use crate::{
    marshal::{self, ActionRequest, ActionResponse},
    query,
    repo::{self, FacadeError, FacadeLayer, FacadeQuery, FacadeSequence, FacadeTopic},
    server::errors::ServerError,
    store, types,
    types::{MetadataBlob, Resource},
};

pub async fn do_action(
    store: store::StoreRef,
    repo: repo::Repository,
    ts_engine: query::TimeseriesGwRef,
    action: ActionRequest,
) -> Result<ActionResponse, ServerError> {
    let response = match action {
        ActionRequest::SequenceCreate(data) => {
            info!("requested resource {} creation", data.name);

            let handle = FacadeSequence::new(data.name.clone(), store, repo);

            // Check if sequence exists, if so return with an error
            let r_id = handle.resource_id().await;
            if r_id.is_ok() {
                return Err(ServerError::SequenceAlreadyExists(
                    handle.locator.name().into(),
                ));
            }

            let str_mdata = data.user_metadata()?;
            let user_mdata = marshal::JsonMetadataBlob::try_from_str(str_mdata.as_str())
                .map_err(FacadeError::from)?;

            // no sequence record was found, let's write it
            let metadata = types::SequenceMetadata::new(user_mdata);
            let r_id = handle.create(Some(metadata)).await?;

            trace!(
                "created resource {} with uuid {}",
                handle.locator, r_id.uuid
            );
            ActionResponse::SequenceCreate(r_id.into())
        }

        ActionRequest::SequenceDelete(data) => {
            warn!("requested deletion of resource {}", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);

            if handle.is_locked().await? {
                return Err(ServerError::SequenceLocked);
            }

            let loc = handle.locator.clone();
            handle.delete().await?;
            warn!("resource {} deleted", loc);

            ActionResponse::Empty
        }

        ActionRequest::SequenceAbort(data) => {
            warn!("abort for {}", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);

            // Avoid aborting on locked sequences
            if handle.is_locked().await? {
                return Err(ServerError::SequenceLocked);
            }

            // Check that sequence id and provided key matches
            let r_id = handle.resource_id().await?;
            let received_uuid: uuid::Uuid = data.key.parse()?;
            if r_id.uuid != received_uuid {
                return Err(ServerError::BadKey);
            }

            // Save handle name (for logging) since the delete will consume the handle
            let loc = handle.locator.clone();
            handle.delete().await?;
            warn!("resource {} deleted", loc.name());

            ActionResponse::Empty
        }

        ActionRequest::SequenceFinalize(data) => {
            info!("resource {} finalized", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);

            // Check that key matches the sequence id
            let r_id = handle.resource_id().await?;
            let received_uuid: uuid::Uuid = data.key.parse()?;

            if r_id.uuid != received_uuid {
                return Err(ServerError::BadKey);
            }

            handle.lock().await?;
            trace!("resource {} locked", handle.locator);

            ActionResponse::Empty
        }

        ActionRequest::SequenceNotifyCreate(data) => {
            info!("new notify for {}", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);
            let ntype: types::NotifyType = data.notify_type.parse()?;
            handle.notify(ntype, data.msg).await?;

            ActionResponse::Empty
        }

        ActionRequest::SequenceNotifyList(data) => {
            info!("notify list for {}", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);

            // Convert notifies to response messages
            let notifies = handle.notify_list().await?;

            ActionResponse::SequenceNotifyList(notifies.into())
        }

        ActionRequest::SequenceNotifyPurge(data) => {
            warn!("notify purge for {}", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);
            handle.notify_purge().await?;

            ActionResponse::Empty
        }

        ActionRequest::TopicCreate(data) => {
            info!("requested resource {} creation", data.name);

            // Find associated sequence
            let handle = FacadeTopic::new(data.name.clone(), store, repo);

            // Check if the topic has already been created
            let r_id = handle.resource_id().await;
            if r_id.is_ok() {
                return Err(ServerError::TopicAlreadyExists(
                    handle.locator.name().into(),
                ));
            }

            // Get all metadata from the request and create a topic record
            // into the repository
            //
            // The double error conversion happends because we need to
            // tell the compiler that the error is coming into a topic
            // related action
            let user_mdata =
                marshal::JsonMetadataBlob::try_from_str(data.user_metadata()?.as_str())
                    .map_err(FacadeError::from)?;

            let mdata = types::TopicMetadata::new(
                types::TopicProperties::new(data.serialization_format, data.ontology_tag),
                user_mdata,
            );

            let received_uuid: uuid::Uuid = data.sequence_key.parse()?;

            let r_id = handle.create(&received_uuid, Some(mdata)).await?;

            trace!(
                "resource {} created with uuid {}",
                handle.locator, r_id.uuid,
            );

            ActionResponse::TopicCreate(r_id.into())
        }

        ActionRequest::TopicDelete(data) => {
            warn!("requested deletion of resource {}", data.name);

            let handle = FacadeTopic::new(data.name.clone(), store, repo);

            if handle.is_locked().await? {
                return Err(ServerError::SequenceLocked);
            }

            handle.delete().await?;
            warn!("resource {} deleted", data.name);

            ActionResponse::Empty
        }

        ActionRequest::TopicNotifyCreate(data) => {
            info!("nofity for {}", data.name);

            let handle = FacadeTopic::new(data.name, store, repo);
            handle.notify(data.notify_type.parse()?, data.msg).await?;

            ActionResponse::Empty
        }

        ActionRequest::TopicNotifyList(data) => {
            info!("notify list for {}", data.name);

            let handle = FacadeTopic::new(data.name, store, repo);
            let notifies = handle.notify_list().await?;
            ActionResponse::TopicNotifyList(notifies.into())
        }

        ActionRequest::TopicNotifyPurge(data) => {
            warn!("nofity purge for {}", data.name);

            let handle = FacadeTopic::new(data.name, store, repo);
            handle.notify_purge().await?;

            ActionResponse::Empty
        }

        ActionRequest::SequenceSystemInfo(data) => {
            info!("[{}] sequence system informations", data.name);

            let handle = FacadeSequence::new(data.name, store, repo);
            let sysinfo = handle.system_info().await?;

            ActionResponse::SequenceSystemInfo(sysinfo.into())
        }

        ActionRequest::TopicSystemInfo(data) => {
            info!("[{}] topic system informations", data.name);

            let handle = FacadeTopic::new(data.name, store, repo);
            let sysinfo = handle.system_info().await?;

            ActionResponse::TopicSystemInfo(sysinfo.into())
        }

        ActionRequest::LayerCreate(data) => {
            info!("creating layer `{}`", data.name);

            let handle = FacadeLayer::new(
                types::LayerLocator::from(data.name.as_str()), //
                store,
                repo,
            );
            handle.create(data.description).await?;

            ActionResponse::Empty
        }

        ActionRequest::LayerDelete(data) => {
            warn!("deleting layer `{}`", data.name);

            let handle = FacadeLayer::new(
                types::LayerLocator::from(data.name.as_str()), //
                store,
                repo,
            );
            handle.delete().await?;

            ActionResponse::Empty
        }

        ActionRequest::LayerUpdate(data) => {
            info!(
                "updating layer `{}` with new name `{}` and new description `{}`",
                data.prev_name, data.curr_name, data.curr_description
            );

            let handle = FacadeLayer::new(
                types::LayerLocator::from(data.prev_name.as_str()),
                store,
                repo,
            );
            handle
                .update(
                    types::LayerLocator::from(data.curr_name.as_str()),
                    &data.curr_description,
                )
                .await?;

            ActionResponse::Empty
        }

        ActionRequest::LayerList(_) => {
            info!("request layer list");

            let layers = FacadeLayer::all(repo).await?;

            ActionResponse::LayerList(layers.into())
        }

        ActionRequest::Query(data) => {
            info!("performing a query");

            let filter = marshal::query_filter_from_serde_value(data.query)?;

            trace!("query filter: {:?}", filter);

            let groups = FacadeQuery::query(filter, ts_engine, repo).await?;

            trace!("groups found: {:?}", groups);

            ActionResponse::Query(groups.into())
        }
    };

    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::{repo, rw};

    /// Creates and empty sequence (no data) for testing purposes.
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

    /// Creates and empty sequence (no data) for testing purposes.
    async fn create_empty_topic(
        repo: &repo::testing::Repository,
        store: &store::testing::Store,
        sequence: &types::ResourceId,
        name: &str,
    ) -> Result<types::ResourceId, repo::FacadeError> {
        let handle = FacadeTopic::new(name.to_owned(), (*store).clone(), (*repo).clone());
        let props = types::TopicProperties::new(rw::Format::Default, "test_tag".to_owned());

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

    #[sqlx::test]
    /// This tests checks the creation against the repository and compares values to check if
    /// the creation was successful.
    async fn sequence_create(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let name = "/test_sequence".to_owned();

        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_engine = query::TimeseriesGw::try_new(store.clone()).unwrap();

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

        let response = do_action((*store).clone(), repo.clone(), Arc::new(ts_engine), action)
            .await
            .unwrap();

        if let ActionResponse::SequenceCreate(_) = response {
            let handle = repo::FacadeSequence::new(name, (*store).clone(), repo.clone());

            let user_metadata: serde_json::Value =
                handle.metadata().await.unwrap().user_metadata.into();

            // check that user_metadata are saved correcly
            assert_eq!(request.user_metadata, user_metadata);
        } else {
            panic!("wrong response return")
        }

        Ok(())
    }

    #[sqlx::test]
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

    #[sqlx::test]
    /// Test checking if the creation of an already existing sequence fails.
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

    #[sqlx::test]
    /// Test checking if the creation of an already existing sequence fails.
    async fn topic_create_unauthorized(pool: sqlx::Pool<repo::Database>) -> sqlx::Result<()> {
        let sequence_name = "test_sequence".to_owned();
        let topic_name = "test_topic".to_owned();

        let repo = repo::testing::Repository::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();

        // Create a first sequence to then try to create it again
        let sequence = create_empty_sequence(&repo, &store, &sequence_name)
            .await
            .unwrap();

        // this should fail since the topic name is not a child of the sequence
        let topic = create_empty_topic(&repo, &store, &sequence, &topic_name).await;

        if topic.is_ok() {
            panic!("the topic creation should fail")
        }

        Ok(())
    }
}
