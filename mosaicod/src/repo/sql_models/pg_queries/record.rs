use crate::{repo, types};

use super::sequence_record;
use super::topic_record;

pub async fn get_resource_locator_from_name(
    repo: &repo::Repository,
    name: &str,
) -> Result<Box<dyn types::Resource>, repo::Error> {
    let mut cx = repo.connection();

    let record = sequence_record::sequence_find_by_locator(
        &mut cx,
        &types::SequenceResourceLocator::from(name),
    )
    .await;
    if let Ok(sequence) = record {
        return Ok(Box::new(types::SequenceResourceLocator::from(
            sequence.locator_name,
        )));
    }

    let record = topic_record::topic_find_by_locator(
        &mut cx, //
        &types::TopicResourceLocator::from(name),
    )
    .await;
    if let Ok(topic) = record {
        return Ok(Box::new(types::TopicResourceLocator::from(
            topic.locator_name,
        )));
    }

    Err(repo::Error::NotFound)
}
