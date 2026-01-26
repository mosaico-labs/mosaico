use crate::{self as repo, sql_models};
use mosaicod_core::types;
use std::collections::HashMap;

pub async fn sequences_group_from_topics(
    exe: &mut impl repo::AsExec,
    topics: impl Iterator<Item = &sql_models::TopicRecord>,
) -> Result<Vec<types::SequenceTopicGroup>, repo::Error> {
    let mut ret: HashMap<i32, types::SequenceTopicGroup> = HashMap::new();

    for topic in topics {
        let group = ret.get_mut(&topic.sequence_id);
        if let Some(group) = group {
            group.topics.push(types::TopicResourceLocator::from(
                topic.locator_name.clone(),
            ));
        } else {
            let seq = repo::sequence_find_by_id(exe, topic.sequence_id).await?;
            ret.insert(
                seq.sequence_id,
                types::SequenceTopicGroup::new(
                    types::SequenceResourceLocator::from(seq.locator_name),
                    vec![types::TopicResourceLocator::from(
                        topic.locator_name.clone(),
                    )],
                ),
            );
        }
    }

    Ok(ret.into_values().collect())
}
