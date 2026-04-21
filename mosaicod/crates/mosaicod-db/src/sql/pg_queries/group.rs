use super::*;
use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use std::collections::HashMap;

pub async fn sequences_group_from_topics(
    exe: &mut impl AsExec,
    topics: impl Iterator<Item = &schema::TopicRecord>,
) -> Result<Vec<types::SequenceTopicGroup>, Error> {
    let mut ret: HashMap<i32, types::SequenceTopicGroup> = HashMap::new();

    for topic in topics {
        let group = ret.get_mut(&topic.sequence_id);
        if let Some(group) = group {
            group.topics.push(
                topic
                    .locator_name
                    .parse()
                    .map_err(|_| Error::BadData(topic.locator_name.clone()))?,
            );
        } else {
            let seq = sequence_find_by_id(exe, topic.sequence_id).await?;
            ret.insert(
                seq.sequence_id,
                types::SequenceTopicGroup::new(
                    seq.locator_name
                        .parse::<types::SequenceLocator>()
                        .map_err(|_| Error::BadData(seq.locator_name))?,
                    vec![
                        topic
                            .locator_name
                            .parse()
                            .map_err(|_| Error::BadData(topic.locator_name.clone()))?,
                    ],
                ),
            );
        }
    }

    Ok(ret.into_values().collect())
}
