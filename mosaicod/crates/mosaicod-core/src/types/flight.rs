use crate::Error;
use crate::types;
use crate::types::TimestampRange;

/// Message used to initiate the flight communication to upload a new datastream
pub struct DoPutCmd {
    pub locator: String,
    pub key: String,
}

/// Request info on a mosaico resource (topic or sequence)
pub struct GetFlightInfoCmd {
    pub locator: String,
    pub timestamp_range: Option<TimestampRange>,
}

/// Request the schema of a mosaico topic
pub struct GetSchemaCmd {
    pub locator: String,
}

/// Request a page of sequences
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListFlightsCmd {
    pub locator: String,
    pub order: types::SequenceOrder,
    pub limit: types::ListLimit,
    pub cursor: Option<types::SequenceCursor>,
}

impl ListFlightsCmd {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.locator.is_empty() && self.locator != "/" {
            return Err(Error::unsupported_locator(self.locator.clone()));
        }

        if let Some(cursor) = &self.cursor
            && cursor.order() != self.order
        {
            return Err(Error::bad_request(
                "cursor does not match order_by".to_owned(),
            ));
        }

        Ok(())
    }
}

pub struct TicketTopic {
    /// Locator for the topic
    pub locator: types::TopicLocator,
    /// Optional timestamp range used to limit the data stream
    pub timestamp_range: Option<TimestampRange>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    fn name_cursor() -> types::SequenceCursor {
        types::SequenceCursor::Name("my_sequence".parse().unwrap())
    }

    fn created_at_cursor() -> types::SequenceCursor {
        types::SequenceCursor::CreatedAt {
            created_at: types::Timestamp::from(1_760_000_000_000_000_000),
            sequence_id: 42,
        }
    }

    #[test]
    fn default_cmd_is_valid() {
        assert!(ListFlightsCmd::default().validate().is_ok());
    }

    #[test]
    fn root_locators_are_valid() {
        for locator in ["", "/"] {
            let cmd = ListFlightsCmd {
                locator: locator.to_owned(),
                ..Default::default()
            };

            assert!(cmd.validate().is_ok(), "locator {locator:?}");
        }
    }

    #[test]
    fn non_root_locator_is_unsupported() {
        let cmd = ListFlightsCmd {
            locator: "my_sequence".to_owned(),
            ..Default::default()
        };

        let err = cmd.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnsupportedLocator(l) if l == "my_sequence"));
    }

    #[test]
    fn cursor_matching_order_is_valid() {
        let by_created_at = ListFlightsCmd {
            order: types::SequenceOrder::CreatedAt,
            cursor: Some(created_at_cursor()),
            ..Default::default()
        };
        let by_name = ListFlightsCmd {
            order: types::SequenceOrder::Name,
            cursor: Some(name_cursor()),
            ..Default::default()
        };

        assert!(by_created_at.validate().is_ok());
        assert!(by_name.validate().is_ok());
    }

    #[test]
    fn cursor_order_mismatch_is_rejected() {
        let name_cursor_by_created_at = ListFlightsCmd {
            order: types::SequenceOrder::CreatedAt,
            cursor: Some(name_cursor()),
            ..Default::default()
        };
        let created_at_cursor_by_name = ListFlightsCmd {
            order: types::SequenceOrder::Name,
            cursor: Some(created_at_cursor()),
            ..Default::default()
        };

        for cmd in [name_cursor_by_created_at, created_at_cursor_by_name] {
            let err = cmd.validate().unwrap_err();
            assert!(matches!(err.kind(), ErrorKind::BadRequest(_)));
        }
    }
}
