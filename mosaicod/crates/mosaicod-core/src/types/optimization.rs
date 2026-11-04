/// What the store optimizer found wrong with a topic: written, with the details, in a notification
/// on the topic ([`Self::notification`]).
///
/// The topic keeps the data the client sent, read back as it was sent. With
/// [`Self::Failed`] the topic is optimized again on the next run; with any other
/// issue, its data is the problem, and its optimization ends there.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum OptimizationIssue {
    /// An image of an image sequence has no format, a format that is not
    /// supported (e.g. `tiff`) or not the one of the other images, or is a
    /// valid image of another supported format.
    FormatMismatch,
    /// An image of an image sequence can't be decoded: it is not a valid image
    /// of any supported format (e.g. garbage, truncated, corrupted data).
    Undecodable,
    /// The images of an image sequence are right, but can't be turned into a
    /// video without losing data (e.g. images of different sizes, a column of
    /// the ontology missing, a column named as one of the frame index's own,
    /// a layout that can't be decoded, no images). Columns besides those of the
    /// ontology are kept, in the frame index.
    Unsupported,
    /// The optimization failed, for a reason that may not last (e.g. the store,
    /// the network, the database, the video encoder): it is tried again.
    Failed,
}

impl OptimizationIssue {
    /// The name of the issue, as written in its notification.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FormatMismatch => "format_mismatch",
            Self::Undecodable => "undecodable",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }

    /// Whether the topic is optimized again on the next run.
    pub fn is_retried(&self) -> bool {
        matches!(self, Self::Failed)
    }

    /// The message of the notification of the issue, with its `details`:
    /// `store optimizer/<issue>: <details>`, the issue as in [`Self::as_str`].
    pub fn notification(&self, details: &str) -> String {
        format!("store optimizer/{self}: {details}")
    }
}

impl std::fmt::Display for OptimizationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::OptimizationIssue;

    #[test]
    fn only_failures_are_retried_and_notifications_name_the_issue() {
        for issue in [
            OptimizationIssue::FormatMismatch,
            OptimizationIssue::Undecodable,
            OptimizationIssue::Unsupported,
            OptimizationIssue::Failed,
        ] {
            assert_eq!(issue.is_retried(), issue == OptimizationIssue::Failed);
        }
        assert_eq!(
            OptimizationIssue::Undecodable.notification("image recorded at 10 ns: garbage"),
            "store optimizer/undecodable: image recorded at 10 ns: garbage"
        );
    }
}
