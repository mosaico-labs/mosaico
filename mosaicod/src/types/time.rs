use std::time::{SystemTime, UNIX_EPOCH};

/// Timestamp format used by mosaico
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect(
                "unable to retrieve system time from unix epoch, the Beatles are still together?",
            )
            .as_millis() as i64;
        Self(now)
    }

    pub fn max() -> Self {
        Self(i64::MAX)
    }

    pub fn min() -> Self {
        Self(i64::MIN)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Timestamp(value)
    }
}

impl From<Timestamp> for i64 {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

impl From<Timestamp> for DateTime {
    fn from(value: Timestamp) -> Self {
        Self(
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value.0)
                .expect("invalid timestamp"),
        )
    }
}

/// `DateTime` format used by mosaico
pub struct DateTime(chrono::DateTime<chrono::Utc>);

impl DateTime {
    pub fn now() -> Self {
        Self(chrono::Utc::now())
    }

    pub fn fmt_to_ms(&self) -> String {
        self.0.format("%Y%m%d%H%M%S%3f").to_string()
    }
}

impl std::fmt::Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
