use std::time::{SystemTime, UNIX_EPOCH};

/// SEntinel value to represent the positive unbounded timestamp
const TIMESTAMP_UB_POS_SENTINEL: i64 = i64::MAX;
/// SEntinel value to represent the negative unbounded timestamp
const TIMESTAMP_UB_NEG_SENTINEL: i64 = i64::MIN;

/// Timestamp format used by mosaico, currently this timestamp represent nanoseconds units of time
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Returns the current system time as a nanosecond-precision UTC timestamp.
    ///
    /// # Panics
    ///
    /// This function will panic if the system clock is set to a time prior to the
    /// Unix Epoch (January 1, 1970).
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect(
                "unable to retrieve system time from unix epoch, the Beatles are still together?",
            )
            .as_nanos() as i64;
        Self(now)
    }

    pub fn is_unbounded_pos(&self) -> bool {
        self.0 == TIMESTAMP_UB_POS_SENTINEL
    }

    pub fn is_unbounded_neg(&self) -> bool {
        self.0 == TIMESTAMP_UB_NEG_SENTINEL
    }

    pub fn is_unbounded(&self) -> bool {
        self.is_unbounded_pos() || self.is_unbounded_neg()
    }

    /// Returns a positive unbounded timestamp value
    pub fn unbounded_pos() -> Self {
        Self(TIMESTAMP_UB_POS_SENTINEL)
    }

    /// Returns a negative unbounded timestamp value
    pub fn unbounded_neg() -> Self {
        Self(TIMESTAMP_UB_NEG_SENTINEL)
    }

    pub fn as_i64(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_unbounded_pos() {
            return write!(f, "+unbounded");
        } else if self.is_unbounded_neg() {
            return write!(f, "-unbounded");
        }
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
        Self(chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(
            value.0,
        ))
    }
}

/// Represents a closed interval of time where both the start and end are included.
///
/// This struct defines a range $[start, end]$. A timestamp is considered
/// contained within this range if $start \le t \le end$.
#[derive(Clone)]
pub struct TimestampRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimestampRange {
    pub fn between(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    pub fn starting_at(start: Timestamp) -> Self {
        Self {
            start,
            end: Timestamp::unbounded_pos(),
        }
    }

    pub fn ending_at(end: Timestamp) -> Self {
        Self {
            start: Timestamp::unbounded_neg(),
            end,
        }
    }

    /// Returns true is both start and end are unbounded timestamps
    pub fn is_unbounded(&self) -> bool {
        self.start.is_unbounded() && self.end.is_unbounded()
    }

    /// Check if the timestamp range if empty (i.e. start >= end)
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

impl std::fmt::Display for TimestampRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.start, self.end)
    }
}

impl std::fmt::Debug for TimestampRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bounds_check() {
        let ub_pos = Timestamp::unbounded_pos();
        let ub_neg = Timestamp::unbounded_neg();
        let ts: Timestamp = 1234567.into();

        assert!(ub_pos.is_unbounded_pos());
        assert!(ub_pos.is_unbounded());

        assert!(ub_neg.is_unbounded_neg());
        assert!(ub_neg.is_unbounded());

        assert!(!ts.is_unbounded());
        assert!(!ts.is_unbounded_pos());
        assert!(!ts.is_unbounded_neg());
    }

    #[test]
    fn timestamp_range_bounds_check() {
        let lb = 10000;
        let ub = 11000;

        let ts_lb_ub = TimestampRange::between(lb.into(), ub.into());
        assert!(!ts_lb_ub.is_unbounded());

        let ts_ub = TimestampRange::ending_at(lb.into());
        assert!(!ts_ub.is_unbounded());

        let ts_ub_2 = TimestampRange::between(Timestamp::unbounded_neg(), ub.into());
        assert!(!ts_ub_2.is_unbounded());

        let ts_lb = TimestampRange::starting_at(lb.into());
        assert!(!ts_lb.is_unbounded());

        let ts_lb_2 = TimestampRange::between(ub.into(), Timestamp::unbounded_pos());
        assert!(!ts_lb_2.is_unbounded());

        let ts_unbounded =
            TimestampRange::between(Timestamp::unbounded_neg(), Timestamp::unbounded_pos());
        assert!(ts_unbounded.is_unbounded());

        let ts_unbounded = TimestampRange::starting_at(Timestamp::unbounded_pos());
        assert!(ts_unbounded.is_unbounded());

        let ts_unbounded = TimestampRange::ending_at(Timestamp::unbounded_neg());
        assert!(ts_unbounded.is_unbounded());
    }

    #[test]
    fn timestamp_range_empty() {
        let ts_empty = TimestampRange::between(11000.into(), 10000.into());
        assert!(ts_empty.is_empty());

        let ts_empty = TimestampRange::between(11000.into(), Timestamp::unbounded_neg());
        assert!(ts_empty.is_empty());

        let ts_empty = TimestampRange::between(Timestamp::unbounded_pos(), 1000.into());
        assert!(ts_empty.is_empty());

        let ts_empty =
            TimestampRange::between(Timestamp::unbounded_pos(), Timestamp::unbounded_neg());
        assert!(ts_empty.is_empty());
    }
}
