use std::collections::BTreeMap;

pub const NUMERIC_MIN_PLACEHOLDER: f64 = f64::MAX;
pub const NUMERIC_MAX_PLACEHOLDER: f64 = f64::MIN;

/// Store [`Stats`] for each field of a given ontology model
#[derive(Debug)]
pub struct OntologyModelStats {
    pub cols: BTreeMap<String, Stats>,
}

impl OntologyModelStats {
    pub fn empty() -> Self {
        Self {
            cols: BTreeMap::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Stats {
    Numeric(NumericStats),
    Textual(TextualStats),
    Unsupported,
    ListNumeric(NumericStats),
    ListTextual(TextualStats),
}

impl Stats {
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Stats::Unsupported)
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Stats::ListNumeric(_) | Stats::ListTextual(_))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumericStats {
    pub min: f64,
    pub max: f64,

    // Counts are `i64` to map the `BIGINT` columns of Postgres
    pub null_count: i64,
    pub nan_count: i64,

    pub first_sample_value: Option<f64>,
    pub last_sample_value: Option<f64>,

    pub valid_sample_count: i64,
    pub mean_value: Option<f64>,
    pub m2_value: Option<f64>,
}

impl Default for NumericStats {
    fn default() -> Self {
        Self::new()
    }
}

impl NumericStats {
    pub fn new() -> Self {
        Self {
            min: NUMERIC_MIN_PLACEHOLDER,
            max: NUMERIC_MAX_PLACEHOLDER,

            null_count: 0,
            nan_count: 0,

            first_sample_value: None,
            last_sample_value: None,

            valid_sample_count: 0,
            mean_value: None,
            m2_value: None,
        }
    }

    /// Merges pre-computed statistics from an Arrow array.
    /// This is more efficient than calling `eval()` for each element.
    pub fn merge(&mut self, min: Option<f64>, max: Option<f64>, null_count: i64, nan_count: i64) {
        if let Some(min_val) = min
            && self.min > min_val
        {
            self.min = min_val;
        }
        if let Some(max_val) = max
            && self.max < max_val
        {
            self.max = max_val;
        }
        self.null_count += null_count;
        self.nan_count += nan_count;
    }

    /// Merges the moments (`count`, `mean`, `m2`) of another set of valid samples
    /// into these stats, using Chan's parallel algorithm.
    ///
    /// `m2` is the sum of squared deviations from the mean, `sum((x - mean)^2)`.
    pub fn merge_moments(&mut self, count: i64, mean: f64, m2: f64) {
        if count == 0 {
            return;
        }

        let n = self.valid_sample_count + count;
        let (mean, m2) = match (self.mean_value, self.m2_value) {
            // First valid samples, nothing to merge with
            _ if self.valid_sample_count == 0 => (mean, m2),
            (Some(mean_a), Some(m2_a)) => {
                let delta = mean - mean_a;

                let weight = count as f64 / n as f64;
                (
                    mean_a + delta * weight,
                    m2_a + m2 + delta * delta * self.valid_sample_count as f64 * weight,
                )
            }
            // Moments already discarded as not finite
            _ => (f64::NAN, f64::NAN),
        };

        (self.mean_value, self.m2_value) = if mean.is_finite() && m2.is_finite() {
            (Some(mean), Some(m2))
        } else {
            (None, None)
        };
        self.valid_sample_count = n;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextualStats {
    pub min: Option<String>,
    pub max: Option<String>,

    // `i64` to map the `BIGINT` column of Postgres
    pub null_count: i64,

    pub first_sample_value: Option<String>,
    pub last_sample_value: Option<String>,
}

impl Default for TextualStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TextualStats {
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,

            null_count: 0,

            first_sample_value: None,
            last_sample_value: None,
        }
    }

    /// Evaluates a new text value and updates the column statistics.
    /// If the provided value is [`None`], it is condered a null value.
    pub fn eval(&mut self, val: &Option<&str>) {
        if let Some(val) = val {
            let val = *val;
            match &self.min {
                Some(current_min) if current_min.as_str() <= val => {}
                _ => self.min = Some(val.to_owned()),
            }

            match &self.max {
                Some(current_max) if current_max.as_str() >= val => {}
                _ => self.max = Some(val.to_owned()),
            }
        } else {
            self.null_count += 1;
        }
    }

    /// Merges pre-computed statistics from an Arrow array.
    /// This is more efficient than calling `eval()` for each element.
    pub fn merge(&mut self, min: Option<&str>, max: Option<&str>, null_count: i64) {
        if let Some(min_val) = min {
            match &self.min {
                Some(current_min) if current_min.as_str() <= min_val => {}
                _ => self.min = Some(min_val.to_owned()),
            }
        }
        if let Some(max_val) = max {
            match &self.max {
                Some(current_max) if current_max.as_str() >= max_val => {}
                _ => self.max = Some(max_val.to_owned()),
            }
        }
        self.null_count += null_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_stats_empty_string_is_valid_min() {
        let mut stats = TextualStats::new();
        stats.eval(&Some(""));
        stats.eval(&Some("a"));
        stats.eval(&Some("b"));

        assert_eq!(stats.min.as_deref(), Some(""));
        assert_eq!(stats.max.as_deref(), Some("b"));
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn text_stats_normal_values() {
        let mut stats = TextualStats::new();
        stats.eval(&Some("b"));
        stats.eval(&Some("a"));
        stats.eval(&Some("c"));

        assert_eq!(stats.min.as_deref(), Some("a"));
        assert_eq!(stats.max.as_deref(), Some("c"));
    }

    #[test]
    fn text_stats_only_nulls() {
        let mut stats = TextualStats::new();
        stats.eval(&None);
        stats.eval(&None);

        assert_eq!(stats.min, None);
        assert_eq!(stats.max, None);
        assert_eq!(stats.null_count, 2);
    }

    #[test]
    fn text_stats_merge_with_empty_string() {
        let mut stats = TextualStats::new();
        stats.merge(Some("a"), Some("z"), 0);
        stats.merge(Some(""), Some("b"), 0);

        assert_eq!(stats.min.as_deref(), Some(""));
        assert_eq!(stats.max.as_deref(), Some("z"));
    }
}
