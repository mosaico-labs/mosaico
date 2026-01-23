use std::collections::HashMap;

const NUMERIC_MIN_PLACEHOLDER: f64 = f64::MAX;
const NUMERIC_MAX_PLACEHOLDER: f64 = f64::MIN;

/// Store [`Stats`] for each field of a given ontology model
#[derive(Debug)]
pub struct OntologyModelStats {
    pub cols: HashMap<String, Stats>,
}

impl OntologyModelStats {
    pub fn empty() -> Self {
        Self {
            cols: HashMap::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Stats {
    Numeric(NumericStats),
    Textual(TextualStats),
    Unsupported,
}

impl Stats {
    pub fn is_unsupported(&self) -> bool {
        if let Stats::Unsupported = self {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumericStats {
    pub min: f64,
    pub max: f64,

    pub has_null: bool,
    pub has_nan: bool,
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

            has_null: false,
            has_nan: false,
        }
    }

    /// Evaluates a new numeric value and updates the column statistics.
    /// If the provided value is [`None`], it is condered a null value.
    pub fn eval(&mut self, val: &Option<f64>) {
        if let Some(val) = val {
            let val = *val;
            if val.is_nan() {
                self.has_nan = true;
            } else {
                if self.min > val {
                    self.min = val;
                }
                if self.max < val {
                    self.max = val;
                }
            }
        } else {
            self.has_null = true;
        }
    }

    /// Merges pre-computed statistics from an Arrow array.
    /// This is more efficient than calling `eval()` for each element.
    pub fn merge(&mut self, min: Option<f64>, max: Option<f64>, has_null: bool, has_nan: bool) {
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
        self.has_null |= has_null;
        self.has_nan |= has_nan;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextualStats {
    pub min: Option<String>,
    pub max: Option<String>,

    pub has_null: bool,
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

            has_null: false,
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
            self.has_null = true;
        }
    }

    /// Consumes the stats and returns owned strings for min and max.
    pub fn into_owned(self) -> (String, String, bool) {
        (
            self.min.unwrap_or_default(),
            self.max.unwrap_or_default(),
            self.has_null,
        )
    }

    /// Merges pre-computed statistics from an Arrow array.
    /// This is more efficient than calling `eval()` for each element.
    pub fn merge(&mut self, min: Option<&str>, max: Option<&str>, has_null: bool) {
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
        self.has_null |= has_null;
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
        assert!(!stats.has_null);
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
        assert!(stats.has_null);
    }

    #[test]
    fn text_stats_merge_with_empty_string() {
        let mut stats = TextualStats::new();
        stats.merge(Some("a"), Some("z"), false);
        stats.merge(Some(""), Some("b"), false);

        assert_eq!(stats.min.as_deref(), Some(""));
        assert_eq!(stats.max.as_deref(), Some("z"));
    }
}
