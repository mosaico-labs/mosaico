use std::borrow::Cow;
use std::collections::HashMap;

/// Placeholder indicating uninitialized minimum text statistic.
/// Empty string compares less than any non-empty string.
const TEXT_MIN_PLACEHOLDER: &str = "";
/// Placeholder indicating uninitialized maximum text statistic.
const TEXT_MAX_PLACEHOLDER: &str = "";

const NUMERIC_MIN_PLACEHOLDER: f64 = f64::MAX;
const NUMERIC_MAX_PLACEHOLDER: f64 = f64::MIN;

#[derive(Debug)]
pub struct ColumnsStats {
    pub stats: HashMap<String, Stats>,
}

impl ColumnsStats {
    pub fn empty() -> Self {
        Self {
            stats: HashMap::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Stats {
    Numeric(NumericStats),
    Text(TextStats),
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextStats {
    pub min: Cow<'static, str>,
    pub max: Cow<'static, str>,

    pub has_null: bool,
}

impl Default for TextStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TextStats {
    pub fn new() -> Self {
        Self {
            min: Cow::Borrowed(TEXT_MIN_PLACEHOLDER),
            max: Cow::Borrowed(TEXT_MAX_PLACEHOLDER),

            has_null: false,
        }
    }

    /// Evaluates a new literal value and updates the column statistics.
    /// If the provided value is [`None`], it is condered a null value.
    pub fn eval(&mut self, val: &Option<&str>) {
        if let Some(val) = val {
            let val = *val;
            if self.min.as_ref() == TEXT_MIN_PLACEHOLDER || *self.min > *val {
                self.min = Cow::Owned(val.to_owned());
            }
            if self.max.as_ref() == TEXT_MAX_PLACEHOLDER || *self.max < *val {
                self.max = Cow::Owned(val.to_owned());
            }
        } else {
            self.has_null = true;
        }
    }

    /// Consumes the stats and returns owned strings for min and max.
    pub fn into_owned(self) -> (String, String, bool) {
        (self.min.into_owned(), self.max.into_owned(), self.has_null)
    }
}
