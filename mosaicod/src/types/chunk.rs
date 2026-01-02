use std::borrow::Cow;
use std::collections::HashMap;

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
pub struct TextStats {
    pub min: Option<Cow<'static, str>>,
    pub max: Option<Cow<'static, str>>,

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
            min: None,
            max: None,

            has_null: false,
        }
    }

    /// Evaluates a new literal value and updates the column statistics.
    /// If the provided value is [`None`], it is condered a null value.
    pub fn eval(&mut self, val: &Option<&str>) {
        if let Some(val) = val {
            let val = *val;
            match &self.min {
                Some(current_min) if **current_min <= *val => {}
                _ => self.min = Some(Cow::Owned(val.to_owned())),
            }

            match &self.max {
                Some(current_max) if **current_max >= *val => {}
                _ => self.max = Some(Cow::Owned(val.to_owned())),
            }
        } else {
            self.has_null = true;
        }
    }

    /// Consumes the stats and returns owned strings for min and max.
    pub fn into_owned(self) -> (String, String, bool) {
        (
            self.min.map(|c| c.into_owned()).unwrap_or_default(),
            self.max.map(|c| c.into_owned()).unwrap_or_default(),
            self.has_null,
        )
    }

    /// Merges pre-computed statistics from an Arrow array.
    /// This is more efficient than calling `eval()` for each element.
    pub fn merge(&mut self, min: Option<&str>, max: Option<&str>, has_null: bool) {
        if let Some(min_val) = min {
            match &self.min {
                Some(current_min) if **current_min <= *min_val => {}
                _ => self.min = Some(Cow::Owned(min_val.to_owned())),
            }
        }
        if let Some(max_val) = max {
            match &self.max {
                Some(current_max) if **current_max >= *max_val => {}
                _ => self.max = Some(Cow::Owned(max_val.to_owned())),
            }
        }
        self.has_null |= has_null;
    }
}
