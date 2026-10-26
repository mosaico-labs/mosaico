use crate::{OntologyField, Op, Value};

#[derive(Debug, thiserror::Error)]
pub enum AggregatorError {
    /// Unsupported aggregator
    #[error("unsupported aggregator")]
    UnsupportedAggregator,

    /// Unsupported operation
    #[error("unsupported operation for the given aggregator")]
    UnsupportedOperation,

    /// The aggregator can not be applied to a list field
    #[error("unsupported aggregator on list fields")]
    UnsupportedListField,

    /// The aggregator can not be computed on the field value type
    #[error("unsupported value type for the given aggregator")]
    UnsupportedValueType,

    /// The predicate has no aggregator
    #[error("missing aggregator")]
    MissingAggregator,

    /// The predicate has an aggregator where only plain predicates are allowed
    #[error("unexpected aggregator")]
    UnexpectedAggregator,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregator {
    Max,
    Min,
    CountSample,
    Range,
    First,
    Last,
    Std,
    Mean,
    Var,
    Rate,
    P50,
    P90,
    P99,
    Count,
}

impl Aggregator {
    /// Returns true if the operand of the aggregator must be numeric whatever the field
    /// type is (e.g. the count or the mean), false if it can be either numeric or text,
    /// following the field type (e.g. the maximum).
    pub fn requires_numeric_operand(&self) -> bool {
        matches!(
            self,
            Self::CountSample
                | Self::Count
                | Self::Range
                | Self::Std
                | Self::Mean
                | Self::Var
                | Self::Rate
                | Self::P50
                | Self::P90
                | Self::P99
        )
    }

    /// Returns true if the aggregator can be computed on a list field, whose stats
    /// are computed on the elements of the lists (e.g. the maximum element), while
    /// the count and the rate are computed on the rows
    pub fn supports_list_field(&self) -> bool {
        matches!(
            self,
            Self::Max | Self::Min | Self::Range | Self::Count | Self::Rate
        )
    }

    pub fn is_supported_aggregator<T>(&self, op: &Op<T>) -> bool {
        match self {
            Self::Max | Self::Min | Self::First | Self::Last => matches!(
                op,
                Op::Eq(_)
                    | Op::Neq(_)
                    | Op::Lt(_)
                    | Op::Leq(_)
                    | Op::Gt(_)
                    | Op::Geq(_)
                    | Op::In(_)
                    | Op::Between(_)
                    | Op::Outside(_)
                    | Op::Match(_)
            ),
            Self::Range | Self::Std | Self::Var => matches!(
                op,
                Op::Eq(_)
                    | Op::Neq(_)
                    | Op::Lt(_)
                    | Op::Leq(_)
                    | Op::Gt(_)
                    | Op::Geq(_)
                    | Op::In(_)
                    | Op::Between(_)
                    | Op::Outside(_)
            ),
            Self::CountSample
            | Self::Count
            | Self::Mean
            | Self::Rate
            | Self::P50
            | Self::P90
            | Self::P99 => matches!(
                op,
                Op::Eq(_)
                    | Op::Neq(_)
                    | Op::Lt(_)
                    | Op::Leq(_)
                    | Op::Gt(_)
                    | Op::Geq(_)
                    | Op::In(_)
                    | Op::Between(_)
                    | Op::Outside(_)
            ),
        }
    }

    /// Returns true if the aggregator can be computed on fields holding values
    /// of the same type of the `op` operands
    pub fn is_supported_value(&self, op: &Op<Value>) -> bool {
        match op.operand() {
            // An aggregate is a single value, it can't be compared with an array.
            // Never hit by `Between`, `Outside` and `In`: the value checked is a range bound
            // (`Outside` and `Between`) or scalars (values of `In` are split into single scalars
            // by `array_elements`)
            Some(
                Value::IntegerArray(_)
                | Value::FloatArray(_)
                | Value::TextArray(_)
                | Value::BooleanArray(_),
            ) => false,
            // Only the aggregators selecting one of the samples are defined on text
            Some(Value::Text(_)) => {
                matches!(self, Self::Max | Self::Min | Self::First | Self::Last)
            }
            Some(Value::Boolean(_)) => false,
            Some(Value::Integer(_) | Value::Float(_)) => true,
            // No operand to check the type against (`Ex`, `Nex`)
            None => false,
        }
    }

    /// Checks that the aggregator can be applied to `field` with `op`
    pub fn validate(&self, field: &OntologyField, op: &Op<Value>) -> Result<(), AggregatorError> {
        if !self.is_supported_aggregator(op) {
            return Err(AggregatorError::UnsupportedOperation);
        }
        if !self.is_supported_value(op) {
            return Err(AggregatorError::UnsupportedValueType);
        }
        if field.field_path().specifier().is_some() {
            return Err(AggregatorError::UnsupportedListField);
        }
        Ok(())
    }
}
