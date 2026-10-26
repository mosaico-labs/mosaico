use mosaicod_query as query;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum StatsKind {
    Numeric,
    Textual,
}

impl StatsKind {
    pub(super) fn from_value(v: &query::Value) -> Self {
        match v {
            query::Value::Integer(_)
            | query::Value::Float(_)
            | query::Value::Boolean(_)
            | query::Value::IntegerArray(_)
            | query::Value::FloatArray(_)
            | query::Value::BooleanArray(_) => Self::Numeric,
            query::Value::Text(_) | query::Value::TextArray(_) => Self::Textual,
        }
    }

    pub(super) fn try_from_op(
        op: &query::Op<query::Value>,
    ) -> Result<Self, query::AggregatorError> {
        op.operand()
            .map(Self::from_value)
            .ok_or(query::AggregatorError::UnsupportedOperation)
    }

    pub(super) fn table(self) -> &'static str {
        match self {
            Self::Numeric => "column_chunk_numeric_t",
            Self::Textual => "column_chunk_textual_t",
        }
    }
}
