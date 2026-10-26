use std::collections::HashMap;

use mosaicod_core::types;
use mosaicod_query as query;

use super::builders::column_table_name;
use super::stats_kind::StatsKind;

/// Builds the query returning the ids of the topics whose aggregated values satisfy
/// every predicate in filter.
pub struct TopicAggregateQueryBuilder;

impl TopicAggregateQueryBuilder {
    pub fn build(
        filter: query::OntologyPredicateGroup<query::Value>,
        on_topic_ids: Vec<i64>,
        field_kinds: &HashMap<String, StatsKind>,
    ) -> Result<(String, Vec<query::Value>), query::Error> {
        let placeholder = query::Placeholder::new();
        let mut values = Vec::new();

        // Check if some topic ids from topic filter
        let topic_clause = if on_topic_ids.is_empty() {
            String::new()
        } else {
            let p = placeholder.clone().consume();
            values.push(query::Value::IntegerArray(on_topic_ids));
            format!("AND chunk_t.topic_id = ANY(${p})")
        };

        let mut having_compiler = super::SqlQueryCompiler::new(placeholder);
        let mut clauses = Vec::new();

        for predicate in filter.into_iter() {
            let (field, op, aggr) = predicate.into_parts();
            let field = field.field_str();
            let aggr_err = |err| query::Error::AggregatorError {
                field: field.clone(),
                err,
            };

            // Only aggregated predicates are allowed here
            let aggr = aggr.ok_or_else(|| aggr_err(query::AggregatorError::MissingAggregator))?;

            // The aggregators taking a numeric operand whatever the field type (e.g. COUNT on a
            // text field) read the stats of the field, the others infer it from the operand.
            // The aggregator is checked because the field can be in `field_kinds` for another
            // predicate on the same field (e.g. a COUNT and a MAX on the same field)
            let kind = match field_kinds.get(&field) {
                Some(kind) if aggr.requires_numeric_operand() => *kind,
                _ => StatsKind::try_from_op(&op).map_err(aggr_err)?,
            };

            let source = format!(
                "chunk_t
                JOIN {table} __stats__ USING(chunk_id)
                JOIN column_t __column__ USING(column_id)",
                table = kind.table(),
            );
            let filter = format!(
                "{column} = '{field}' {topic_clause}",
                column = column_table_name()
            );

            let joins = aggregate_joins(&aggr, &source, &filter);

            let aggregate = aggregate_expr(aggr, kind)
                .ok_or_else(|| aggr_err(query::AggregatorError::UnsupportedAggregator))?;

            let (having, mut having_values) =
                query::CompileClause::compile_clause(&mut having_compiler, &aggregate, op)?
                    .into_parts();
            values.append(&mut having_values);

            clauses.push(format!(
                "SELECT chunk_t.topic_id FROM {source} {joins}
                WHERE {filter}
                GROUP BY chunk_t.topic_id
                HAVING {having}"
            ));
        }

        Ok((clauses.join(" INTERSECT "), values))
    }
}

/// Max of the chunk, NULL when the chunk has no valid values
fn max_value_expr(kind: StatsKind) -> String {
    match kind {
        StatsKind::Numeric => format!(
            "NULLIF(__stats__.max_value, {:e}::float8)",
            types::NUMERIC_MAX_PLACEHOLDER
        ),
        StatsKind::Textual => "NULLIF(__stats__.max_value, '')".to_owned(),
    }
}

/// Min of the chunk, NULL when the chunk has no valid values
fn min_value_expr(kind: StatsKind) -> String {
    match kind {
        StatsKind::Numeric => format!(
            "NULLIF(__stats__.min_value, {:e}::float8)",
            types::NUMERIC_MIN_PLACEHOLDER
        ),
        StatsKind::Textual => "NULLIF(__stats__.min_value, '')".to_owned(),
    }
}

/// Number of valid samples of a chunk, null (and nan for numeric) excluded
fn valid_count_expr(kind: StatsKind) -> &'static str {
    match kind {
        StatsKind::Numeric => "(chunk_t.row_count - __stats__.null_count - __stats__.nan_count)",
        StatsKind::Textual => "(chunk_t.row_count - __stats__.null_count)",
    }
}

/// Number of samples of a chunk, null and nan included, the same for every column
const TOTAL_COUNT: &str = "chunk_t.row_count";

/// Mean of the valid values of the topic, weighting the chunk means by their
/// valid values: `μ = Σ nᵢ·meanᵢ / N`.
fn mean_aggr_expr() -> String {
    let n = valid_count_expr(StatsKind::Numeric);
    format!("(SUM({n} * __stats__.mean_value) / NULLIF(SUM({n}), 0))")
}

/// Sample variance (n - 1) of the valid values of the topic, merging the chunk
/// mean and M2 (Chan) around the topic mean `μ` joined as `__topic__`:
/// `M2 = Σ M2ᵢ + Σ nᵢ·(meanᵢ - μ)²`. Every term is non-negative, so no precision
/// is lost subtracting large sums.
/// The result is NULL with less than two valid values.
fn variance_aggr_expr() -> String {
    let n = valid_count_expr(StatsKind::Numeric);
    // The chunks without valid values have NULL mean and M2 and are skipped by SUM,
    // the denominator is NULL also when `N - 1` is negative
    format!(
        "(SUM(__stats__.m2_value
              + {n} * POWER(__stats__.mean_value - __topic__.mean_value, 2))
          / NULLIF(GREATEST(SUM({n}) - 1, 0), 0))"
    )
}

/// JOINs needed by VAR and STD: they merge the chunk M2 around the topic mean,
/// joined as `__topic__` and computed on the same chunks.
fn aggregate_joins(aggr: &query::Aggregator, source: &str, filter: &str) -> String {
    match aggr {
        query::Aggregator::Var | query::Aggregator::Std => format!(
            "JOIN (SELECT chunk_t.topic_id, {mean} AS mean_value
                   FROM {source} WHERE {filter}
                   GROUP BY chunk_t.topic_id) __topic__ USING(topic_id)",
            mean = mean_aggr_expr(),
        ),
        _ => String::new(),
    }
}

/// Samples per second of the topic, null and nan included: `(count - 1) / duration`,
/// with the duration taken from the first and last timestamps of its chunks.
/// The result is NULL with less than two samples or a zero duration.
fn rate_aggr_expr() -> String {
    let n = TOTAL_COUNT;
    format!(
        "(CASE WHEN SUM({n}) > 1 THEN
             (SUM({n}) - 1)::float8 / NULLIF(
                 (MAX(chunk_t.last_sample_timestamp_ns)
                  - MIN(chunk_t.first_sample_timestamp_ns))::float8 / 1e9,
                 0
             )
         END)"
    )
}

/// SQL expression computing `aggr` over the chunk stats of a topic,
/// `None` if `aggr` is not supported on `kind`.
fn aggregate_expr(aggr: query::Aggregator, kind: StatsKind) -> Option<String> {
    match (aggr, kind) {
        (query::Aggregator::Max, _) => Some(format!("MAX({})", max_value_expr(kind))),
        (query::Aggregator::Min, _) => Some(format!("MIN({})", min_value_expr(kind))),
        (query::Aggregator::Range, StatsKind::Numeric) => Some(format!(
            "(MAX({}) - MIN({}))",
            max_value_expr(kind),
            min_value_expr(kind)
        )),
        (query::Aggregator::CountSample, _) => {
            Some(format!("SUM({})::BIGINT", valid_count_expr(kind)))
        }
        (query::Aggregator::Count, _) => Some(format!("SUM({TOTAL_COUNT})::BIGINT")),
        (query::Aggregator::Mean, StatsKind::Numeric) => Some(mean_aggr_expr()),
        (query::Aggregator::Var, StatsKind::Numeric) => Some(variance_aggr_expr()),
        (query::Aggregator::Std, StatsKind::Numeric) => {
            Some(format!("SQRT({})", variance_aggr_expr()))
        }
        // The chunk id makes the order deterministic between chunks with the same timestamp
        (query::Aggregator::First, _) => Some(
            "(ARRAY_AGG(__stats__.first_sample_value \
             ORDER BY chunk_t.first_sample_timestamp_ns, chunk_t.chunk_id))[1]"
                .to_owned(),
        ),
        (query::Aggregator::Last, _) => Some(
            "(ARRAY_AGG(__stats__.last_sample_value \
             ORDER BY chunk_t.last_sample_timestamp_ns DESC, chunk_t.chunk_id DESC))[1]"
                .to_owned(),
        ),
        (query::Aggregator::Rate, _) => Some(rate_aggr_expr()),
        _ => None,
    }
}
