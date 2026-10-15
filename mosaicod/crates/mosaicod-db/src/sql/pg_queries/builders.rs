use mosaicod_core::types;
use mosaicod_query as query;

// (cabba) TODO: this code is dog shit, we need to fix it ASAP

/// Builds the query returning the ids of the topics whose aggregated values satisfy
/// every predicate in filter.
pub struct TopicAggregateQueryBuilder;

impl TopicAggregateQueryBuilder {
    pub fn build(
        filter: query::OntologyPredicateGroup<query::Value>,
        on_topic_ids: Vec<i64>,
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
            let kind =
                StatsKind::try_from_op(&op).map_err(|err| query::Error::AggregatorError {
                    field: field.field_str(),
                    err,
                })?;

            let aggregate = aggr
                .and_then(|aggr| aggregate_expr(aggr, kind))
                .ok_or_else(|| query::Error::AggregatorError {
                    field: field.field_str(),
                    err: query::AggregatorError::UnsupportedAggregator,
                })?;

            let (having, mut having_values) =
                query::CompileClause::compile_clause(&mut having_compiler, &aggregate, op)?
                    .into_parts();
            values.append(&mut having_values);

            clauses.push(format!(
                "SELECT chunk_t.topic_id FROM chunk_t
                JOIN {table} __stats__ USING(chunk_id)
                JOIN column_t __column__ USING(column_id)
                WHERE {column} = '{field}' {topic_clause}
                GROUP BY chunk_t.topic_id
                HAVING {having}",
                table = kind.table(),
                column = column_table_name(),
                field = field.field_str(),
            ));
        }

        Ok((clauses.join(" INTERSECT "), values))
    }
}

#[derive(Clone, Copy, PartialEq)]
enum StatsKind {
    Numeric,
    Textual,
}

impl StatsKind {
    fn from_value(v: &query::Value) -> Self {
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

    fn try_from_op(op: &query::Op<query::Value>) -> Result<Self, query::AggregatorError> {
        let value = match op {
            query::Op::Eq(v)
            | query::Op::Neq(v)
            | query::Op::Leq(v)
            | query::Op::Geq(v)
            | query::Op::Lt(v)
            | query::Op::Gt(v)
            | query::Op::Match(v) => Some(v),
            query::Op::Between(r) | query::Op::Outside(r) => Some(&r.min),
            query::Op::In(items) => items.first(),
            query::Op::Ex | query::Op::Nex => None,
        };

        value
            .map(Self::from_value)
            .ok_or(query::AggregatorError::UnsupportedOperation)
    }

    fn table(self) -> &'static str {
        match self {
            Self::Numeric => "column_chunk_numeric_t",
            Self::Textual => "column_chunk_textual_t",
        }
    }

    fn max_value(self) -> String {
        match self {
            Self::Numeric => format!(
                "NULLIF(__stats__.max_value, {:e}::float8)",
                types::NUMERIC_MAX_PLACEHOLDER
            ),
            Self::Textual => "NULLIF(__stats__.max_value, '')".to_owned(),
        }
    }

    fn min_value(self) -> String {
        match self {
            Self::Numeric => format!(
                "NULLIF(__stats__.min_value, {:e}::float8)",
                types::NUMERIC_MIN_PLACEHOLDER
            ),
            Self::Textual => "NULLIF(__stats__.min_value, '')".to_owned(),
        }
    }
}

/// SQL expression computing `aggr` over the chunk stats of a topic,
/// `None` if `aggr` is not supported on `kind`.
fn aggregate_expr(aggr: query::Aggregator, kind: StatsKind) -> Option<String> {
    match (aggr, kind) {
        (query::Aggregator::Max, _) => Some(format!("MAX({})", kind.max_value())),
        (query::Aggregator::Min, _) => Some(format!("MIN({})", kind.min_value())),
        (query::Aggregator::Range, StatsKind::Numeric) => Some(format!(
            "(MAX({}) - MIN({}))",
            kind.max_value(),
            kind.min_value()
        )),
        _ => None,
    }
}

pub struct ChunkQueryBuilder {
    placeholder_counter: usize,
}

impl ChunkQueryBuilder {
    pub fn build(
        filter: query::OntologyPredicateGroup<query::Value>,
        on_topic_ids: Vec<i64>,
    ) -> Result<(String, Vec<query::Value>), query::Error> {
        let mut qb = query::ClausesCompiler::new();

        let mut pidx = 1;

        if !on_topic_ids.is_empty() {
            let mut by_topic_mapper = FilterChunksByTopicMapper::new();

            qb = qb.expr(
                "", // this element will not be used
                query::Op::In(on_topic_ids),
                &mut by_topic_mapper,
            );

            pidx = by_topic_mapper.placeholder_counter;
        }

        let mut qb_chunk = Self {
            placeholder_counter: pidx,
        };

        qb = qb.ontology_predicate_group(filter, &mut qb_chunk);

        let qr = qb.compile()?;
        let joined_clauses = qr.clauses.join(" INTERSECT ");

        let query = build_query(joined_clauses);

        Ok((query, qr.values))
    }

    fn consume_placeholder(&mut self) -> String {
        let p = format!("${}", self.placeholder_counter);
        self.placeholder_counter += 1;
        p
    }
}

pub fn build_query(joined_clauses: String) -> String {
    format!(
        "WITH __selected_chunks__ AS({joined_clauses}) SELECT chunk_t.* FROM chunk_t JOIN __selected_chunks__ USING (chunk_id)"
    )
}

fn build_clause_union(where_clauses: &str) -> String {
    let numeric = format!(
        "SELECT chunk_id FROM chunk_t
        JOIN column_chunk_numeric_t __stats__ USING(chunk_id)
        JOIN column_t __column__ USING(column_id)
        WHERE {where_clauses}"
    );
    let textual = format!(
        "SELECT chunk_id FROM chunk_t
        JOIN column_chunk_textual_t __stats__ USING(chunk_id)
        JOIN column_t __column__ USING(column_id)
        WHERE {where_clauses}"
    );
    format!("({numeric}) UNION ({textual})")
}

fn build_clause(where_clauses: String, v: &query::Value) -> String {
    format!(
        "SELECT chunk_id FROM chunk_t
        JOIN {} __stats__ USING(chunk_id)
        JOIN column_t __column__ USING(column_id)
        WHERE {where_clauses}",
        StatsKind::from_value(v).table()
    )
}

fn column_table_name() -> String {
    "(__column__.ontology_tag || '.' || __column__.column_name)".into()
}

impl query::CompileClause for ChunkQueryBuilder {
    fn compile_clause<V>(
        &mut self,
        field: &str,
        op: query::Op<V>,
    ) -> Result<query::CompiledClause, query::Error>
    where
        V: Into<query::Value> + query::IsSupportedOp,
    {
        let clause = match op {
            query::Op::Eq(v) => {
                let v = v.into();
                let column_name = column_table_name();
                match &v {
                    // Array equality cannot be pruned with scalar min/max stats; just
                    // check that the column exists in the chunk and let DataFusion filter.
                    query::Value::IntegerArray(_)
                    | query::Value::FloatArray(_)
                    | query::Value::TextArray(_)
                    | query::Value::BooleanArray(_) => {
                        let clause = format!("{column_name} = {field}");
                        query::CompiledClause::new(build_clause(clause, &v), vec![])
                    }
                    _ => {
                        let p = self.consume_placeholder();
                        let clause = format!(
                            "{column_name} = {field} AND __stats__.min_value <= {p} AND __stats__.max_value >= {p}"
                        );
                        query::CompiledClause::new(build_clause(clause, &v), vec![v])
                    }
                }
            }
            query::Op::Neq(v) => {
                let v = v.into();
                let column_name = column_table_name();
                match &v {
                    query::Value::IntegerArray(_)
                    | query::Value::FloatArray(_)
                    | query::Value::TextArray(_)
                    | query::Value::BooleanArray(_) => {
                        let clause = format!("{column_name} = {field}");
                        query::CompiledClause::new(build_clause(clause, &v), vec![])
                    }
                    _ => {
                        let p = self.consume_placeholder();
                        let clause = format!(
                            "{column_name} = {field} AND (__stats__.min_value > {p} OR __stats__.max_value < {p})"
                        );
                        query::CompiledClause::new(build_clause(clause, &v), vec![v])
                    }
                }
            }
            query::Op::Leq(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!("{column_name} = {field} AND __stats__.min_value <= {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Geq(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!("{column_name} = {field} AND __stats__.max_value >= {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Lt(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!("{column_name} = {field} AND __stats__.min_value < {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Gt(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!("{column_name} = {field} AND __stats__.max_value > {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }

            query::Op::Ex => {
                let col = column_table_name();
                let clause = format!("{col} = {field}");
                query::CompiledClause::new(build_clause_union(&clause), vec![])
            }
            query::Op::Nex => {
                let col = column_table_name();
                let clause = format!("{col} = {field} AND __stats__.has_null = TRUE");
                query::CompiledClause::new(build_clause_union(&clause), vec![])
            }

            query::Op::Between(range) => {
                let vmin = range.min.into();
                let vmax = range.max.into();
                let pmin = self.consume_placeholder();
                let pmax = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!(
                    "{column_name} = {field} AND __stats__.min_value <= {pmax} AND __stats__.max_value >= {pmin}"
                );

                query::CompiledClause::new(build_clause(clause, &vmin), vec![vmin, vmax])
            }

            query::Op::Outside(range) => {
                let vmin = range.min.into();
                let vmax = range.max.into();
                let pmin = self.consume_placeholder();
                let pmax = self.consume_placeholder();
                let column_name = column_table_name();

                let clause = format!(
                    "{column_name} = {field} AND (__stats__.min_value < {pmin} OR __stats__.max_value > {pmax})"
                );

                query::CompiledClause::new(build_clause(clause, &vmin), vec![vmin, vmax])
            }

            query::Op::In(items) => {
                let values: Vec<query::Value> = items.into_iter().map(Into::into).collect();

                if values.is_empty() {
                    return Err(query::Error::empty_in(field.to_owned()));
                }

                // Check if all the values inside array are of same type
                let first = std::mem::discriminant(&values[0]);
                if values.iter().any(|v| std::mem::discriminant(v) != first) {
                    return Err(query::Error::unsupported_op(field.into()));
                }

                let p = self.consume_placeholder();
                let column_name = column_table_name();

                let (cast, array_value) = match &values[0] {
                    query::Value::Integer(_) => {
                        let arr: Vec<i64> = values
                            .iter()
                            .map(|v| match v {
                                query::Value::Integer(i) => *i,
                                _ => unreachable!(),
                            })
                            .collect();
                        // float8 because min and max are saved as float8
                        ("float8", query::Value::IntegerArray(arr))
                    }
                    query::Value::Float(_) => {
                        let arr: Vec<f64> = values
                            .iter()
                            .map(|v| match v {
                                query::Value::Float(f) => *f,
                                _ => unreachable!(),
                            })
                            .collect();
                        ("float8", query::Value::FloatArray(arr))
                    }
                    query::Value::Text(_) => {
                        let arr: Vec<String> = values
                            .iter()
                            .map(|v| match v {
                                query::Value::Text(t) => t.clone(),
                                _ => unreachable!(),
                            })
                            .collect();
                        ("text", query::Value::TextArray(arr))
                    }
                    query::Value::Boolean(_) => {
                        let arr: Vec<f64> = values
                            .iter()
                            .map(|v| match v {
                                query::Value::Boolean(b) => {
                                    if *b {
                                        1.0
                                    } else {
                                        0.0
                                    }
                                }
                                _ => unreachable!(),
                            })
                            .collect();
                        ("float8", query::Value::FloatArray(arr))
                    }
                    _ => return Err(query::Error::unsupported_op(field.into())),
                };

                let clause = format!(
                    "{column_name} = {field} AND EXISTS (
                        SELECT 1 FROM UNNEST({p}::{cast}[]) AS v
                        WHERE __stats__.min_value <= v AND __stats__.max_value >= v
                    )"
                );

                query::CompiledClause::new(build_clause(clause, &values[0]), vec![array_value])
            }
            query::Op::Match(v) => {
                let v = v.into();

                if let query::Value::Text(text) = &v
                    && text.is_empty()
                {
                    return Err(query::Error::empty_pattern(field.to_owned()));
                }

                let column_name = column_table_name();
                // Regex cannot be pruned using [min_value, max_value] stats: even if the pattern
                // falls lexicographically within the range, the chunk may not contain any matching
                // value (e.g. range ["apple", "zebra"] tells us nothing about whether "truck.*"
                // matches any row). All chunks that have the column are kept; DataFusion filters
                // row-by-row at query execution time.
                let clause = format!("{column_name} = {field}");
                query::CompiledClause::new(build_clause(clause, &v), vec![])
            }
        };

        Ok(clause)
    }
}

impl query::OntologyFieldFmt for ChunkQueryBuilder {
    fn ontology_column_fmt(&self, subfield: &query::OntologyField) -> String {
        format!("'{}'", subfield.field_str())
    }
}

/// Used to append restrict the query to only a set of topic
struct FilterChunksByTopicMapper {
    placeholder_counter: usize,
}

impl FilterChunksByTopicMapper {
    pub fn new() -> Self {
        Self {
            placeholder_counter: 1,
        }
    }

    fn consume_placeholder(&mut self) -> String {
        let p = format!("${}", self.placeholder_counter);
        self.placeholder_counter += 1;
        p
    }
}

impl query::CompileClause for FilterChunksByTopicMapper {
    fn compile_clause<V>(
        &mut self,
        _field: &str,
        op: query::Op<V>,
    ) -> Result<query::CompiledClause, query::Error>
    where
        V: Into<query::Value> + query::IsSupportedOp,
    {
        let clause = match op {
            query::Op::In(items) => {
                if items.is_empty() {
                    return Ok(query::CompiledClause::empty());
                }

                // Generate placeholders and collect values
                let values: Vec<query::Value> = items.into_iter().map(Into::into).collect();
                let placeholders: Vec<String> =
                    values.iter().map(|_| self.consume_placeholder()).collect();

                let clause = format!(
                    "SELECT chunk_id FROM chunk_t WHERE chunk_t.topic_id IN ({})",
                    placeholders.join(", ")
                );

                query::CompiledClause::new(clause, values)
            }
            _ => {
                return Err(query::Error::unsupported_op(
                    "only topic filter with in clause supported".into(),
                ));
            }
        };

        Ok(clause)
    }
}
