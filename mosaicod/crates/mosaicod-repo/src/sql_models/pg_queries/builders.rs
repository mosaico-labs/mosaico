use mosaicod_query as query;

// (cabba) TODO: this code is dog shit, we need to fix it ASAP

pub struct ChunkQueryBuilder {
    placeholder_counter: usize,
}

impl ChunkQueryBuilder {
    pub fn build(
        filter: query::OntologyExprGroup<query::Value>,
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

        qb = qb.ontology_expr_group(filter, &mut qb_chunk);

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

fn build_clause(where_clauses: String, v: &query::Value) -> String {
    match v {
        query::Value::Integer(_) | query::Value::Float(_) | query::Value::Boolean(_) => {
            let select = r#"
            SELECT chunk_id FROM chunk_t 
            JOIN column_chunk_numeric_t __stats__ USING(chunk_id)
            JOIN column_t __column__ USING(column_id)
            "#;

            format!("{select} WHERE {where_clauses}")
        }
        query::Value::Text(_) => {
            let select = r#"
            SELECT chunk_id FROM chunk_t 
            JOIN column_chunk_textual_t __stats__ USING(chunk_id) 
            JOIN column_t __column__ USING(column_id)
            "#;

            format!("{select} WHERE {where_clauses}")
        }
    }
}

fn column_table_name_by_value(_v: &query::Value) -> String {
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
                let p = self.consume_placeholder();
                let column_name = column_table_name_by_value(&v);

                let clause = format!(
                    "{column_name} = {field} AND __stats__.min_value >= {p} AND __stats__.max_value <= {p}"
                );
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Neq(_) => return Err(query::Error::unsupported_op(field.into())),
            query::Op::Leq(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name_by_value(&v);

                let clause = format!("{column_name} = {field} AND __stats__.min_value <= {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Geq(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name_by_value(&v);

                let clause = format!("{column_name} = {field} AND __stats__.max_value >= {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Lt(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name_by_value(&v);

                let clause = format!("{column_name} = {field} AND __stats__.min_value < {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }
            query::Op::Gt(v) => {
                let v = v.into();
                let p = self.consume_placeholder();
                let column_name = column_table_name_by_value(&v);

                let clause = format!("{column_name} = {field} AND __stats__.max_value > {p}");
                query::CompiledClause::new(build_clause(clause, &v), vec![v])
            }

            query::Op::Ex => return Err(query::Error::unsupported_op(field.into())),
            query::Op::Nex => return Err(query::Error::unsupported_op(field.into())),

            query::Op::Between(range) => {
                let vmin = range.min.into();
                let vmax = range.max.into();
                let pmin = self.consume_placeholder();
                let pmax = self.consume_placeholder();
                let column_name = column_table_name_by_value(&vmin);

                let clause = format!(
                    "{column_name} = {field} AND __stats__.min_value <= {pmax} AND __stats__.max_value >= {pmin}"
                );

                query::CompiledClause::new(build_clause(clause, &vmin), vec![vmin, vmax])
            }

            query::Op::In(_) => return Err(query::Error::unsupported_op(field.into())),
            query::Op::Match(_) => return Err(query::Error::unsupported_op(field.into())),
        };

        Ok(clause)
    }
}

impl query::OntologyFieldFmt for ChunkQueryBuilder {
    fn ontology_column_fmt(&self, subfield: &query::OntologyField) -> String {
        format!("'{}'", subfield.value())
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

    pub fn with_starting_placeholder(mut self, placeholder: usize) -> Self {
        self.placeholder_counter = placeholder;
        self
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
