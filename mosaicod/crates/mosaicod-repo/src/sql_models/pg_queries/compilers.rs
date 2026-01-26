use mosaicod_query as query;

pub struct JsonQueryCompiler {
    internal: internal::JsonQueryCompiler,
}

impl JsonQueryCompiler {
    pub fn new() -> Self {
        Self {
            internal: internal::JsonQueryCompiler::new(),
        }
    }

    pub fn with_field_and_placeholder(
        &mut self,
        field: String,
        placeholder: usize,
    ) -> &mut internal::JsonQueryCompiler {
        self.internal.field(field);
        self.internal.placeholder(placeholder);
        &mut self.internal
    }
}

pub struct SqlQueryCompiler {
    placeholder_counter: usize,
}

impl SqlQueryCompiler {
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

    pub fn current_placeholder(&self) -> usize {
        self.placeholder_counter
    }
}

impl query::CompileClause for SqlQueryCompiler {
    fn compile_clause<V>(
        &mut self,
        field: &str,
        op: query::Op<V>,
    ) -> Result<query::CompiledClause, query::Error>
    where
        V: Into<query::Value> + query::IsSupportedOp,
    {
        if !op.is_supported_op() {
            return Err(query::Error::unsupported_op(field.to_owned()));
        }

        let r = match op {
            query::Op::Eq(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} = {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Neq(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} != {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Leq(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} <= {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Geq(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} >= {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Lt(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} < {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Gt(v) => {
                let v: query::Value = v.into();
                query::CompiledClause::new(
                    format!("{field} > {}", self.consume_placeholder()),
                    vec![v],
                )
            }
            query::Op::Ex => {
                query::CompiledClause::new(format!("({field}) IS NOT NULL"), Vec::new())
            }
            query::Op::Nex => query::CompiledClause::new(format!("({field}) IS NULL"), Vec::new()),
            query::Op::Between(range) => {
                let min: query::Value = range.min.into();
                let max: query::Value = range.max.into();

                let pmin = self.consume_placeholder();
                let pmax = self.consume_placeholder();

                let clause = format!("({field} >= {pmin}) AND ({field} <= {pmax})");

                query::CompiledClause::new(clause, vec![min, max])
            }
            query::Op::In(items) => {
                if items.is_empty() {
                    return Ok(query::CompiledClause::empty());
                }

                // Generate placeholders and collect values
                let values: Vec<query::Value> = items.into_iter().map(Into::into).collect();
                let placeholders: Vec<String> =
                    values.iter().map(|_| self.consume_placeholder()).collect();

                let clause = format!("{} IN ({})", field, placeholders.join(", "));

                query::CompiledClause::new(clause, values)
            }
            query::Op::Match(v) => {
                let value: query::Value = v.into();
                if let query::Value::Text(text) = value {
                    let value = query::Value::Text(format!("%{}%", text));
                    let clause = format!("{} LIKE {}", field, self.consume_placeholder());
                    query::CompiledClause::new(clause, vec![value])
                } else {
                    return Err(query::Error::unsupported_op(field.to_owned()));
                }
            }
        };

        Ok(r)
    }
}

mod internal {
    use super::*;

    pub struct JsonQueryCompiler {
        placeholder_counter: usize,
        field: String,
    }

    impl JsonQueryCompiler {
        pub fn new() -> Self {
            Self {
                placeholder_counter: 1,
                field: String::new(),
            }
        }

        pub fn field(&mut self, field: String) {
            self.field = field;
        }

        pub fn placeholder(&mut self, placeholder: usize) {
            self.placeholder_counter = placeholder;
        }

        fn consume_placeholder(&mut self) -> String {
            let p = format!("${}", self.placeholder_counter);
            self.placeholder_counter += 1;
            p
        }

        fn fmt_value(&self, field: &str, v: &query::Value) -> String {
            match v {
                query::Value::Integer(_) | query::Value::Float(_) => format!("({field})::numeric"),
                query::Value::Text(_) => field.to_owned(),
                query::Value::Boolean(_) => format!("({field})::boolean"),
            }
        }

        fn fmt_clause(&self, subfield: &str) -> String {
            let subfield = format!(
                "{{{}}}",
                subfield.split(".").collect::<Vec<&str>>().join(",")
            );
            format!("{} #>> '{subfield}'", self.field)
        }
    }

    impl query::CompileClause for JsonQueryCompiler {
        fn compile_clause<V>(
            &mut self,
            field: &str,
            op: query::Op<V>,
        ) -> Result<query::CompiledClause, query::Error>
        where
            V: Into<query::Value> + query::IsSupportedOp,
        {
            if !op.is_supported_op() {
                return Err(query::Error::unsupported_op(field.to_owned()));
            }

            let field = &self.fmt_clause(field);

            let r = match op {
                query::Op::Eq(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} = {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Neq(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} != {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Leq(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} <= {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Geq(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} >= {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Lt(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} < {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Gt(v) => {
                    let v: query::Value = v.into();
                    query::CompiledClause::new(
                        format!(
                            "{} > {}",
                            self.fmt_value(field, &v),
                            self.consume_placeholder()
                        ),
                        vec![v],
                    )
                }
                query::Op::Ex => {
                    query::CompiledClause::new(format!("({field}) IS NOT NULL"), Vec::new())
                }
                query::Op::Nex => {
                    query::CompiledClause::new(format!("({field}) IS NULL"), Vec::new())
                }
                query::Op::Between(range) => {
                    let min: query::Value = range.min.into();
                    let max: query::Value = range.max.into();

                    let pmin = self.consume_placeholder();
                    let pmax = self.consume_placeholder();

                    // Here we are passing min, but we could also pass max since they have the same type
                    // by design
                    let field = self.fmt_value(field, &min);

                    let clause = format!("({field} >= {pmin}) AND ({field} <= {pmax})");

                    query::CompiledClause::new(clause, vec![min, max])
                }
                query::Op::In(_) => return Err(query::Error::unsupported_op(field.to_owned())),
                query::Op::Match(_) => return Err(query::Error::unsupported_op(field.to_owned())),
            };

            // r.clause = self.fmt_clause(&r.clause);

            Ok(r)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use mosaicod_query::{ClausesCompiler, Op};
    use std::collections::HashMap;

    #[test]
    fn unsupported_op() {
        let mut fmt = SqlQueryCompiler::new();

        let qr = ClausesCompiler::new()
            .expr("my-field", Op::Gt("topic-name".to_owned()), &mut fmt)
            .compile();

        assert!(qr.is_err());
        assert!(matches!(qr.err().unwrap(), query::Error::OpError { .. }));
    }

    #[test]
    fn topic_fields() {
        let mut fmt = SqlQueryCompiler::new();

        let qr = ClausesCompiler::new()
            .expr(
                "topic.locator_name",
                Op::Match("my-topic".to_owned()),
                &mut fmt,
            )
            .expr(
                "topic.ontology_tag",
                Op::Neq("my-ontology-tag".to_owned()),
                &mut fmt,
            )
            .compile()
            .expect("problem building query");

        dbg!(&qr);

        if let Some(idx) = qr
            .clauses
            .iter()
            .position(|c| c == r#"topic.locator_name LIKE $1"#)
        {
            assert_eq!(qr.values[idx], query::Value::Text("%my-topic%".to_owned()));
        } else {
            panic!("match not found");
        }

        if let Some(idx) = qr
            .clauses
            .iter()
            .position(|c| c == r#"topic.ontology_tag != $2"#)
        {
            assert_eq!(
                qr.values[idx],
                query::Value::Text("my-ontology-tag".to_owned())
            );
        } else {
            panic!("match not found");
        }
    }

    #[test]
    fn user_metadata() {
        let mdata: HashMap<String, query::Op<query::Value>> = HashMap::from([
            (
                "my.custom.field.1".to_owned(),
                query::Op::Eq(query::Value::Float(10.0)),
            ),
            (
                "my.custom.field.2".to_owned(),
                query::Op::Neq(query::Value::Boolean(true)),
            ),
        ]);

        let mut jqc = JsonQueryCompiler::new();
        let fmt = jqc.with_field_and_placeholder("topic.user_metadata".to_owned(), 1);

        let mut cc = ClausesCompiler::new();
        for (k, v) in mdata {
            cc = cc.expr(&k, v, fmt);
        }
        let qr = cc.compile().expect("problem building query");

        dbg!(&qr);

        if let Some(idx) = qr.clauses.iter().position(|c| {
            c.contains(r#"(topic.user_metadata #>> '{my,custom,field,1}')::numeric = $"#)
        }) {
            // check that the placeholder has the correct value
            assert_eq!(
                qr.clauses[idx].chars().last(),
                (idx + 1).to_string().chars().last()
            );
            assert_eq!(qr.values[idx], query::Value::Float(10.0));
        } else {
            panic!("match not found");
        }

        if let Some(idx) = qr.clauses.iter().position(|c| {
            c.contains(r#"(topic.user_metadata #>> '{my,custom,field,2}')::boolean != $"#)
        }) {
            // check that the placeholder has the correct value
            assert_eq!(
                qr.clauses[idx].chars().last(),
                (idx + 1).to_string().chars().last()
            );
            assert_eq!(qr.values[idx], query::Value::Boolean(true));
        } else {
            panic!("match not found");
        }
    }
}
