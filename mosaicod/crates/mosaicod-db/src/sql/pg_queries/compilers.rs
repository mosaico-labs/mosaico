use mosaicod_query as query;

pub struct JsonQueryCompiler {
    internal: internal::JsonQueryCompiler,
}

impl JsonQueryCompiler {
    pub fn new(placeholder: query::Placeholder) -> Self {
        Self {
            internal: internal::JsonQueryCompiler::new(placeholder),
        }
    }

    pub fn with_field(
        &mut self,
        field: String,
        // placeholder: usize,
    ) -> &mut internal::JsonQueryCompiler {
        self.internal.field(field);
        // self.internal.placeholder(placeholder);
        &mut self.internal
    }
}

pub struct SqlQueryCompiler {
    placeholder: query::Placeholder,
}

impl SqlQueryCompiler {
    pub fn new(placeholder: query::Placeholder) -> Self {
        Self { placeholder }
    }

    fn consume_placeholder(&mut self) -> String {
        let current_idx = self.placeholder.consume();
        format!("${}", current_idx)
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
                    return Err(query::Error::empty_in(field.to_owned()));
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
                    if text.is_empty() {
                        return Err(query::Error::empty_pattern(field.to_owned()));
                    }
                    let clause = format!(
                        "mosaico_regex_match({field}, {})",
                        self.consume_placeholder()
                    );
                    query::CompiledClause::new(clause, vec![query::Value::Text(text)])
                } else {
                    return Err(query::Error::unsupported_op(field.to_owned()));
                }
            }
        };

        Ok(r)
    }
}

mod internal {
    use mosaicod_query::Placeholder;

    use super::*;

    pub struct JsonQueryCompiler {
        placeholder: query::Placeholder,
        field: String,
    }

    impl JsonQueryCompiler {
        pub fn new(placeholder: Placeholder) -> Self {
            Self {
                placeholder,
                field: String::new(),
            }
        }

        pub fn field(&mut self, field: String) {
            self.field = field;
        }

        fn consume_placeholder(&mut self) -> String {
            let current_idx = self.placeholder.consume();
            format!("${}", current_idx)
        }

        fn fmt_clause(&self, subfield: &str) -> String {
            format!("$.{}", subfield)
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
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ == $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Neq(v) => {
                    let v: query::Value = v.into();
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ != $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Leq(v) => {
                    let v: query::Value = v.into();
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ <= $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Geq(v) => {
                    let v: query::Value = v.into();
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ >= $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Lt(v) => {
                    let v: query::Value = v.into();
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ < $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Gt(v) => {
                    let v: query::Value = v.into();
                    let placeholder = self.consume_placeholder();
                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ > $val)', jsonb_build_object('val', {}))",
                            self.field, field, placeholder
                        ),
                        vec![v],
                    )
                }
                query::Op::Ex => query::CompiledClause::new(
                    format!("jsonb_path_exists({}, '{}')", self.field, field,),
                    Vec::new(),
                ),
                query::Op::Nex => query::CompiledClause::new(
                    format!("NOT jsonb_path_exists({}, '{}')", self.field, field,),
                    Vec::new(),
                ),
                query::Op::Between(range) => {
                    let min: query::Value = range.min.into();
                    let max: query::Value = range.max.into();

                    let pmin = self.consume_placeholder();
                    let pmax = self.consume_placeholder();

                    query::CompiledClause::new(
                        format!(
                            "jsonb_path_exists({}, '{} ? (@ >= $min && @ <= $max)', jsonb_build_object('min', {}, 'max', {}))",
                            self.field, field, pmin, pmax
                        ),
                        vec![min, max],
                    )
                }
                query::Op::In(items) => {
                    if items.is_empty() {
                        return Err(query::Error::empty_in(field.to_owned()));
                    }
                    let values: Vec<query::Value> = items.into_iter().map(Into::into).collect();
                    let condition = values
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("@ == $val{i}"))
                        .collect::<Vec<String>>()
                        .join(" || ");
                    let placeholders = values
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("'val{}', {}", i, self.consume_placeholder()))
                        .collect::<Vec<String>>()
                        .join(", ");
                    let clause = format!(
                        "jsonb_path_exists({}, '{} ? ({})', jsonb_build_object({}))",
                        self.field, field, condition, placeholders
                    );
                    query::CompiledClause::new(clause, values)
                }
                query::Op::Match(v) => {
                    // NOTE: here we can't use Json path, because it uses its own regex syntax based
                    // on the DBMS(POSIX for Postgres and XPath for others).

                    let value: query::Value = v.into();
                    if let query::Value::Text(text) = value {
                        if text.is_empty() {
                            return Err(query::Error::empty_pattern(field.to_owned()));
                        }

                        let placeholder = self.consume_placeholder();

                        // Here we still use the #>> operator and not jsonpath.
                        let subfield = format!(
                            "{{{}}}",
                            field.strip_prefix("$.").unwrap().replace(".", ",")
                        );

                        let clause = format!(
                            "mosaico_regex_match({} #>> '{}', {})",
                            self.field, subfield, placeholder
                        );
                        query::CompiledClause::new(clause, vec![query::Value::Text(text)])
                    } else {
                        return Err(query::Error::unsupported_op(field.to_owned()));
                    }
                }
            };

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
        let placeholder = query::Placeholder::new();
        let mut fmt = SqlQueryCompiler::new(placeholder);

        let qr = ClausesCompiler::new()
            .expr("my-field", Op::Gt("topic-name".to_owned()), &mut fmt)
            .compile();

        assert!(qr.is_err());
        assert!(matches!(qr.err().unwrap(), query::Error::OpError { .. }));
    }

    #[test]
    fn topic_fields() {
        let placeholder = query::Placeholder::new();
        let mut fmt = SqlQueryCompiler::new(placeholder);

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
            .position(|c| c == r#"mosaico_regex_match(topic.locator_name, $1)"#)
        {
            assert_eq!(qr.values[idx], query::Value::Text("my-topic".to_owned()));
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
    fn user_metadata_in() {
        let mdata: HashMap<String, query::Op<query::Value>> = HashMap::from([(
            "imu.acceleration.x".to_owned(),
            query::Op::In(vec![
                query::Value::Integer(1),
                query::Value::Integer(6),
                query::Value::Integer(-1),
            ]),
        )]);

        let placeholder = query::Placeholder::new();
        let mut jqc = JsonQueryCompiler::new(placeholder);
        let fmt = jqc.with_field("topic.user_metadata".to_owned());

        let mut cc = ClausesCompiler::new();
        for (k, v) in mdata {
            cc = cc.expr(&k, v, fmt);
        }
        let qr = cc.compile().expect("problem building query");

        dbg!(&qr);

        let found = qr.clauses.iter().any(|c| {
            c.contains(
                r#"jsonb_path_exists(topic.user_metadata, '$.imu.acceleration.x ? (@ == $val0 || @ == $val1 || @ == $val2)', jsonb_build_object('val0', $1, 'val1', $2, 'val2', $3))"#,
            )
        });
        assert!(found, "in clause not found in {:?}", qr.clauses);

        assert_eq!(qr.values[0], query::Value::Integer(1));
        assert_eq!(qr.values[1], query::Value::Integer(6));
        assert_eq!(qr.values[2], query::Value::Integer(-1));
    }

    #[test]
    fn user_metadata_match() {
        let mdata: HashMap<String, query::Op<query::Value>> = HashMap::from([(
            "vehicle.name".to_owned(),
            query::Op::Match(query::Value::Text("^truck".to_owned())),
        )]);

        let placeholder = query::Placeholder::new();
        let mut jqc = JsonQueryCompiler::new(placeholder);
        let fmt = jqc.with_field("topic.user_metadata".to_owned());

        let mut cc = ClausesCompiler::new();
        for (k, v) in mdata {
            cc = cc.expr(&k, v, fmt);
        }
        let qr = cc.compile().expect("problem building query");

        dbg!(&qr);

        let found = qr.clauses.iter().any(|c| {
            c.contains(r#"mosaico_regex_match(topic.user_metadata #>> '{vehicle,name}', $1)"#)
        });
        assert!(found, "match clause not found in {:?}", qr.clauses);
        assert_eq!(qr.values[0], query::Value::Text("^truck".to_owned()));
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

        let placeholder = query::Placeholder::new();
        let mut jqc = JsonQueryCompiler::new(placeholder);
        let fmt = jqc.with_field("topic.user_metadata".to_owned());

        let mut cc = ClausesCompiler::new();
        for (k, v) in mdata {
            cc = cc.expr(&k, v, fmt);
        }
        let qr = cc.compile().expect("problem building query");

        dbg!(&qr);

        if let Some(idx) = qr.clauses.iter().position(|c| {
            c.contains(r#"jsonb_path_exists(topic.user_metadata, '$.my.custom.field.1 ? (@ == $val)', jsonb_build_object('val', $"#)
        }) {
            // check that the placeholder has the correct value
            assert_eq!(
                qr.clauses[idx].chars().nth_back(2).unwrap(),
                (idx + 1).to_string().chars().last().unwrap()
            );
            assert_eq!(qr.values[idx], query::Value::Float(10.0));
        } else {
            panic!("match not found");
        }

        if let Some(idx) = qr.clauses.iter().position(|c| {
            c.contains(r#"jsonb_path_exists(topic.user_metadata, '$.my.custom.field.2 ? (@ != $val)', jsonb_build_object('val', $"#)
        }) {
            // check that the placeholder has the correct value
            assert_eq!(
                qr.clauses[idx].chars().nth_back(2).unwrap(),
                (idx + 1).to_string().chars().last().unwrap()
            );
            assert_eq!(qr.values[idx], query::Value::Boolean(true));
        } else {
            panic!("match not found");
        }
    }
}
