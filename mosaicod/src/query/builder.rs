use crate::query::OntologyField;

use super::{Error, IsSupportedOp, OntologyExprGroup, Op, Value};

const EMPTY_CLAUSE: &str = "()";

pub struct CompiledClause {
    pub clause: String,
    pub values: Vec<Value>,
}

impl CompiledClause {
    pub fn new(clause: String, values: Vec<Value>) -> Self {
        Self { clause, values }
    }

    pub fn empty() -> Self {
        Self {
            clause: EMPTY_CLAUSE.to_owned(),
            values: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clause == EMPTY_CLAUSE
    }

    pub fn into_parts(self) -> (String, Vec<Value>) {
        (self.clause, self.values)
    }
}

pub trait CompileClause {
    fn compile_clause<V>(&mut self, field: &str, op: Op<V>) -> Result<CompiledClause, Error>
    where
        V: Into<Value> + IsSupportedOp;
}

/// Specify how a given ontology field needs to be formatted
pub trait OntologyFieldFmt {
    fn ontology_column_fmt(&self, val: &OntologyField) -> String;
}

#[derive(Debug)]
pub struct CompilerResult {
    pub clauses: Vec<String>,
    pub values: Vec<Value>,
}

impl CompilerResult {
    fn new() -> Self {
        Self {
            clauses: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn is_unfiltered(&self) -> bool {
        self.clauses.is_empty()
    }
}

#[derive(Debug)]
pub struct ClausesCompiler {
    result: CompilerResult,
    error: Option<Error>,
}

impl Default for ClausesCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl ClausesCompiler {
    pub fn new() -> Self {
        Self {
            result: CompilerResult::new(),
            error: None,
        }
    }

    pub fn expr<F, V>(mut self, field: &str, op: Op<V>, formatter: &mut F) -> Self
    where
        V: Into<Value> + IsSupportedOp,
        F: CompileClause,
    {
        if self.error.is_some() {
            return self;
        }

        let result = formatter.compile_clause(field, op);
        if let Err(err) = result {
            self.error = Some(err);
            return self;
        }

        // Unwrap here is safe since we checked the error above
        let (clause, mut values) = result.unwrap().into_parts();

        self.result.clauses.push(clause);
        self.result.values.append(&mut values);

        self
    }

    // es: field = topic.user_metadata
    pub fn ontology_expr_group<F, V>(
        mut self,
        filter: OntologyExprGroup<V>,
        formatter: &mut F,
    ) -> Self
    where
        V: Into<Value> + IsSupportedOp,
        F: CompileClause + OntologyFieldFmt,
    {
        if self.error.is_some() {
            return self;
        }

        for expr in filter.into_iter() {
            let (ontology_field, op) = expr.into_parts();
            let field = formatter.ontology_column_fmt(&ontology_field);
            self = self.expr(&field, op, formatter);
        }

        self
    }

    pub fn compile(self) -> Result<CompilerResult, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }

        Ok(self.result)
    }
}
