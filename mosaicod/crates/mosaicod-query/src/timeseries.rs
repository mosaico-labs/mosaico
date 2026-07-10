//! This module provides the timeseries gateway, a wrapper around the datafusion
//! query engine tailored for reading and processing timeseries data files stored in the
//! application's underlying object store (S3, GCS, etc.).
//!
//! The engine integrates directly with the configured [`store::Store`] to resolve
//! paths and access data sources like Parquet files efficiently.
use super::{Error, IndexSpecifier, OntologyExprGroup, OntologyField, Op, Value};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::functions_nested::expr_fn::{
    array_distinct, array_element, array_has, array_has_any, array_intersect, array_max, array_min,
    cardinality, make_array,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use log::trace;
use mosaicod_core::{params, types};
use mosaicod_rw::ToParquetProperties;
use mosaicod_store as store;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub type TimeseriesEngineRef = Arc<TimeseriesEngine>;

pub struct TimeseriesEngine {
    runtime: Arc<RuntimeEnv>,
    store: Arc<store::Store>,
}

impl TimeseriesEngine {
    pub fn try_new(store: Arc<store::Store>, memory_limit_bytes: usize) -> Result<Self, Error> {
        let memory_pool = if memory_limit_bytes != 0 {
            Some(Arc::new(FairSpillPool::new(memory_limit_bytes)))
        } else {
            None
        };

        let mut builder = RuntimeEnvBuilder::new().with_object_store_registry(store.registry());

        if let Some(memory_pool) = memory_pool {
            builder = builder
                .with_memory_pool(memory_pool)
                .with_disk_manager_builder(DiskManagerBuilder::default());
        }

        let runtime = Arc::new(builder.build()?);

        Ok(TimeseriesEngine {
            runtime,
            store: store.clone(),
        })
    }

    /// Read time-series data from a path.
    ///
    /// All files in the provided path will be included in the read.
    ///
    /// If `batch_size` is provided, the system will use it to configure the batch size
    /// for the query engine. This allows callers to control message sizes based on
    /// pre-computed statistics from the database.
    pub async fn read(
        &self,
        path: impl AsRef<Path>,
        format: types::Format,
        batch_size: Option<usize>,
    ) -> Result<TimeseriesResult, Error> {
        // Use Parquet format strategy for listing options
        let parquet_strategy = format
            .to_parquet_properties()
            .expect("TimeseriesGateway::read requires a Parquet-based format");
        let listing_options = parquet_strategy.listing_options();

        let mut conf = SessionConfig::new();
        if let Some(batch_size) = batch_size {
            conf = conf
                .with_batch_size(batch_size)
                // Reduce the number of partition used to avoid management overhead
                .with_target_partitions(1)
                // Parquet specific optimizations
                .set_bool("datafusion.execution.parquet.pushdown_filters", true)
                .set_bool("datafusion.execution.parquet.reorder_filters", true);
        }

        let ctx = SessionContext::new_with_config_rt(conf, self.runtime.clone());

        // we use `data` as internal reference for this context
        ctx.register_listing_table(
            "data",
            self.datafile_url(path)?,
            listing_options,
            None,
            None,
        )
        .await?;

        let select = format!(
            "SELECT * FROM data ORDER BY {}",
            params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP
        );

        let df = ctx.sql(&select).await?;

        Ok(TimeseriesResult { data_frame: df })
    }

    fn datafile_url(&self, path: impl AsRef<Path>) -> Result<url::Url, Error> {
        Ok(self
            .store
            .as_ref()
            .url_schema
            .join(&path.as_ref().to_string_lossy())?)
    }
}

#[derive(Clone)]
pub struct TimeseriesResult {
    data_frame: DataFrame,
}

impl TimeseriesResult {
    pub fn schema_with_metadata(&self, metadata: HashMap<String, String>) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            self.data_frame.schema().fields().clone(),
            metadata,
        ))
    }

    pub fn filter_by_timestamp_range(
        mut self,
        ts_range: types::TimestampRange,
    ) -> Result<Self, Error> {
        if !ts_range.start.is_unbounded() {
            self.data_frame = self.data_frame.filter(
                col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)
                    .gt_eq(lit(ts_range.start.as_i64())),
            )?;
        }

        if !ts_range.end.is_unbounded() {
            self.data_frame = self.data_frame.filter(
                col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)
                    .lt(lit(ts_range.end.as_i64())),
            )?;
        }

        Ok(self)
    }

    pub fn filter<V>(self, filter: OntologyExprGroup<V>) -> Result<Self, Error>
    where
        V: Into<Value>,
    {
        let schema = self.data_frame.schema().as_arrow().clone();
        let expr = expr_group_to_df_expr(filter, &schema)?;

        let data_frame = if let Some(expr) = expr {
            // Resolve LambdaVariable.field for any higher-order functions (e.g.
            // array_any_match). The programmatic df.filter() path does not run
            // the SQL analyzer, so lambda variable types must be resolved
            // manually using the current schema before handing the expression
            // to the DataFrame API.
            let expr = expr
                .resolve_lambda_variables(self.data_frame.schema())?
                .data;
            trace!("filter expression: {}", expr);
            self.data_frame.filter(expr)?
        } else {
            self.data_frame
        };

        Ok(TimeseriesResult { data_frame })
    }

    pub async fn stream(self) -> Result<SendableRecordBatchStream, Error> {
        self.data_frame.execute_stream().await.map_err(|e| e.into())
    }

    pub async fn count(self) -> Result<usize, Error> {
        Ok(self.data_frame.count().await?)
    }

    /// Checks if there are any rows matching the current query.
    /// This is more efficient than `count()` when you only need to know if results exist,
    /// as it stops after finding the first matching row.
    pub async fn has_rows(self) -> Result<bool, Error> {
        // Limit to 1 row for early termination - avoids full scan
        let limited = self.data_frame.limit(0, Some(1))?;
        Ok(limited.count().await? > 0)
    }

    /// Returns the timestamp range matching the current query.
    /// Timestamp range represent the timestamp of the first and last occurrence of the
    /// query conditions.
    ///
    /// # Errors
    ///
    /// This function will return a [`Error::DataFusion`] if backend fails or
    /// an [`Error::NullMinMaxTimestamps`] if there is some problem retrieving the
    /// timestamp values (very rare since schema are checked before data upload)
    pub async fn timestamp_range(self) -> Result<Option<types::TimestampRange>, Error> {
        let stats = self.data_frame.aggregate(
            vec![],
            vec![
                min(col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)),
                max(col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)),
            ],
        )?;

        let batches = stats.collect().await?;

        if let Some(batch) = batches.first() {
            let ts_min = ScalarValue::try_from_array(batch.column(0), 0)?;
            let ts_max = ScalarValue::try_from_array(batch.column(1), 0)?;

            let ts_min = scalar_value_to_timestamp(ts_min);
            let ts_max = scalar_value_to_timestamp(ts_max);

            return match (ts_min, ts_max) {
                (Some(min), Some(max)) => Ok(Some(types::TimestampRange::between(min, max))),
                (None, None) => Ok(None),
                _ => Err(Error::NullMinMaxTimestamps),
            };
        }

        Ok(None)
    }
}

fn scalar_value_to_timestamp(value: ScalarValue) -> Option<types::Timestamp> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(v.into()),
        _ => None,
    }
}

/// Verifies that `field` resolves to a list type in `schema`
fn field_schema_is_list(field: &OntologyField, schema: &Schema) -> bool {
    let parsed = field.field_path();
    let mut segs = parsed.field_segments();

    let Some(first) = segs.next() else {
        return false;
    };
    let Ok(arrow_field) = schema.field_with_name(first) else {
        return false;
    };
    let mut dtype = arrow_field.data_type();

    // With a specifier, list_access.segment_index tells us the exact segment
    // that holds the list, navigate only that far and stop (sub-fields inside
    // the list are irrelevant here).
    // Without a specifier, navigate all segments so dtype ends up on the last
    // field, which is the one we want to verify is a list.
    let list_idx = parsed.list_access.as_ref().map(|la| la.segment_index);

    if list_idx != Some(0) {
        for (i, seg) in segs.enumerate() {
            let seg_idx = i + 1;
            match dtype {
                DataType::Struct(fields) => {
                    let Some(f) = fields.iter().find(|f| f.name() == seg) else {
                        return false;
                    };
                    dtype = f.data_type();
                }
                _ => return false,
            }
            if list_idx == Some(seg_idx) {
                break;
            }
        }
    }

    matches!(
        dtype,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

/// Converts an [`OntologyField`] dot-path into a nested DataFusion [`Expr`].
/// Each segment becomes a `.field()` access on the previous one, e.g.
/// `"acceleration.x"` -> `col("acceleration").field("x")`.
fn unfold_field(field: &OntologyField) -> Expr {
    let parsed = field.field_path();
    let mut all = parsed.field_segments();
    let mut expr = col(all.next().expect("field has at least one segment"));
    for seg in all {
        expr = expr.field(seg);
    }
    expr
}

fn plain_list_op_to_df_expr<V: Into<Value>>(
    arr: Expr,
    op: Op<V>,
    field_name: &str,
) -> Result<Option<Expr>, Error> {
    Ok(match op {
        Op::Eq(v) => Some(list_value_eq_expr(arr, v.into(), field_name)?),
        Op::Neq(v) => Some(list_value_eq_expr(arr, v.into(), field_name)?.not()),
        _ => return Err(Error::unsupported_op(field_name.to_owned())),
    })
}

/// DataFusion does not support `=` on List columns directly, so we decompose
/// into scalar comparisons. Different-length lists always produce false.
fn list_value_eq_expr(arr: Expr, v: Value, field_name: &str) -> Result<Expr, Error> {
    let (item_exprs, len): (Vec<Expr>, usize) = match v {
        Value::IntegerArray(items) => {
            let n = items.len();
            (items.into_iter().map(lit).collect(), n)
        }
        Value::FloatArray(items) => {
            let n = items.len();
            (items.into_iter().map(lit).collect(), n)
        }
        Value::TextArray(items) => {
            let n = items.len();
            (items.into_iter().map(lit).collect(), n)
        }
        Value::BooleanArray(items) => {
            let n = items.len();
            (items.into_iter().map(lit).collect(), n)
        }
        scalar => return Ok(arr.eq(value_to_df_expr(scalar))),
    };

    let max = params::params().max_size_plain_list_eq.value;
    if len > max {
        return Err(Error::list_too_large(field_name.to_owned(), max));
    }

    let len_check = cardinality(arr.clone()).eq(lit(len as u64));
    Ok(item_exprs
        .into_iter()
        .enumerate()
        .fold(len_check, |acc, (i, item_expr)| {
            // array_element uses 1-based indexing
            acc.and(array_element(arr.clone(), lit(i as i64 + 1)).eq(item_expr))
        }))
}

/// Applies a scalar (non-array) operator to a DataFusion expression.
fn scalar_op_to_df_expr<V: Into<Value>>(expr: Expr, op: Op<V>) -> Result<Option<Expr>, Error> {
    Ok(Some(match op {
        Op::Eq(v) => expr.eq(value_to_df_expr(v.into())),
        Op::Neq(v) => expr.not_eq(value_to_df_expr(v.into())),
        Op::Leq(v) => expr.lt_eq(value_to_df_expr(v.into())),
        Op::Geq(v) => expr.gt_eq(value_to_df_expr(v.into())),
        Op::Lt(v) => expr.lt(value_to_df_expr(v.into())),
        Op::Gt(v) => expr.gt(value_to_df_expr(v.into())),
        Op::Ex | Op::Nex =>
        // No-op. Existence of a column is checked previously. Here we are evaluating the content (data values).
        {
            return Ok(None);
        }
        Op::Between(range) => {
            let vmin = value_to_df_expr(range.min.into());
            let vmax = value_to_df_expr(range.max.into());
            expr.clone().gt_eq(vmin).and(expr.lt_eq(vmax))
        }
        Op::In(items) => {
            let list = items
                .into_iter()
                .map(|v| value_to_df_expr(v.into()))
                .collect();
            expr.in_list(list, false)
        }
        Op::Match(v) => {
            let Value::Text(text) = v.into() else {
                return Err(Error::unsupported_op(expr.to_string()));
            };

            let regex_pattern = super::regex::wildcard_to_posix_regex(text.as_str())
                .map_err(|e| super::regex_to_query_error(e, expr.to_string()))?
                .to_string();

            regexp_like(expr, value_to_df_expr(regex_pattern.into()), None)
        }
    }))
}

/// Builds the DataFusion expression for [?], at least one element satisfies the predicate.
fn any_op_to_df_expr<V: Into<Value>>(arr: Expr, op: Op<V>) -> Result<Option<Expr>, Error> {
    Ok(Some(match op {
        Op::Eq(v) => array_has(arr, value_to_df_expr(v.into())),
        Op::Neq(v) => {
            let v = value_to_df_expr(v.into());
            let res = cardinality(array_remove_all(arr, v));
            res.gt(lit(0))
        }
        Op::Gt(v) => array_max(arr).gt(value_to_df_expr(v.into())),
        Op::Geq(v) => array_max(arr).gt_eq(value_to_df_expr(v.into())),
        Op::Lt(v) => array_min(arr).lt(value_to_df_expr(v.into())),
        Op::Leq(v) => array_min(arr).lt_eq(value_to_df_expr(v.into())),
        Op::Between(range) => {
            let vmin = value_to_df_expr(range.min.into());
            let vmax = value_to_df_expr(range.max.into());
            let x = lambda_var("x");
            let body = x.clone().gt_eq(vmin).and(x.lt_eq(vmax));
            array_any_match(arr, lambda(["x"], body))
        }
        Op::In(items) => {
            let set = make_array(
                items
                    .into_iter()
                    .map(|v| value_to_df_expr(v.into()))
                    .collect(),
            );
            array_has_any(arr, set)
        }
        Op::Match(v) => {
            let Value::Text(text) = v.into() else {
                return Err(Error::unsupported_op(arr.to_string()));
            };

            let regex_pattern = super::regex::wildcard_to_posix_regex(text.as_str())
                .map_err(|e| super::regex_to_query_error(e, arr.to_string()))?
                .to_string();

            let x = lambda_var("x");
            let body = regexp_like(x, value_to_df_expr(regex_pattern.into()), None);
            array_any_match(arr, lambda(["x"], body))
        }
        Op::Ex | Op::Nex =>
        // No-op. Existence of a column is checked previously. Here we are evaluating the content (data values).
        {
            return Ok(None);
        }
    }))
}

/// Builds the DataFusion expression for [!], every element satisfies the predicate.
fn all_op_to_df_expr<V: Into<Value>>(arr: Expr, op: Op<V>) -> Result<Option<Expr>, Error> {
    Ok(Some(match op {
        Op::Eq(v) => {
            let v = value_to_df_expr(v.into());

            array_min(arr.clone())
                .eq(v.clone())
                .and(array_max(arr).eq(v))
        }
        Op::Neq(v) => array_has(arr, value_to_df_expr(v.into())).not(),
        Op::Gt(v) => array_min(arr).gt(value_to_df_expr(v.into())),
        Op::Geq(v) => array_min(arr).gt_eq(value_to_df_expr(v.into())),
        Op::Lt(v) => array_max(arr).lt(value_to_df_expr(v.into())),
        Op::Leq(v) => array_max(arr).lt_eq(value_to_df_expr(v.into())),
        Op::Between(range) => {
            let vmin = value_to_df_expr(range.min.into());
            let vmax = value_to_df_expr(range.max.into());

            array_min(arr.clone())
                .gt_eq(vmin)
                .and(array_max(arr).lt_eq(vmax))
        }
        Op::In(items) => {
            let set = make_array(
                items
                    .into_iter()
                    .map(|v| value_to_df_expr(v.into()))
                    .collect(),
            );
            let distinct_count = cardinality(array_distinct(arr.clone()));
            let intersect_count = cardinality(array_intersect(arr, set));
            distinct_count.eq(intersect_count)
        }
        Op::Match(v) => {
            // all elements match <-> no element fails to match

            let Value::Text(text) = v.into() else {
                return Err(Error::unsupported_op(arr.to_string()));
            };

            let regex_pattern = super::regex::wildcard_to_posix_regex(text.as_str())
                .map_err(|e| super::regex_to_query_error(e, arr.to_string()))?
                .to_string();

            let x = lambda_var("x");
            let body = not(regexp_like(x, value_to_df_expr(regex_pattern.into()), None));
            not(array_any_match(arr, lambda(["x"], body)))
        }
        Op::Ex | Op::Nex =>
        // No-op. Existence of a column is checked previously. Here we are evaluating the content (data values).
        {
            return Ok(None);
        }
    }))
}

/// Builds the DataFusion expression for the portion of the field path that leads up to
/// (and including) the list column, stopping before any sub-fields that follow the list
/// specifier.
///
/// Examples:
/// - `"pose[?].x"`     ->  `col("pose")`
/// - `"a.b[?].c.d"`    ->  `col("a").field("b")`
/// - `"value"`         ->  `col("value")`
fn list_col_expr(field: &OntologyField) -> Expr {
    let parsed = field.field_path();
    let list_idx = parsed.list_access.as_ref().map(|la| la.segment_index);
    let mut segs = parsed.field_segments().enumerate();
    let (_, first) = segs.next().expect("field has at least one segment");
    let mut expr = col(first);
    for (i, seg) in segs {
        if list_idx.is_some_and(|li| i > li) {
            break;
        }
        expr = expr.field(seg);
    }
    expr
}

/// Returns the field-path segments that come *after* the list specifier, i.e. the
/// sub-fields to navigate inside each struct element of the list.
///
/// For `pose[?].x` the list is at segment 0 (`pose`) and the sub-path is `["x"]`.
/// For `a.b[?].c.d` the list is at segment 1 (`b`) and the sub-path is `["c", "d"]`.
/// For `x[?]` (list of primitives, no struct navigation) the sub-path is empty (`[]`).
///
/// An empty result means the predicate targets the list elements directly (e.g. a
/// `List<f64>`), so the existing scalar array functions (`array_max`, `array_has`, …)
/// are sufficient. A non-empty result means struct field access is required inside a
/// lambda, see [`struct_elem_predicate`].
fn inner_field_segs(field: &OntologyField) -> Vec<String> {
    let parsed = field.field_path();
    match &parsed.list_access {
        None => vec![],
        Some(la) => parsed
            .field_segments()
            .enumerate()
            .filter(|(i, _)| *i > la.segment_index)
            .map(|(_, s)| s.to_owned())
            .collect(),
    }
}

/// Applies a sequence of field-name segments to an expression via nested `.field()` calls.
///
/// Used in two contexts:
/// - Inside a lambda body, where `expr` is a `lambda_var("elem")` representing one struct
///   element of the list, and `field_segments` navigates into it (e.g. `elem.field("x")`).
/// - After `array_element`, where `expr` is the result of indexing into a list with `[N]`
///   and `field_segments` navigates into the resulting struct (e.g. `readings[0].field("x")`).
///
/// When `field_segments` is empty this is a no-op and the expression is returned unchanged.
fn chain_field_accesses(mut expr: Expr, field_segments: &[String]) -> Expr {
    for seg in field_segments {
        expr = expr.field(seg.as_str());
    }
    expr
}

/// Builds the predicate body for a lambda operating on a single struct element of a list.
///
/// When filtering `readings[?].x > 3`, DataFusion needs an expression of the form:
///
/// ```text
/// array_any_match(col("readings"), lambda(["elem"], elem.x > 3))
/// ```
///
/// where `elem.x > 3` is the per-element predicate evaluated against each struct in the
/// list. This function builds exactly that predicate: it creates a `lambda_var("elem")`
/// (the placeholder for the current element), navigates into the target sub-field via
/// [`chain_field_accesses`] (e.g. `elem.field("x")`), and then applies the comparison
/// operator (e.g. `.gt(3.0)`).
fn struct_elem_predicate<V: Into<Value>>(
    field_segments: &[String],
    op: Op<V>,
) -> Result<Option<Expr>, Error> {
    let make_fe = || chain_field_accesses(lambda_var("elem"), field_segments);
    Ok(Some(match op {
        Op::Eq(v) => make_fe().eq(value_to_df_expr(v.into())),
        Op::Neq(v) => make_fe().not_eq(value_to_df_expr(v.into())),
        Op::Gt(v) => make_fe().gt(value_to_df_expr(v.into())),
        Op::Geq(v) => make_fe().gt_eq(value_to_df_expr(v.into())),
        Op::Lt(v) => make_fe().lt(value_to_df_expr(v.into())),
        Op::Leq(v) => make_fe().lt_eq(value_to_df_expr(v.into())),
        Op::Between(range) => {
            let vmin = value_to_df_expr(range.min.into());
            let vmax = value_to_df_expr(range.max.into());
            make_fe().gt_eq(vmin).and(make_fe().lt_eq(vmax))
        }
        Op::In(items) => {
            let list = items
                .into_iter()
                .map(|v| value_to_df_expr(v.into()))
                .collect();
            make_fe().in_list(list, false)
        }
        Op::Match(v) => {
            let Value::Text(text) = v.into() else {
                return Err(Error::unsupported_op(field_segments.join(".")));
            };

            let regex_pattern = super::regex::wildcard_to_posix_regex(text.as_str())
                .map_err(|e| super::regex_to_query_error(e, field_segments.join(".")))?
                .to_string();

            regexp_like(make_fe(), value_to_df_expr(regex_pattern.into()), None)
        }
        Op::Ex | Op::Nex =>
        // No-op. Existence of a column is checked previously. Here we are evaluating the content (data values).
        {
            return Ok(None);
        }
    }))
}

/// Builds the DataFusion expression for `[?]` on a `List<Struct<…>>` column, where the
/// predicate targets a sub-field inside each struct element (e.g. `readings[?].x > 3`).
fn any_op_struct_to_df_expr<V: Into<Value>>(
    arr: Expr,
    field_segments: &[String],
    op: Op<V>,
) -> Result<Option<Expr>, Error> {
    let body = struct_elem_predicate(field_segments, op)?;
    match body {
        None => Ok(None),
        Some(body) => Ok(Some(array_any_match(arr, lambda(["elem"], body)))),
    }
}

/// Builds the DataFusion expression for `[!]` on a `List<Struct<…>>` column.
fn all_op_struct_to_df_expr<V: Into<Value>>(
    arr: Expr,
    field_segments: &[String],
    op: Op<V>,
) -> Result<Option<Expr>, Error> {
    let body = struct_elem_predicate(field_segments, op)?;
    match body {
        None => Ok(None),
        Some(body) => Ok(Some(not(array_any_match(arr, lambda(["elem"], not(body)))))),
    }
}

fn expr_group_to_df_expr<V>(
    filter: OntologyExprGroup<V>,
    schema: &Schema,
) -> Result<Option<Expr>, Error>
where
    V: Into<Value>,
{
    let mut ret: Option<Expr> = None;

    for expr in filter.into_iter() {
        let (field, op) = expr.into_parts();
        let parsed = field.field_path();

        let expr = match parsed.specifier() {
            None => {
                let arr = unfold_field(&field);
                if field_schema_is_list(&field, schema) {
                    plain_list_op_to_df_expr(arr, op, &field.field())
                } else {
                    scalar_op_to_df_expr(arr, op)
                }
            }?,
            Some(IndexSpecifier::At(i)) => {
                if !field_schema_is_list(&field, schema) {
                    return Err(Error::bad_field_with_message(
                        field.to_string(),
                        "expected list type in `schema'".to_owned(),
                    ));
                } else {
                    // DataFusion array_element uses 1-indexing; apply any sub-field after indexing.
                    let arr = list_col_expr(&field);
                    let sub = inner_field_segs(&field);
                    let elem = chain_field_accesses(array_element(arr, lit(*i as i64 + 1)), &sub);
                    scalar_op_to_df_expr(elem, op)
                }
            }?,
            Some(IndexSpecifier::Any) => {
                if !field_schema_is_list(&field, schema) {
                    return Err(Error::bad_field_with_message(
                        field.to_string(),
                        "expected list type in `schema'".to_owned(),
                    ));
                } else {
                    let arr = list_col_expr(&field);
                    let sub = inner_field_segs(&field);
                    if sub.is_empty() {
                        any_op_to_df_expr(arr, op)
                    } else {
                        any_op_struct_to_df_expr(arr, &sub, op)
                    }
                }
            }?,
            Some(IndexSpecifier::All) => {
                if !field_schema_is_list(&field, schema) {
                    return Err(Error::bad_field_with_message(
                        field.to_string(),
                        "expected list type in `schema'".to_owned(),
                    ));
                } else {
                    let arr = list_col_expr(&field);
                    let sub = inner_field_segs(&field);
                    if sub.is_empty() {
                        all_op_to_df_expr(arr, op)
                    } else {
                        all_op_struct_to_df_expr(arr, &sub, op)
                    }
                }
            }?,
        };

        if let Some(expr) = expr {
            if ret.is_none() {
                ret = Some(expr);
            } else {
                ret = Some(ret.unwrap().and(expr));
            }
        }
    }

    Ok(ret)
}

fn value_to_df_expr(v: Value) -> Expr {
    match v {
        Value::Integer(v) => lit(v),
        Value::Float(v) => lit(v),
        Value::Text(v) => lit(v),
        Value::Boolean(v) => lit(v),
        Value::IntegerArray(items) => make_array(items.into_iter().map(lit).collect()),
        Value::FloatArray(items) => make_array(items.into_iter().map(lit).collect()),
        Value::TextArray(items) => make_array(items.into_iter().map(lit).collect()),
        Value::BooleanArray(items) => make_array(items.into_iter().map(lit).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::Range;
    use super::*;
    use mosaicod_core::traits::AsyncWriteToPath;
    use mosaicod_ext::arrow;
    use mosaicod_store as store;

    async fn write_dummy_file(store: &store::Store, file_path: &str) {
        let batch = arrow::testing::dummy_batch();
        let schema = batch.schema().clone();

        use parquet::arrow::arrow_writer::ArrowWriter;

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        store.write_to_path(file_path, buffer).await.unwrap();
    }

    /// Writes a local parquet file and tries to read and retrieve data in the correct timestamp
    /// range
    #[tokio::test]
    async fn timeseries_range() {
        params::load_params_from_env(params::ParamsLoadOptions::testing()).unwrap();

        let file_path = "dummy_file.parquet";

        let store = store::testing::Store::new_random_on_tmp().unwrap();

        write_dummy_file(&store, file_path).await;

        let ts_gw = TimeseriesEngine::try_new((*store).clone(), 0).unwrap();

        let res = ts_gw
            .read(file_path, types::Format::Default, None)
            .await
            .unwrap();

        let expr_grp = OntologyExprGroup::new(vec![
            (
                OntologyField::try_new("tag.value".to_owned()).unwrap(),
                Op::Between(Range::try_new(3, 5).unwrap()),
            )
                .into(),
        ]);

        let res = res.filter(expr_grp).unwrap();

        let ts_range = res.timestamp_range().await.unwrap();

        if let Some(ts) = ts_range {
            assert_eq!(ts.start, 10010.into());
            assert_eq!(ts.end, 10020.into());
        }
    }
}
