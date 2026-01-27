//! This module provides the timeseries gateway, a wrapper around the datafusion
//! query engine tailored for reading and processing timeseries data files stored in the
//! application's underlying object store (S3, GCS, etc.).
//!
//! The engine integrates directly with the configured [`store::Store`] to resolve
//! paths and access data sources like Parquet files efficiently.
use super::{Error, OntologyExprGroup, OntologyField, Op, Value};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use log::trace;
use mosaicod_core::{params, types};
use mosaicod_rw::ToParquetProperties;
use mosaicod_store as store;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub type TimeseriesRef = Arc<Timeseries>;

pub struct Timeseries {
    runtime: Arc<RuntimeEnv>,
    store: Arc<store::Store>,
}

impl Timeseries {
    pub fn try_new(store: Arc<store::Store>) -> Result<Self, Error> {
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_store_registry(store.registry())
                .build()?,
        );

        Ok(Timeseries {
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
            conf = conf.with_batch_size(batch_size);
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
        let expr = expr_group_to_df_expr(filter);

        let data_frame = if let Some(expr) = expr {
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
    /// This function will return a [`Error::DataFusion`] if backend fails, a [`Error::NotFound`] if
    /// no value is returned or an [`Error::BadField`] is there is some problem retrieving the
    /// timestamp values (very rare since schema are checked before data upload)
    pub async fn timestamp_range(self) -> Result<types::TimestampRange, Error> {
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

            let ts_min = scalar_value_to_timestamp(ts_min).ok_or_else(|| {
                Error::bad_field(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP.to_owned())
            })?;
            let ts_max = scalar_value_to_timestamp(ts_max).ok_or_else(|| {
                Error::bad_field(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP.to_owned())
            })?;

            return Ok(types::TimestampRange::between(ts_min, ts_max));
        }

        Err(Error::NotFound)
    }
}

fn scalar_value_to_timestamp(value: ScalarValue) -> Option<types::Timestamp> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(v.into()),
        _ => None,
    }
}

fn unfold_field(field: &OntologyField) -> Expr {
    let mut fields = field.field().split(".");
    // By construction fields needs to have at least a value
    let mut col = col(fields.next().unwrap());
    for s in fields {
        col = col.field(s);
    }
    col
}

fn expr_group_to_df_expr<V>(filter: OntologyExprGroup<V>) -> Option<Expr>
where
    V: Into<Value>,
{
    let mut ret: Option<Expr> = None;

    for expr in filter.into_iter() {
        let (field, op) = expr.into_parts();
        let expr = match op {
            Op::Eq(v) => Some(unfold_field(&field).eq(value_to_df_expr(v.into()))),
            Op::Neq(v) => Some(unfold_field(&field).not_eq(value_to_df_expr(v.into()))),
            Op::Leq(v) => Some(unfold_field(&field).lt_eq(value_to_df_expr(v.into()))),
            Op::Geq(v) => Some(unfold_field(&field).gt_eq(value_to_df_expr(v.into()))),
            Op::Lt(v) => Some(unfold_field(&field).lt(value_to_df_expr(v.into()))),
            Op::Gt(v) => Some(unfold_field(&field).gt(value_to_df_expr(v.into()))),
            Op::Ex => None,  // no-op
            Op::Nex => None, // no-op
            Op::Between(range) => {
                let vmin: Value = range.min.into();
                let vmax: Value = range.max.into();
                let emin = unfold_field(&field).lt_eq(value_to_df_expr(vmax));
                let emax = unfold_field(&field).gt_eq(value_to_df_expr(vmin));
                Some(emin.and(emax))
            }
            Op::In(items) => {
                let list = items
                    .into_iter()
                    .map(|v| value_to_df_expr(v.into()))
                    .collect();
                Some(unfold_field(&field).in_list(list, false))
            }
            Op::Match(v) => Some(unfold_field(&field).like(value_to_df_expr(v.into()))),
        };

        if let Some(expr) = expr {
            if ret.is_none() {
                ret = Some(expr);
            } else {
                ret = Some(ret.unwrap().and(expr));
            }
        }
    }

    ret
}

fn value_to_df_expr(v: Value) -> Expr {
    match v {
        Value::Integer(v) => lit(v),
        Value::Float(v) => lit(v),
        Value::Text(v) => lit(v),
        Value::Boolean(v) => lit(v),
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
        let file_path = "dummy_file.parquet";

        let store = store::testing::Store::new_random_on_tmp().unwrap();

        write_dummy_file(&store, file_path).await;

        let ts_gw = Timeseries::try_new((*store).clone()).unwrap();

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

        assert_eq!(ts_range.start, 10010.into());
        assert_eq!(ts_range.end, 10020.into());
    }
}
