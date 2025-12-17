//! This module provides the timeseries gateway, a wrapper around the datafusion
//! query engine tailored for reading and processing timeseries data files stored in the
//! application's underlying object store (S3, GCS, etc.).
//!
//! The engine integrates directly with the configured [`store::Store`] to resolve
//! paths and access data sources like Parquet files efficiently.
use log::trace;

use crate::{params, query, rw, store};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::Error;

pub type TimeseriesGwRef = Arc<TimeseriesGw>;

pub struct TimeseriesGw {
    runtime: Arc<RuntimeEnv>,
    store: Arc<store::Store>,
}

impl TimeseriesGw {
    pub fn try_new(store: Arc<store::Store>) -> Result<Self, Error> {
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_store_registry(store.registry())
                .build()?,
        );

        Ok(TimeseriesGw {
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
        format: rw::Format,
        batch_size: Option<usize>,
    ) -> Result<TimeseriesGwResult, Error> {
        let listing_options = get_listing_options(format);

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
            params::ARROW_SCHEMA_COLUMN_NAME_TIMESTAMP
        );

        let df = ctx.sql(&select).await?;

        Ok(TimeseriesGwResult { data_frame: df })
    }

    fn datafile_url(&self, path: impl AsRef<Path>) -> Result<url::Url, Error> {
        Ok(self
            .store
            .as_ref()
            .url_schema
            .join(&path.as_ref().to_string_lossy())?)
    }
}

pub struct TimeseriesGwResult {
    data_frame: DataFrame,
}

impl TimeseriesGwResult {
    pub fn schema_with_metadata(&self, metadata: HashMap<String, String>) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            self.data_frame.schema().fields().clone(),
            metadata,
        ))
    }

    pub fn filter(self, filter: query::OntologyFilter) -> Result<Self, Error> {
        let expr = ontology_filter_to_df_expr(filter);

        let data_frame = if let Some(expr) = expr {
            trace!("filter expression: {}", expr);
            self.data_frame.filter(expr)?
        } else {
            self.data_frame
        };

        Ok(TimeseriesGwResult { data_frame })
    }

    pub async fn stream(self) -> Result<SendableRecordBatchStream, Error> {
        self.data_frame.execute_stream().await.map_err(|e| e.into())
    }

    pub async fn count(self) -> Result<usize, Error> {
        Ok(self.data_frame.count().await?)
    }
}

fn get_listing_options(_format: rw::Format) -> ListingOptions {
    ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet")
}

fn unfold_field(field: &query::OntologyField) -> Expr {
    let mut fields = field.field().split(".");
    // By construction fields needs to have at least a value
    let mut col = col(fields.next().unwrap());
    for s in fields {
        col = col.field(s);
    }
    col
}

fn ontology_filter_to_df_expr(filter: query::OntologyFilter) -> Option<Expr> {
    let mut ret: Option<Expr> = None;

    for (field, op) in filter.into_iterator() {
        let expr = match op {
            query::Op::Eq(v) => Some(unfold_field(&field).eq(value_to_df_expr(v))),
            query::Op::Neq(v) => Some(unfold_field(&field).not_eq(value_to_df_expr(v))),
            query::Op::Leq(v) => Some(unfold_field(&field).lt_eq(value_to_df_expr(v))),
            query::Op::Geq(v) => Some(unfold_field(&field).gt_eq(value_to_df_expr(v))),
            query::Op::Lt(v) => Some(unfold_field(&field).lt(value_to_df_expr(v))),
            query::Op::Gt(v) => Some(unfold_field(&field).gt(value_to_df_expr(v))),
            query::Op::Ex => None,  // no-op
            query::Op::Nex => None, // no-op
            query::Op::Between(range) => {
                let vmin: query::Value = range.min;
                let vmax: query::Value = range.max;
                let emin = unfold_field(&field).lt_eq(value_to_df_expr(vmax));
                let emax = unfold_field(&field).gt_eq(value_to_df_expr(vmin));
                Some(emin.and(emax))
            }
            query::Op::In(items) => {
                let list = items.into_iter().map(value_to_df_expr).collect();
                Some(unfold_field(&field).in_list(list, false))
            }
            query::Op::Match(v) => Some(unfold_field(&field).like(value_to_df_expr(v))),
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

fn value_to_df_expr(v: query::Value) -> Expr {
    match v {
        query::Value::Integer(v) => lit(v),
        query::Value::Float(v) => lit(v),
        query::Value::Text(v) => lit(v),
        query::Value::Boolean(v) => lit(v),
    }
}
