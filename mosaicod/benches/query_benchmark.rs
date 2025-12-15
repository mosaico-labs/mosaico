//! Benchmarks for query and write operations
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::RecordBatch;

use mosaicod::rw::{ChunkWriter, Format};

/// Create a test RecordBatch with realistic sensor data
fn create_sensor_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new(
            "position",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        ),
        Field::new(
            "velocity",
            DataType::Struct(
                vec![
                    Field::new("vx", DataType::Float64, false),
                    Field::new("vy", DataType::Float64, false),
                    Field::new("vz", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        ),
    ]));

    // Generate data
    let timestamps: Vec<i64> = (0..num_rows as i64).collect();
    let sensor_ids: Vec<&str> = (0..num_rows).map(|i| if i % 2 == 0 { "imu_front" } else { "imu_rear" }).collect();

    let x_vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 0.1).collect();
    let y_vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 0.2).collect();
    let z_vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 0.3).collect();

    let vx_vals: Vec<f64> = (0..num_rows).map(|i| (i as f64).sin()).collect();
    let vy_vals: Vec<f64> = (0..num_rows).map(|i| (i as f64).cos()).collect();
    let vz_vals: Vec<f64> = (0..num_rows).map(|_| 0.0).collect();

    let timestamp_array: ArrayRef = Arc::new(Int64Array::from(timestamps));
    let sensor_id_array: ArrayRef = Arc::new(StringArray::from(sensor_ids));

    let x_array: ArrayRef = Arc::new(Float64Array::from(x_vals));
    let y_array: ArrayRef = Arc::new(Float64Array::from(y_vals));
    let z_array: ArrayRef = Arc::new(Float64Array::from(z_vals));
    let position_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("x", DataType::Float64, false)), x_array),
        (Arc::new(Field::new("y", DataType::Float64, false)), y_array),
        (Arc::new(Field::new("z", DataType::Float64, false)), z_array),
    ]));

    let vx_array: ArrayRef = Arc::new(Float64Array::from(vx_vals));
    let vy_array: ArrayRef = Arc::new(Float64Array::from(vy_vals));
    let vz_array: ArrayRef = Arc::new(Float64Array::from(vz_vals));
    let velocity_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("vx", DataType::Float64, false)), vx_array),
        (Arc::new(Field::new("vy", DataType::Float64, false)), vy_array),
        (Arc::new(Field::new("vz", DataType::Float64, false)), vz_array),
    ]));

    RecordBatch::try_new(
        schema,
        vec![timestamp_array, sensor_id_array, position_array, velocity_array],
    )
    .expect("Failed to create RecordBatch")
}

/// Benchmark ChunkWriter write + finalize (parquet encoding + compression)
fn bench_chunk_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_writer");

    for num_rows in [100, 1000, 10000].iter() {
        let batch = create_sensor_batch(*num_rows);
        let schema = batch.schema();

        group.bench_with_input(
            BenchmarkId::new("write_default", num_rows),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let mut writer = ChunkWriter::try_new(schema.clone(), Format::Default)
                        .expect("Failed to create writer");
                    writer.write(black_box(batch)).expect("Failed to write");
                    let (buffer, _stats) = writer.finalize().expect("Failed to finalize");
                    black_box(buffer)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_ragged_zstd", num_rows),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let mut writer = ChunkWriter::try_new(schema.clone(), Format::Ragged)
                        .expect("Failed to create writer");
                    writer.write(black_box(batch)).expect("Failed to write");
                    let (buffer, _stats) = writer.finalize().expect("Failed to finalize");
                    black_box(buffer)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark multiple batch writes (simulating streaming ingestion)
fn bench_streaming_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_writes");

    let batch = create_sensor_batch(100);
    let schema = batch.schema();

    for num_batches in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("batches", num_batches),
            num_batches,
            |b, &num_batches| {
                b.iter(|| {
                    let mut writer = ChunkWriter::try_new(schema.clone(), Format::Default)
                        .expect("Failed to create writer");
                    for _ in 0..num_batches {
                        writer.write(black_box(&batch)).expect("Failed to write");
                    }
                    let (buffer, _stats) = writer.finalize().expect("Failed to finalize");
                    black_box(buffer)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_chunk_writer, bench_streaming_writes);
criterion_main!(benches);
