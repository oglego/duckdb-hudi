use duckdb::{
    arrow::{
        array::{
            Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
            Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
            LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
            Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
            UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        },
        datatypes::{DataType, SchemaRef, TimeUnit},
    },
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use hudi::table::{
    builder::TableBuilder as HudiTableBuilder, ReadOptions, Table as HudiTable,
};
use hudi::file_group::{file_slice::FileSlice, reader::FileGroupReader};
use std::{
    error::Error,
    ffi::CString,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
};

// ---------------------------------------------------------------------------
// Global Tokio runtime — created once, reused across all VTab calls.
// Creating a new runtime per call would spin up OS thread pools on every
// bind()/init() invocation and is wasteful under concurrent DuckDB queries.
// ---------------------------------------------------------------------------
static TOKIO_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RT.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create global Tokio runtime")
    })
}

// ---------------------------------------------------------------------------
// Structs — no #[repr(C)].
//
// DuckDB's Rust vtab bindings store BindData / InitData as Box<T> behind a
// void*; the layout is entirely Rust's concern, not C's. Adding #[repr(C)]
// to structs that contain String, Vec, or Arc is meaningless and misleads
// readers into thinking they are FFI-safe.
// ---------------------------------------------------------------------------

struct HudiBindData {
    _table_uri: String,
    /// The built table is kept here so init() can reuse it instead of building
    /// a second time (which incurs a redundant metadata round-trip).
    table: Arc<HudiTable>,
    schema: SchemaRef,
}

struct HudiInitData {
    current_slice_idx: AtomicUsize,
    file_slices: Vec<FileSlice>,
    fg_reader: Arc<FileGroupReader>,
}

// ---------------------------------------------------------------------------
// Arrow → DuckDB logical type mapping
// ---------------------------------------------------------------------------

fn arrow_type_to_logical_type(dt: &DataType) -> LogicalTypeHandle {
    match dt {
        DataType::Boolean => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        DataType::Int8 => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
        DataType::Int16 => LogicalTypeHandle::from(LogicalTypeId::Smallint),
        DataType::Int32 => LogicalTypeHandle::from(LogicalTypeId::Integer),
        DataType::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        DataType::UInt8 => LogicalTypeHandle::from(LogicalTypeId::UTinyint),
        DataType::UInt16 => LogicalTypeHandle::from(LogicalTypeId::USmallint),
        DataType::UInt32 => LogicalTypeHandle::from(LogicalTypeId::UInteger),
        DataType::UInt64 => LogicalTypeHandle::from(LogicalTypeId::UBigint),
        DataType::Float32 => LogicalTypeHandle::from(LogicalTypeId::Float),
        DataType::Float64 => LogicalTypeHandle::from(LogicalTypeId::Double),
        DataType::Utf8 | DataType::LargeUtf8 => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        DataType::Binary | DataType::LargeBinary => LogicalTypeHandle::from(LogicalTypeId::Blob),
        DataType::Date32 => LogicalTypeHandle::from(LogicalTypeId::Date),
        DataType::Time32(_) | DataType::Time64(_) => LogicalTypeHandle::from(LogicalTypeId::Time),
        DataType::Timestamp(TimeUnit::Second, _) => {
            LogicalTypeHandle::from(LogicalTypeId::TimestampS)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            LogicalTypeHandle::from(LogicalTypeId::TimestampMs)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            // DuckDB TIMESTAMP stores microseconds by default.
            LogicalTypeHandle::from(LogicalTypeId::Timestamp)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            LogicalTypeHandle::from(LogicalTypeId::TimestampNs)
        }
        DataType::Decimal128(precision, scale) => {
            LogicalTypeHandle::decimal(*precision, *scale as u8)
        }
        // Everything else is returned as VARCHAR so the extension remains
        // broadly compatible. A TODO comment marks the fallback for future work.
        _ => {
            // TODO: add mappings for Date64, Duration, FixedSizeBinary, etc.
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
    }
}

// ---------------------------------------------------------------------------
// Typed column writer
//
// Writes one Arrow array column into the matching DuckDB FlatVector.
// Primitive types are written via unsafe typed slices; strings/blobs use the
// Inserter API. Null cells call set_null() on the vector.
// ---------------------------------------------------------------------------

macro_rules! write_primitive_column {
    ($arrow_array:expr, $arrow_type:ty, $rust_type:ty, $vec:expr, $nrows:expr) => {{
        let arr = $arrow_array
            .as_any()
            .downcast_ref::<$arrow_type>()
            .expect(concat!("expected ", stringify!($arrow_type)));
        let slice = unsafe { $vec.as_mut_slice::<$rust_type>() };
        for row in 0..$nrows {
            if !arr.is_null(row) {
                slice[row] = arr.value(row);
            }
        }
        for row in 0..$nrows {
            if arr.is_null(row) {
                $vec.set_null(row);
            }
        }
    }};
}

fn write_column(
    arrow_col: &dyn Array,
    vec: &mut duckdb::core::FlatVector<'_>,
    num_rows: usize,
) -> std::result::Result<(), Box<dyn Error>> {
    use duckdb::core::Inserter;

    match arrow_col.data_type() {
        DataType::Boolean => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("expected BooleanArray");
            let slice = unsafe { vec.as_mut_slice::<bool>() };
            for row in 0..num_rows {
                if !arr.is_null(row) {
                    slice[row] = arr.value(row);
                }
            }
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                }
            }
        }
        DataType::Int8 => write_primitive_column!(arrow_col, Int8Array, i8, vec, num_rows),
        DataType::Int16 => write_primitive_column!(arrow_col, Int16Array, i16, vec, num_rows),
        DataType::Int32 => write_primitive_column!(arrow_col, Int32Array, i32, vec, num_rows),
        DataType::Int64 => write_primitive_column!(arrow_col, Int64Array, i64, vec, num_rows),
        DataType::UInt8 => write_primitive_column!(arrow_col, UInt8Array, u8, vec, num_rows),
        DataType::UInt16 => write_primitive_column!(arrow_col, UInt16Array, u16, vec, num_rows),
        DataType::UInt32 => write_primitive_column!(arrow_col, UInt32Array, u32, vec, num_rows),
        DataType::UInt64 => write_primitive_column!(arrow_col, UInt64Array, u64, vec, num_rows),
        DataType::Float32 => write_primitive_column!(arrow_col, Float32Array, f32, vec, num_rows),
        DataType::Float64 => write_primitive_column!(arrow_col, Float64Array, f64, vec, num_rows),
        // Date32: days since Unix epoch — DuckDB DATE uses the same i32 representation.
        DataType::Date32 => write_primitive_column!(arrow_col, Date32Array, i32, vec, num_rows),
        // Time (seconds): DuckDB TIME stores microseconds as i64. Convert.
        DataType::Time32(TimeUnit::Second) => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .expect("expected Time32SecondArray");
            let slice = unsafe { vec.as_mut_slice::<i64>() };
            for row in 0..num_rows {
                if !arr.is_null(row) {
                    slice[row] = (arr.value(row) as i64) * 1_000_000;
                }
            }
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                }
            }
        }
        // Time (milliseconds): DuckDB TIME is microseconds.
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .expect("expected Time32MillisecondArray");
            let slice = unsafe { vec.as_mut_slice::<i64>() };
            for row in 0..num_rows {
                if !arr.is_null(row) {
                    slice[row] = (arr.value(row) as i64) * 1_000;
                }
            }
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                }
            }
        }
        // Time (microseconds): direct.
        DataType::Time64(TimeUnit::Microsecond) => {
            write_primitive_column!(arrow_col, Time64MicrosecondArray, i64, vec, num_rows)
        }
        // Time (nanoseconds): DuckDB TIME is microseconds — truncate.
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("expected Time64NanosecondArray");
            let slice = unsafe { vec.as_mut_slice::<i64>() };
            for row in 0..num_rows {
                if !arr.is_null(row) {
                    slice[row] = arr.value(row) / 1_000;
                }
            }
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                }
            }
        }
        // Timestamps: DuckDB stores as i64 in various precisions.
        DataType::Timestamp(TimeUnit::Second, _) => {
            write_primitive_column!(arrow_col, TimestampSecondArray, i64, vec, num_rows)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            write_primitive_column!(arrow_col, TimestampMillisecondArray, i64, vec, num_rows)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            write_primitive_column!(arrow_col, TimestampMicrosecondArray, i64, vec, num_rows)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            write_primitive_column!(arrow_col, TimestampNanosecondArray, i64, vec, num_rows)
        }
        // Decimal128: DuckDB DECIMAL stores the raw i128 value.
        DataType::Decimal128(_, _) => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("expected Decimal128Array");
            let slice = unsafe { vec.as_mut_slice::<i128>() };
            for row in 0..num_rows {
                if !arr.is_null(row) {
                    slice[row] = arr.value(row);
                }
            }
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                }
            }
        }
        // Strings and binary: use the Inserter API (handles UTF-8 and arbitrary bytes).
        DataType::Utf8 => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray");
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                } else {
                    vec.insert(row, arr.value(row));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("expected LargeStringArray");
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                } else {
                    vec.insert(row, arr.value(row));
                }
            }
        }
        DataType::Binary => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("expected BinaryArray");
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                } else {
                    vec.insert(row, arr.value(row));
                }
            }
        }
        DataType::LargeBinary => {
            let arr = arrow_col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("expected LargeBinaryArray");
            for row in 0..num_rows {
                if arr.is_null(row) {
                    vec.set_null(row);
                } else {
                    vec.insert(row, arr.value(row));
                }
            }
        }
        // Fallback: stringify via arrow-cast for any type not handled above.
        _ => {
            for row in 0..num_rows {
                let s = format!("{:?}", arrow_col);
                vec.insert(row, CString::new(s)?);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// VTab implementation
// ---------------------------------------------------------------------------

struct HudiScanVTab;

impl VTab for HudiScanVTab {
    type InitData = HudiInitData;
    type BindData = HudiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let table_uri = bind.get_parameter(0).to_string();

        // Build the Hudi table and fetch the schema in a single async block so
        // the table object is available for reuse in init().
        let (table, schema) = get_runtime().block_on(async {
            let hudi_table = HudiTableBuilder::from_base_uri(&table_uri).build().await?;
            let raw_schema = hudi_table.get_schema().await?;
            Ok::<_, Box<dyn std::error::Error>>((hudi_table, raw_schema))
        })?;

        // Register columns with their real DuckDB logical types.
        for field in schema.fields() {
            let logical_type = arrow_type_to_logical_type(field.data_type());
            bind.add_result_column(field.name(), logical_type);
        }

        Ok(HudiBindData {
            _table_uri: table_uri,
            table: Arc::new(table),
            schema: Arc::new(schema),
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<HudiBindData>();

        // Re-use the table that bind() already built — no second metadata I/O.
        let (table, schema) = unsafe {
            ((*bind_data).table.clone(), (*bind_data).schema.clone())
        };

        let (file_slices, fg_reader) = get_runtime().block_on(async {
            let options = ReadOptions::default();
            let slices = table.get_file_slices(&options).await?;
            let reader = table
                .create_file_group_reader_with_options(
                    Some(&options),
                    std::iter::empty::<(&str, &str)>(),
                )
                .await?;
            Ok::<_, Box<dyn std::error::Error>>((slices, reader))
        })?;

        // schema is kept alive via Arc inside HudiBindData; we don't need it
        // directly in InitData since the batch carries its own schema.
        drop(schema);

        Ok(HudiInitData {
            current_slice_idx: AtomicUsize::new(0),
            file_slices,
            fg_reader: Arc::new(fg_reader),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("[hudi_scan] func entered");
        let init_data = func.get_init_data();

        // SeqCst ordering ensures cross-thread visibility when DuckDB calls
        // func() from parallel scanner threads.
        let idx = init_data
            .current_slice_idx
            .fetch_add(1, Ordering::SeqCst);

        if idx >= init_data.file_slices.len() {
            // Signal to DuckDB that this scan is complete.
            output.set_len(0);
            return Ok(());
        }

        let file_slice = &init_data.file_slices[idx];
        let fg_reader = init_data.fg_reader.clone();
        let options = ReadOptions::default();

        // Read one file slice at a time: memory footprint is bounded to a
        // single batch rather than the entire table.
        let batch = get_runtime().block_on(async {
            fg_reader
                .read_file_slice(file_slice, &options)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error>)
        })?;

        let num_rows = batch.num_rows();
        eprintln!("[hudi_scan] batch rows={} cols={}", num_rows, batch.num_columns());
        if num_rows == 0 {
            output.set_len(0);
            return Ok(());
        }

        for col_idx in 0..batch.num_columns() {
            let arrow_col = batch.column(col_idx);
            eprintln!("[hudi_scan] writing col {} type {:?}", col_idx, arrow_col.data_type());
            let mut duckdb_vector = output.flat_vector(col_idx);
            write_column(arrow_col.as_ref(), &mut duckdb_vector, num_rows)?;
        }

        // Tell DuckDB how many rows were written to this output chunk.
        output.set_len(num_rows);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

const EXTENSION_NAME: &str = "hudi_scan";

#[duckdb_entrypoint_c_api(ext_name = "duckdb_hudi")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<HudiScanVTab>(EXTENSION_NAME)
        .expect("Failed to register hudi_scan table function");
    Ok(())
}