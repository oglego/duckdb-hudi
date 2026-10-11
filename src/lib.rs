use duckdb::{
    arrow::{
        array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch},
        compute::cast,
        datatypes::{
            ArrowPrimitiveType, DataType, Date32Type, Float32Type, Float64Type, Int16Type,
            Int32Type, Int64Type, Int8Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
            UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
        util::display::array_value_to_string,
    },
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use futures::{stream::BoxStream, StreamExt};
use hudi::table::{builder::TableBuilder as HudiTableBuilder, ReadOptions, Table as HudiTable};
use std::{
    error::Error,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::runtime::Runtime;
use url::Url;

const EXTENSION_NAME: &str = "hudi_scan";

/// DuckDB's standard vector size. A data chunk can never hold more rows than this,
/// so Arrow batches are sliced to at most this many rows before being emitted.
const DUCKDB_VECTOR_SIZE: usize = 2048;

/// Shared tokio runtime. Creating a runtime per `bind`/`init` call is expensive and
/// spawns a fresh worker pool every time.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> Result<&'static Runtime, Box<dyn Error>> {
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }
    let rt = Runtime::new()?;
    Ok(RUNTIME.get_or_init(|| rt))
}

/// How an Arrow column is copied into a DuckDB vector.
#[derive(Clone, Copy, Debug)]
enum ColKind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Date32,
    /// Any Arrow timestamp without a timezone (normalised to microseconds).
    Timestamp,
    /// Any Arrow timestamp with a timezone (normalised to microseconds, UTC).
    TimestampTz,
    Utf8,
    /// Unsupported / nested type: rendered through Arrow's display formatting as VARCHAR.
    Varchar,
}

fn map_type(dt: &DataType) -> (ColKind, LogicalTypeId) {
    match dt {
        DataType::Boolean => (ColKind::Bool, LogicalTypeId::Boolean),
        DataType::Int8 => (ColKind::I8, LogicalTypeId::Tinyint),
        DataType::Int16 => (ColKind::I16, LogicalTypeId::Smallint),
        DataType::Int32 => (ColKind::I32, LogicalTypeId::Integer),
        DataType::Int64 => (ColKind::I64, LogicalTypeId::Bigint),
        DataType::UInt8 => (ColKind::U8, LogicalTypeId::UTinyint),
        DataType::UInt16 => (ColKind::U16, LogicalTypeId::USmallint),
        DataType::UInt32 => (ColKind::U32, LogicalTypeId::UInteger),
        DataType::UInt64 => (ColKind::U64, LogicalTypeId::UBigint),
        DataType::Float32 => (ColKind::F32, LogicalTypeId::Float),
        DataType::Float64 => (ColKind::F64, LogicalTypeId::Double),
        DataType::Date32 => (ColKind::Date32, LogicalTypeId::Date),
        DataType::Timestamp(_, None) => (ColKind::Timestamp, LogicalTypeId::Timestamp),
        DataType::Timestamp(_, Some(_)) => (ColKind::TimestampTz, LogicalTypeId::TimestampTZ),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            (ColKind::Utf8, LogicalTypeId::Varchar)
        }
        _ => (ColKind::Varchar, LogicalTypeId::Varchar),
    }
}

struct HudiBindData {
    table: Arc<HudiTable>,
    schema: SchemaRef,
    /// One entry per schema field, computed once in `bind`.
    kinds: Vec<ColKind>,
}

struct ScanState {
    stream: BoxStream<'static, Result<RecordBatch, String>>,
    /// A batch that is larger than one DuckDB vector, and the offset of its next slice.
    pending: Option<(RecordBatch, usize)>,
}

struct HudiInitData {
    state: Mutex<ScanState>,
    /// Schema field indices to emit, in output order (from projection pushdown).
    projection: Vec<usize>,
}

fn normalize_table_uri(uri: &str) -> Result<String, Box<dyn Error>> {
    // A single-character "scheme" is a Windows drive letter (`C:\data\t`), not a URL.
    if let Ok(url) = Url::parse(uri) {
        if url.scheme().len() > 1 {
            return Ok(uri.to_owned());
        }
    }

    let path = Path::new(uri);
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    Url::from_file_path(absolute_path)
        .map(|url| url.to_string())
        .map_err(|_| format!("hudi_scan: invalid table path: {uri}").into())
}

/// Copies a primitive Arrow array straight into a DuckDB vector.
fn copy_primitive<T: ArrowPrimitiveType>(col: &dyn Array, out: &mut FlatVector) {
    let arr = col.as_primitive::<T>();
    let len = arr.len();
    {
        // SAFETY: `T::Native` has the same layout as the DuckDB physical type selected
        // in `map_type`, and `len <= DUCKDB_VECTOR_SIZE` is the vector capacity.
        let dst = unsafe { out.as_mut_slice_with_len::<T::Native>(len) };
        dst.copy_from_slice(&arr.values()[..]);
    }
    if arr.null_count() > 0 {
        for i in 0..len {
            if arr.is_null(i) {
                out.set_null(i);
            }
        }
    }
}

fn write_column(kind: ColKind, col: &ArrayRef, out: &mut FlatVector) -> Result<(), Box<dyn Error>> {
    let len = col.len();
    match kind {
        ColKind::Bool => {
            let arr: &BooleanArray = col.as_boolean();
            {
                // SAFETY: DuckDB BOOLEAN is a one-byte bool; `len` fits the vector.
                let dst = unsafe { out.as_mut_slice_with_len::<bool>(len) };
                for (i, slot) in dst.iter_mut().enumerate() {
                    *slot = arr.is_valid(i) && arr.value(i);
                }
            }
            for i in 0..len {
                if arr.is_null(i) {
                    out.set_null(i);
                }
            }
        }
        ColKind::I8 => copy_primitive::<Int8Type>(col.as_ref(), out),
        ColKind::I16 => copy_primitive::<Int16Type>(col.as_ref(), out),
        ColKind::I32 => copy_primitive::<Int32Type>(col.as_ref(), out),
        ColKind::I64 => copy_primitive::<Int64Type>(col.as_ref(), out),
        ColKind::U8 => copy_primitive::<UInt8Type>(col.as_ref(), out),
        ColKind::U16 => copy_primitive::<UInt16Type>(col.as_ref(), out),
        ColKind::U32 => copy_primitive::<UInt32Type>(col.as_ref(), out),
        ColKind::U64 => copy_primitive::<UInt64Type>(col.as_ref(), out),
        ColKind::F32 => copy_primitive::<Float32Type>(col.as_ref(), out),
        ColKind::F64 => copy_primitive::<Float64Type>(col.as_ref(), out),
        ColKind::Date32 => copy_primitive::<Date32Type>(col.as_ref(), out),
        ColKind::Timestamp | ColKind::TimestampTz => {
            let tz = match col.data_type() {
                DataType::Timestamp(_, tz) => tz.clone(),
                other => return Err(format!("hudi_scan: expected timestamp, got {other}").into()),
            };
            let micros = cast(col, &DataType::Timestamp(TimeUnit::Microsecond, tz))?;
            copy_primitive::<TimestampMicrosecondType>(micros.as_ref(), out);
        }
        ColKind::Utf8 => {
            let utf8 = cast(col, &DataType::Utf8)?;
            let arr = utf8.as_string::<i32>();
            for i in 0..len {
                if arr.is_null(i) {
                    out.set_null(i);
                } else {
                    out.insert(i, arr.value(i));
                }
            }
        }
        ColKind::Varchar => {
            for i in 0..len {
                if col.is_null(i) {
                    out.set_null(i);
                } else {
                    let s = array_value_to_string(col, i)?;
                    out.insert(i, s.as_str());
                }
            }
        }
    }
    Ok(())
}

struct HudiScanVTab;

impl VTab for HudiScanVTab {
    type InitData = HudiInitData;
    type BindData = HudiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let table_uri = normalize_table_uri(&bind.get_parameter(0).to_string())?;

        let rt = runtime()?;
        let (table, schema) = rt.block_on(async {
            let table = HudiTableBuilder::from_base_uri(&table_uri).build().await?;
            let schema = table.get_schema().await?;
            Ok::<_, Box<dyn Error>>((table, schema))
        })?;

        let mut kinds = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let (kind, type_id) = map_type(field.data_type());
            bind.add_result_column(field.name(), LogicalTypeHandle::from(type_id));
            kinds.push(kind);
        }

        Ok(HudiBindData {
            table: Arc::new(table),
            schema: Arc::new(schema),
            kinds,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        // SAFETY: DuckDB guarantees the bind data set in `bind` outlives `init`.
        let bind_data = unsafe { &*info.get_bind_data::<HudiBindData>() };
        let fields = bind_data.schema.fields();

        // Projection pushdown: DuckDB tells us which columns the query needs, and the
        // output chunk contains only those columns, in this order.
        let projection: Vec<usize> = info
            .get_column_indices()
            .into_iter()
            .map(|i| i as usize)
            .collect();
        if let Some(bad) = projection.iter().find(|&&i| i >= fields.len()) {
            return Err(format!("hudi_scan: unexpected projected column index {bad}").into());
        }

        let mut options = ReadOptions::new().with_batch_size(DUCKDB_VECTOR_SIZE)?;
        let is_identity =
            projection.len() == fields.len() && projection.iter().enumerate().all(|(a, &b)| a == b);
        if !is_identity {
            // With no columns needed (e.g. `count(*)`), still read one cheap column so
            // row counts are preserved.
            let names: Vec<String> = if projection.is_empty() {
                fields.iter().take(1).map(|f| f.name().clone()).collect()
            } else {
                projection
                    .iter()
                    .map(|&i| fields[i].name().clone())
                    .collect()
            };
            options = options.with_projection(names);
        }

        let table = Arc::clone(&bind_data.table);
        let stream = runtime()?.block_on(async move { table.read_stream(&options).await })?;
        let stream = stream.map(|r| r.map_err(|e| e.to_string())).boxed();

        Ok(HudiInitData {
            state: Mutex::new(ScanState {
                stream,
                pending: None,
            }),
            projection,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        let mut state = init_data
            .state
            .lock()
            .map_err(|_| "hudi_scan: scan state mutex poisoned")?;

        // Pull the next non-empty batch (empty chunks would signal end-of-scan to
        // DuckDB while real data may still follow) and cap it at one vector.
        let batch = loop {
            if let Some((batch, offset)) = state.pending.take() {
                let n = DUCKDB_VECTOR_SIZE.min(batch.num_rows() - offset);
                let slice = batch.slice(offset, n); // zero-copy
                if offset + n < batch.num_rows() {
                    state.pending = Some((batch, offset + n));
                }
                break Some(slice);
            }
            match runtime()?.block_on(state.stream.next()) {
                Some(Ok(b)) => {
                    if b.num_rows() > 0 {
                        state.pending = Some((b, 0));
                    }
                }
                Some(Err(e)) => return Err(format!("hudi_scan: read failed: {e}").into()),
                None => break None,
            }
        };

        let Some(batch) = batch else {
            output.set_len(0);
            return Ok(());
        };

        let fields = bind_data.schema.fields();
        for (out_idx, &field_idx) in init_data.projection.iter().enumerate() {
            let name = fields[field_idx].name();
            let col = batch
                .column_by_name(name)
                .ok_or_else(|| format!("hudi_scan: batch is missing column {name}"))?;
            let mut vector = output.flat_vector(out_idx);
            write_column(bind_data.kinds[field_idx], col, &mut vector)?;
        }

        output.set_len(batch.num_rows());
        Ok(())
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api(ext_name = "duckdb_hudi")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<HudiScanVTab>(EXTENSION_NAME)?;
    Ok(())
}
