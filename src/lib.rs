use duckdb::{
    arrow::{
        array::{Array, RecordBatch},
        datatypes::SchemaRef,
        util::display::array_value_to_string,
    },
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use std::{
    error::Error,
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};
use url::Url;

/// DuckDB's standard vector size. A data chunk can never hold more rows than this,
/// so Arrow batches are sliced to at most this many rows in `init`.
const DUCKDB_VECTOR_SIZE: usize = 2048;

#[repr(C)]
struct HudiBindData {
    table_uri: String,
    schema: SchemaRef,
}

#[repr(C)]
struct HudiInitData {
    current_batch_idx: AtomicUsize,
    batches: Vec<RecordBatch>,
}

fn normalize_table_uri(uri: &str) -> Result<String, Box<dyn Error>> {
    if Url::parse(uri).is_ok() {
        return Ok(uri.to_owned());
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

struct HudiScanVTab;

impl VTab for HudiScanVTab {
    type InitData = HudiInitData;
    type BindData = HudiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let table_uri = normalize_table_uri(&bind.get_parameter(0).to_string())?;

        let rt = tokio::runtime::Runtime::new()?;
        let schema = rt.block_on(async {
            let hudi_table = HudiTableBuilder::from_base_uri(&table_uri).build().await?;
            let raw_schema = hudi_table.get_schema().await?;
            Ok::<_, Box<dyn std::error::Error>>(raw_schema)
        })?;

        for field in schema.fields() {
            bind.add_result_column(field.name(), LogicalTypeHandle::from(LogicalTypeId::Varchar));
        }

        Ok(HudiBindData { 
            table_uri, 
            schema: Arc::new(schema) 
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<HudiBindData>();
        
        let table_uri = unsafe { (*bind_data).table_uri.clone() };
        
        let rt = tokio::runtime::Runtime::new()?;
        let batches = rt.block_on(async {
            let hudi_table = HudiTableBuilder::from_base_uri(&table_uri).build().await?;
            
            // Create a default set of read options (no filters/projections yet)
            let options = hudi::table::ReadOptions::default();
            
            // Pass the reference to .read() as expected by the crate
            let data_batches = hudi_table.read(&options).await?; 
            
            Ok::<Vec<RecordBatch>, Box<dyn std::error::Error>>(data_batches)
        })?;

        // Drop empty batches (an empty chunk would signal end-of-scan to DuckDB while
        // real data may still follow) and slice oversized ones so every batch fits in
        // a single DuckDB data chunk. Slicing is zero-copy.
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .flat_map(|b| {
                let n = b.num_rows();
                (0..n)
                    .step_by(DUCKDB_VECTOR_SIZE)
                    .map(|offset| b.slice(offset, DUCKDB_VECTOR_SIZE.min(n - offset)))
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(HudiInitData {
            current_batch_idx: AtomicUsize::new(0),
            batches,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let idx = init_data.current_batch_idx.fetch_add(1, Ordering::Relaxed);

        if idx >= init_data.batches.len() {
            output.set_len(0);
            return Ok(());
        }

        let batch = &init_data.batches[idx];
        let num_rows = batch.num_rows();

        for (col_idx, field) in func.get_bind_data().schema.fields().iter().enumerate() {
            let arrow_col = batch.column_by_name(field.name()).ok_or_else(|| {
                format!("hudi_scan: batch is missing column {}", field.name())
            })?;
            let mut duckdb_vector = output.flat_vector(col_idx);

            for row_idx in 0..num_rows {
                if arrow_col.is_null(row_idx) {
                    duckdb_vector.set_null(row_idx);
                    continue;
                }
                let value_str = array_value_to_string(arrow_col, row_idx)?;
                duckdb_vector.insert(row_idx, value_str.as_str());
            }
        }

        // Fixed length cast type constraint
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
    con.register_table_function::<HudiScanVTab>(EXTENSION_NAME)?;
    Ok(())
}