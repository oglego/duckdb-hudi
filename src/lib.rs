use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use arrow::array::RecordBatch;
use std::{
    error::Error,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C)]
struct HudiBindData {
    table_uri: String,
    schema: arrow::datatypes::SchemaRef,
}

#[repr(C)]
struct HudiInitData {
    current_batch_idx: AtomicUsize,
    batches: Vec<RecordBatch>,
}

struct HudiScanVTab;

impl VTab for HudiScanVTab {
    type InitData = HudiInitData;
    type BindData = HudiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let table_uri = bind.get_parameter(0).to_string();

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
        
        if num_rows == 0 {
            output.set_len(0);
            return Ok(());
        }

        for col_idx in 0..batch.num_columns() {
            let arrow_col = batch.column(col_idx);
            let duckdb_vector = output.flat_vector(col_idx);

            for row_idx in 0..num_rows {
                // Extracts individual cells safely into strings using arrow_cast
                let value_str = arrow_cast::display::array_value_to_string(arrow_col, row_idx)?;
                duckdb_vector.insert(row_idx, std::ffi::CString::new(value_str)?);
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
    con.register_table_function::<HudiScanVTab>(EXTENSION_NAME)
        .expect("Failed to register hudi_scan table function");
    Ok(())
}