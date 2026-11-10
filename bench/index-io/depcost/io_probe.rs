use std::sync::OnceLock;

use object_store::ObjectStoreExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Reads one byte range through object_store (file://, s3://) on an embedded tokio runtime.
#[pyfunction]
pub fn read_range(py: Python<'_>, url: &str, options: Vec<(String, String)>, start: u64, end: u64) -> PyResult<Vec<u8>> {
    let rt = RUNTIME.get_or_init(|| tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap());
    let url = url::Url::parse(url).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    py.detach(|| {
        let (store, path) = object_store::parse_url_opts(&url, options)?;
        rt.block_on(store.get_range(&path, start..end)).map(|b| b.to_vec())
    })
    .map_err(|e: object_store::Error| PyRuntimeError::new_err(e.to_string()))
}
