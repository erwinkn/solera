//! A31 review harness: the exact df9ac17 `native/src/layers.rs`, exposed alone as `solera._native`.
use pyo3::prelude::*;
pub mod layers;

#[pymodule]
#[pyo3(name = "_native")]
fn native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    layers::register(m)
}
