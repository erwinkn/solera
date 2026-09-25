//! `solera_native`: native implementation of the `.kx` key index format.
//!
//! Exposes the same functions, with the same signatures, as
//! `solera/keys/_python.py`. Keys, versions and file contents cross the
//! boundary as `bytes`; flags as a `bytes` with one byte per entry.

pub mod format;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList};

use format::{Error, Options};

fn to_py(e: Error) -> PyErr {
    match e {
        Error::Format(m) | Error::Value(m) => PyValueError::new_err(m),
    }
}

fn slices(v: &[PyBackedBytes]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_ref()).collect()
}

fn list_of_bytes<'py>(py: Python<'py>, items: &[Vec<u8>]) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, items.iter().map(|b| PyBytes::new(py, b)))
}

#[pyfunction]
#[pyo3(signature = (keys, versions, deleted, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1))]
#[allow(clippy::too_many_arguments)]
fn encode_file<'py>(
    py: Python<'py>,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
    deleted: PyBackedBytes,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    codec: u8,
) -> PyResult<Bound<'py, PyBytes>> {
    let o = Options {
        block_size,
        level,
        bits_per_item,
        k,
        codec,
    };
    let out =
        format::encode_file(&slices(&keys), &slices(&versions), &deleted, o).map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

#[pyfunction]
fn decode_block<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
    codec: u8,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let (keys, versions, flags) = format::decode_block(&data, codec).map_err(to_py)?;
    Ok((
        list_of_bytes(py, &keys)?,
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &flags),
    ))
}

#[pyfunction]
fn bloom_check_keys<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &format::bloom_check_keys(&bits, nbits, k, &slices(&keys)),
    )
}

#[pyfunction]
fn bloom_check_tombstones<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &format::bloom_check_tombstones(&bits, nbits, k, &slices(&keys)),
    )
}

#[pyfunction]
fn bloom_check_pairs<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
) -> PyResult<Bound<'py, PyBytes>> {
    if keys.len() != versions.len() {
        return Err(PyValueError::new_err(
            "keys and versions must have the same length",
        ));
    }
    Ok(PyBytes::new(
        py,
        &format::bloom_check_pairs(&bits, nbits, k, &slices(&keys), &slices(&versions)),
    ))
}

#[pyfunction]
fn sort_entries<'py>(
    py: Python<'py>,
    keys: Vec<Bound<'py, PyBytes>>,
    versions: Vec<Bound<'py, PyBytes>>,
    deleted: PyBackedBytes,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let n = keys.len();
    if versions.len() != n || deleted.len() != n {
        return Err(PyValueError::new_err(
            "keys, versions and deleted must have the same length",
        ));
    }
    let key_slices: Vec<&[u8]> = keys.iter().map(|b| b.as_bytes()).collect();
    let order = format::sort_order(&key_slices).map_err(to_py)?;
    // Hand back the original bytes objects, reordered: no copies.
    let sk = PyList::new(py, order.iter().map(|&i| keys[i].clone()))?;
    let sv = PyList::new(py, order.iter().map(|&i| versions[i].clone()))?;
    let sd: Vec<u8> = order.iter().map(|&i| deleted[i]).collect();
    Ok((sk, sv, PyBytes::new(py, &sd)))
}

#[pyfunction]
#[pyo3(signature = (files, *, drop_deleted, block_size=65536, level=1, bits_per_item=14, k=10, max_file_bytes=67108864))]
#[allow(clippy::too_many_arguments)]
fn merge_files<'py>(
    py: Python<'py>,
    files: Vec<PyBackedBytes>,
    drop_deleted: bool,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    max_file_bytes: usize,
) -> PyResult<Bound<'py, PyList>> {
    let o = Options {
        block_size,
        level,
        bits_per_item,
        k,
        codec: format::CODEC_ZLIB,
    };
    let out =
        format::merge_files(&slices(&files), drop_deleted, o, max_file_bytes).map_err(to_py)?;
    list_of_bytes(py, &out)
}

type Triple<'py> = (Bound<'py, PyBytes>, Bound<'py, PyList>, Bound<'py, PyBytes>);

#[pyfunction]
fn lookup<'py>(
    py: Python<'py>,
    blocks: Vec<PyBackedBytes>,
    codec: u8,
    keys: Vec<PyBackedBytes>,
) -> PyResult<Triple<'py>> {
    let (found, versions, deleted) =
        format::lookup(&slices(&blocks), codec, &slices(&keys)).map_err(to_py)?;
    Ok((
        PyBytes::new(py, &found),
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &deleted),
    ))
}

fn run_slices(runs: &[Vec<PyBackedBytes>]) -> Vec<Vec<&[u8]>> {
    runs.iter().map(|r| slices(r)).collect()
}

#[pyfunction]
fn merge_range<'py>(
    py: Python<'py>,
    runs: Vec<Vec<PyBackedBytes>>,
    codec: u8,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    drop_deleted: bool,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let (k, v, f) = format::merge_range(
        &run_slices(&runs),
        codec,
        after.as_ref().map(|a| a.as_ref()),
        upto.as_ref().map(|u| u.as_ref()),
        drop_deleted,
    )
    .map_err(to_py)?;
    Ok((
        list_of_bytes(py, &k)?,
        list_of_bytes(py, &v)?,
        PyBytes::new(py, &f),
    ))
}

#[pyfunction]
fn replace_diff<'py>(
    py: Python<'py>,
    runs: Vec<Vec<PyBackedBytes>>,
    codec: u8,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
) -> PyResult<(
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Bound<'py, PyList>,
    u64,
)> {
    if keys.len() != versions.len() {
        return Err(PyValueError::new_err(
            "keys and versions must have the same length",
        ));
    }
    let (changed, existed, removed, live) = format::replace_diff(
        &run_slices(&runs),
        codec,
        &slices(&keys),
        &slices(&versions),
    )
    .map_err(to_py)?;
    Ok((
        PyBytes::new(py, &changed),
        PyBytes::new(py, &existed),
        list_of_bytes(py, &removed)?,
        live,
    ))
}

#[pymodule]
fn solera_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_file, m)?)?;
    m.add_function(wrap_pyfunction!(decode_block, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_keys, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_tombstones, m)?)?;
    m.add_function(wrap_pyfunction!(sort_entries, m)?)?;
    m.add_function(wrap_pyfunction!(merge_files, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(merge_range, m)?)?;
    m.add_function(wrap_pyfunction!(replace_diff, m)?)?;
    Ok(())
}
