//! `solera._native`: the `.kx` key index format and the per-key work on it.
//!
//! Two kinds of functions. Kernels over byte strings the caller holds —
//! encoding, decoding, filter checks, lookups, bounded scans — mirror
//! `solera/keys/_python.py`, the format's reference. Jobs stream over a whole
//! index — a full replacement, a compaction, a recount: a `Job` asks for the
//! file segments it needs and hands back the files it writes, and Python does
//! the I/O in between. Keys, versions and file contents cross the boundary
//! as `bytes`; flags as a `bytes` with one byte per entry.

pub mod arrow;
pub mod digest;
pub mod format;
pub mod garbage;
pub mod jobs;
pub mod local;
mod pyvalue;
pub mod rows;
pub mod sort;
pub mod stream;

use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::RecordBatch;
use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBool, PyBytes, PyCapsule, PyDict, PyInt, PyList, PyString};

use format::{Error, Options};
use jobs::{Compact, Count, Patch, Replace, Step};
use rows::{Arena, Constant, Source, Stream, Table, Versions};
use stream::Segment;

create_exception!(
    _native,
    FormatError,
    PyValueError,
    "A key index file is malformed or fails a checksum."
);

fn to_py(e: Error) -> PyErr {
    match e {
        Error::Format(m) => FormatError::new_err(m),
        Error::Value(m) => PyValueError::new_err(m),
        Error::Callback(e) => match e.downcast::<PyErr>() {
            Ok(e) => *e,
            Err(e) => PyValueError::new_err(e.to_string()),
        },
    }
}

fn slices(v: &[PyBackedBytes]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_ref()).collect()
}

fn list_of_bytes<'py>(py: Python<'py>, items: &[Vec<u8>]) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, items.iter().map(|b| PyBytes::new(py, b)))
}

fn arena_list<'py>(py: Python<'py>, a: &Arena) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, (0..a.len()).map(|i| PyBytes::new(py, a.get(i))))
}

fn options(block_size: usize, level: u32, bits_per_item: u64, k: u8, codec: u8) -> Options {
    Options {
        block_size,
        level,
        bits_per_item,
        k,
        codec,
    }
}

// -- kernels ----------------------------------------------------------------------------

/// Optional per-entry columns: locators (0 when absent), and the predecessor
/// `(version, locator)` of a delta entry's key, or None.
type Extra = (Vec<u64>, Vec<Option<(PyBackedBytes, u64)>>);

fn extra(
    n: usize,
    locators: Option<Vec<u64>>,
    predecessors: Option<Vec<Option<(PyBackedBytes, u64)>>>,
) -> PyResult<Extra> {
    let locators = locators.unwrap_or_else(|| vec![0; n]);
    let predecessors = predecessors.unwrap_or_else(|| (0..n).map(|_| None).collect());
    if locators.len() != n || predecessors.len() != n {
        return Err(PyValueError::new_err(
            "locators and predecessors must have one item per key",
        ));
    }
    Ok((locators, predecessors))
}

fn prev(p: &Option<(PyBackedBytes, u64)>) -> format::Predecessor<'_> {
    p.as_ref().map(|(v, l)| (v.as_ref(), *l))
}

#[pyfunction]
#[pyo3(signature = (keys, versions, deleted, *, locators=None, predecessors=None, block_size=65536, level=1, bits_per_item=14, k=10, codec=1))]
#[allow(clippy::too_many_arguments)]
fn encode_file<'py>(
    py: Python<'py>,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
    deleted: PyBackedBytes,
    locators: Option<Vec<u64>>,
    predecessors: Option<Vec<Option<(PyBackedBytes, u64)>>>,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    codec: u8,
) -> PyResult<Bound<'py, PyBytes>> {
    let o = options(block_size, level, bits_per_item, k, codec);
    let (locators, predecessors) = extra(keys.len(), locators, predecessors)?;
    let predecessors: Vec<format::Predecessor> = predecessors.iter().map(prev).collect();
    let out = format::encode_file(
        &slices(&keys),
        &slices(&versions),
        &deleted,
        &locators,
        &predecessors,
        o,
    )
    .map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// Entries (sorted, unique keys) as files of about `max_file_bytes` each.
#[pyfunction]
#[pyo3(signature = (keys, versions, deleted, *, locators=None, predecessors=None, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
#[allow(clippy::too_many_arguments)]
fn write_files<'py>(
    py: Python<'py>,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
    deleted: PyBackedBytes,
    locators: Option<Vec<u64>>,
    predecessors: Option<Vec<Option<(PyBackedBytes, u64)>>>,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    codec: u8,
    max_file_bytes: usize,
) -> PyResult<Bound<'py, PyList>> {
    if versions.len() != keys.len() || deleted.len() != keys.len() {
        return Err(PyValueError::new_err(
            "keys, versions and deleted must have the same length",
        ));
    }
    let (locators, predecessors) = extra(keys.len(), locators, predecessors)?;
    let o = options(block_size, level, bits_per_item, k, codec);
    let files = py
        .detach(|| {
            let mut w = stream::Writer::new(o, max_file_bytes);
            for i in 0..keys.len() {
                w.push(
                    &keys[i],
                    &versions[i],
                    deleted[i] != 0,
                    locators[i],
                    prev(&predecessors[i]),
                )?;
            }
            w.finish(false)?;
            Ok(w.files.into_iter().collect::<Vec<_>>())
        })
        .map_err(to_py)?;
    list_of_bytes(py, &files)
}

type Block5<'py> = (
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyBytes>,
    Vec<u64>,
    Bound<'py, PyList>,
);

/// A block's keys, versions, deleted flags, locators, and each entry's predecessor `(version, locator)` or None.
#[pyfunction]
fn decode_block<'py>(py: Python<'py>, data: PyBackedBytes, codec: u8) -> PyResult<Block5<'py>> {
    let (keys, versions, flags, locators, prev) =
        format::decode_block(&data, codec).map_err(to_py)?;
    let predecessors = PyList::empty(py);
    for p in prev {
        match p {
            Some((v, l)) => predecessors.append((PyBytes::new(py, &v), l))?,
            None => predecessors.append(py.None())?,
        }
    }
    Ok((
        list_of_bytes(py, &keys)?,
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &flags),
        locators,
        predecessors,
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

type Found<'py> = (
    Bound<'py, PyBytes>,
    Bound<'py, PyList>,
    Bound<'py, PyBytes>,
    Vec<u64>,
);

/// Per sorted key: found, version, deleted, locator.
#[pyfunction]
fn lookup<'py>(
    py: Python<'py>,
    blocks: Vec<PyBackedBytes>,
    codec: u8,
    keys: Vec<PyBackedBytes>,
) -> PyResult<Found<'py>> {
    let (found, versions, deleted, locators) =
        format::lookup(&slices(&blocks), codec, &slices(&keys)).map_err(to_py)?;
    Ok((
        PyBytes::new(py, &found),
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &deleted),
        locators,
    ))
}

type Merged<'py> = (
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyBytes>,
    Vec<u64>,
);

/// The merged view's keys, versions, deleted flags and locators.
#[pyfunction]
fn merge_range<'py>(
    py: Python<'py>,
    runs: Vec<Vec<PyBackedBytes>>,
    codec: u8,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    drop_deleted: bool,
) -> PyResult<Merged<'py>> {
    let runs: Vec<Vec<&[u8]>> = runs.iter().map(|r| slices(r)).collect();
    let (k, v, f, l) = format::merge_range(
        &runs,
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
        l,
    ))
}

/// Every entry of a garbage file (docs/key-index-format.md § Garbage files):
/// keys, versions, locators.
#[pyfunction]
fn decode_garbage<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Merged<'py>> {
    let (k, v, l) = garbage::decode(data).map_err(to_py)?;
    Ok((
        list_of_bytes(py, &k)?,
        list_of_bytes(py, &v)?,
        PyBytes::new(py, &vec![0u8; l.len()]),
        l,
    ))
}

// -- tails ----------------------------------------------------------------------------

#[pyfunction]
fn filter_nbits(items: u64, bits_per_item: u64) -> u64 {
    format::filter_nbits(items, bits_per_item)
}

fn footer_dict<'py>(py: Python<'py>, f: &format::Footer) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("codec", f.codec)?;
    d.set_item("entries", f.entries)?;
    d.set_item("filters_offset", f.filters_offset)?;
    d.set_item("filters_length", f.filters_length)?;
    d.set_item("index_offset", f.index_offset)?;
    d.set_item("index_length", f.index_length)?;
    d.set_item("index_crc", f.index_crc)?;
    Ok(d)
}

#[pyfunction]
fn parse_footer<'py>(py: Python<'py>, footer: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    footer_dict(py, &format::parse_footer(footer).map_err(to_py)?)
}

/// A file's block index from its last bytes (`part` ends at `file_size`).
#[pyfunction]
fn parse_index<'py>(py: Python<'py>, part: &[u8], file_size: u64) -> PyResult<Bound<'py, PyDict>> {
    let t = format::parse_index(part, file_size).map_err(to_py)?;
    let d = footer_dict(py, &t.footer)?;
    d.set_item("size", file_size)?;
    d.set_item("min_key", PyBytes::new(py, &t.min_key))?;
    d.set_item("max_key", PyBytes::new(py, &t.max_key))?;
    let blocks = PyList::empty(py);
    for (first, off, size, n, crc) in &t.blocks {
        blocks.append((PyBytes::new(py, first), off, size, n, crc))?;
    }
    d.set_item("blocks", blocks)?;
    Ok(d)
}

/// A file's tail — filters, index and footer (`tail` ends at `file_size`).
#[pyfunction]
fn parse_tail<'py>(py: Python<'py>, tail: &[u8], file_size: u64) -> PyResult<Bound<'py, PyDict>> {
    let d = parse_index(py, tail, file_size)?;
    let filters = format::parse_filters(tail, file_size).map_err(to_py)?;
    for (name, (nbits, k, bits)) in ["key_filter", "pair_filter", "tomb_filter"]
        .into_iter()
        .zip(filters)
    {
        d.set_item(name, (nbits, k, PyBytes::new(py, bits)))?;
    }
    Ok(d)
}

#[pyfunction]
fn check_block(data: &[u8], crc: u32) -> PyResult<()> {
    if crc32fast::hash(data) != crc {
        return Err(FormatError::new_err("block checksum mismatch"));
    }
    Ok(())
}

// -- written content ------------------------------------------------------------------

/// Takes a whole Arrow C stream (`__arrow_c_stream__`), without the GIL: a
/// producer may need it on another thread.
fn arrow_batches(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let capsule = obj
        .call_method1("__arrow_c_stream__", (py.None(),))?
        .cast_into::<PyCapsule>()
        .map_err(|_| PyTypeError::new_err("__arrow_c_stream__ must return a PyCapsule"))?;
    let ptr = capsule.pointer_checked(Some(c"arrow_array_stream"))?;
    // SAFETY: the capsule holds an ArrowArrayStream; taking it leaves a released one behind.
    let stream =
        unsafe { FFI_ArrowArrayStream::from_raw(ptr.as_ptr() as *mut FFI_ArrowArrayStream) };
    struct Reader(ArrowArrayStreamReader);
    // SAFETY: the Arrow C stream interface lets a consumer pull from any thread.
    unsafe impl Send for Reader {}
    let reader = Reader(
        ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PyValueError::new_err(e.to_string()))?,
    );
    py.detach(move || reader.0.collect::<Result<Vec<_>, _>>())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

fn key_of(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        out.extend_from_slice(b.as_bytes());
        return Ok(());
    }
    let s = match obj.cast::<PyString>() {
        Ok(s) => s.clone(),
        Err(_) => obj.str()?,
    };
    match s.to_str() {
        Ok(t) => out.extend_from_slice(t.as_bytes()),
        // Lone surrogates: as `key_bytes` encodes them.
        Err(_) => out.extend_from_slice(
            s.call_method1("encode", ("utf-8", "surrogateescape"))?
                .cast::<PyBytes>()?
                .as_bytes(),
        ),
    }
    Ok(())
}

/// A row's key, by the one rule rows and stores share (`solera.stores.key_text`):
/// a `str`, UTF-8 encoded, or an `int` (not a `bool`) as its decimal text.
fn row_key(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if obj.cast::<PyString>().is_ok()
        || (obj.cast::<PyInt>().is_ok() && obj.cast::<PyBool>().is_err())
    {
        return key_of(obj, out);
    }
    Err(PyValueError::new_err(format!(
        "a key must be a str or an int, not {}",
        obj.get_type().name()?
    )))
}

/// How rows are read: their key column, their revision column if declared,
/// and the columns their digest leaves out — the key, and any the store adds
/// itself (a partition column), so a row digests the same wherever it is read.
struct Records {
    key: String,
    revision: Option<String>,
    skip: Vec<String>,
}

impl Records {
    fn new(key: &str, revision: Option<&str>, exclude: Vec<String>) -> Records {
        let mut skip = vec![key.to_string()];
        skip.extend(exclude);
        Records {
            key: key.into(),
            revision: revision.map(Into::into),
            skip,
        }
    }

    /// The key and revision columns' names, as Python strings made once.
    fn names<'py>(&self, py: Python<'py>) -> Names<'py> {
        (
            PyString::new(py, &self.key),
            self.revision.as_deref().map(|r| PyString::new(py, r)),
        )
    }

    /// Appends a mapping row's key, and its version: its digest, or its revision's text.
    fn read<'py>(
        &self,
        w: &mut pyvalue::Walker<'py>,
        names: &Names<'py>,
        row: &Bound<'py, PyAny>,
        keys: Option<&mut Vec<u8>>,
        version: &mut Vec<u8>,
    ) -> PyResult<()> {
        if let Some(keys) = keys {
            let k = field(row, &names.0)?.ok_or_else(|| PyKeyError::new_err(self.key.clone()))?;
            row_key(&k, keys)?;
        }
        match (&self.revision, &names.1) {
            (Some(rev), Some(name)) => {
                let v = field(row, name)?.ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "a row lacks the declared revision field {rev:?}"
                    ))
                })?;
                w.render(&v, version)?;
            }
            _ => version.extend_from_slice(&w.row(row, &self.skip)?),
        }
        Ok(())
    }
}

type Names<'py> = (Bound<'py, PyString>, Option<Bound<'py, PyString>>);

/// Versions read with the keys, in the rows' own order — the order they lie
/// in memory, where reading them in key order would miss the cache at every
/// row — and handed out in key order.
struct Read {
    versions: Arena,
    rows: bool,
}

impl Versions for Read {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> format::Result<()> {
        for &r in rows {
            out.push(self.versions.get(r as usize));
        }
        Ok(())
    }

    fn rows(&self) -> bool {
        self.rows
    }
}

/// `(key, …)` pairs' versions, a window of rows at a time under the GIL.
enum PyVersions {
    /// `(key, value)` pairs: `value(v)`.
    Values(Py<PyList>),
    /// `(key, version)` pairs: the version as given.
    Pairs(Py<PyList>),
}

impl PyVersions {
    fn fill_py(&self, py: Python<'_>, rows: &[u32], out: &mut Arena) -> PyResult<()> {
        let mut w = pyvalue::Walker::new(py)?;
        let mut buf = Vec::new();
        match self {
            PyVersions::Values(list) => {
                let list = list.bind(py);
                for &r in rows {
                    buf.clear();
                    w.value(&list.get_item(r as usize)?.get_item(1)?, &mut buf, 0)?;
                    out.push(&digest::value(&buf));
                }
            }
            PyVersions::Pairs(list) => {
                let list = list.bind(py);
                for &r in rows {
                    buf.clear();
                    key_of(&list.get_item(r as usize)?.get_item(1)?, &mut buf)?;
                    out.push(&buf);
                }
            }
        }
        Ok(())
    }
}

impl Versions for PyVersions {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> format::Result<()> {
        Python::attach(|py| self.fill_py(py, rows, out)).map_err(|e| Error::Callback(Box::new(e)))
    }
}

/// `row[name]` of a mapping row, None when it has no such field.
fn field<'py>(
    row: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match row.cast::<PyDict>() {
        Ok(d) => d.get_item(name),
        Err(_) => match row.get_item(name) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.is_instance_of::<PyKeyError>(row.py()) => Ok(None),
            Err(e) => Err(e),
        },
    }
}

/// Packs each item's key once: `key(item)` as its `str`, UTF-8 encoded.
fn pack<'py>(
    py: Python<'py>,
    items: &Bound<'py, PyList>,
    key: impl Fn(&Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>>,
) -> PyResult<Arena> {
    let mut keys = Arena::default();
    keys.ends.reserve(items.len());
    for (i, item) in items.iter().enumerate() {
        key_of(&key(&item)?, &mut keys.data)?;
        keys.ends.push(keys.data.len());
        if i % 65536 == 65535 {
            py.detach(|| ()); // let other threads run: this loop holds the GIL
        }
    }
    keys.data.shrink_to_fit();
    Ok(keys)
}

/// A keyed write's content, sorted by key (unless it arrived sorted) and
/// read a key at a time: every key is the group of rows that carry it, and
/// its version is computed as the reader reaches it (docs/row-digest.md).
#[pyclass(module = "solera._native")]
struct Rows {
    table: Option<Table>,
    len: usize,
    presorted: bool,
}

impl Rows {
    fn new(
        py: Python<'_>,
        keys: Box<dyn sort::Keys + Send>,
        versions: Box<dyn Versions>,
    ) -> PyResult<Rows> {
        let table = py.detach(|| Table::new(keys, versions)).map_err(to_py)?;
        Ok(Rows {
            len: table.len(),
            presorted: table.presorted(),
            table: Some(table),
        })
    }
}

#[pymethods]
impl Rows {
    /// Rows as mappings: the key is `row[key]`, a `str` or an `int`; the
    /// version is the group of the key's row digests, without the key column
    /// and the `exclude`d ones, or with `revision` that column's text, which
    /// the key's rows must share.
    #[staticmethod]
    #[pyo3(signature = (rows, key, revision=None, exclude=vec![]))]
    fn records(
        py: Python<'_>,
        rows: Bound<'_, PyList>,
        key: &str,
        revision: Option<&str>,
        exclude: Vec<String>,
    ) -> PyResult<Rows> {
        let records = Records::new(key, revision, exclude);
        let (mut w, names) = (pyvalue::Walker::new(py)?, records.names(py));
        let (mut keys, mut versions) = (Arena::default(), Arena::default());
        keys.ends.reserve(rows.len());
        versions.ends.reserve(rows.len());
        for (i, row) in rows.iter().enumerate() {
            records.read(
                &mut w,
                &names,
                &row,
                Some(&mut keys.data),
                &mut versions.data,
            )?;
            keys.ends.push(keys.data.len());
            versions.ends.push(versions.data.len());
            if i % 65536 == 65535 {
                py.detach(|| ()); // let other threads run: this loop holds the GIL
            }
        }
        keys.data.shrink_to_fit();
        versions.data.shrink_to_fit();
        let versions = Read {
            versions,
            rows: revision.is_none(),
        };
        Rows::new(py, Box::new(keys), Box::new(versions))
    }

    /// `(key, value)` pairs (a `keyed=True` output): the version is `value(v)`.
    #[staticmethod]
    fn values(py: Python<'_>, items: Bound<'_, PyList>) -> PyResult<Rows> {
        let keys = pack(py, &items, |item| item.get_item(0))?;
        Rows::new(
            py,
            Box::new(keys),
            Box::new(PyVersions::Values(items.unbind())),
        )
    }

    /// `(key, version)` pairs, versions as given (`str` or `bytes`).
    #[staticmethod]
    fn pairs(py: Python<'_>, items: Bound<'_, PyList>) -> PyResult<Rows> {
        let keys = pack(py, &items, |item| item.get_item(0))?;
        Rows::new(
            py,
            Box::new(keys),
            Box::new(PyVersions::Pairs(items.unbind())),
        )
    }

    /// Keys, every one at `version` (a partition set's elements).
    #[staticmethod]
    fn keys(py: Python<'_>, keys: Bound<'_, PyList>, version: &[u8]) -> PyResult<Rows> {
        let packed = pack(py, &keys, |k| Ok(k.clone()))?;
        Rows::new(py, Box::new(packed), Box::new(Constant(version.to_vec())))
    }

    /// Arrow data (any object with `__arrow_c_stream__`), read in place: keys
    /// from the `key` column, versions from the `revision` column's text, or
    /// else each key's group of row digests without the `exclude`d columns
    /// (`arrow.rs`).
    #[staticmethod]
    #[pyo3(signature = (data, key, revision=None, exclude=vec![]))]
    fn arrow(
        py: Python<'_>,
        data: Bound<'_, PyAny>,
        key: &str,
        revision: Option<&str>,
        exclude: Vec<String>,
    ) -> PyResult<Rows> {
        let batches = arrow_batches(py, &data)?;
        let keys = arrow::keys(&batches, key, false).map_err(to_py)?;
        let records = Records::new(key, revision, exclude);
        let versions: Box<dyn Versions> = match revision {
            Some(r) => Box::new(arrow::Revision::new(&batches, r).map_err(to_py)?),
            None => Box::new(arrow::RowDigest::new(batches, &records.skip).map_err(to_py)?),
        };
        Rows::new(py, keys, versions)
    }

    /// Rows, not keys.
    fn __len__(&self) -> usize {
        self.len
    }

    /// Whether the keys arrived sorted, so no sort ran.
    #[getter]
    fn presorted(&self) -> bool {
        self.presorted
    }

    /// Every key and its version, in key order — for a patch, which checks
    /// its keys one by one. Uses the rows up.
    fn entries<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>)> {
        let table = self
            .table
            .take()
            .ok_or_else(|| PyValueError::new_err("rows already used"))?;
        let mut src = Source::Table(table);
        let (keys, versions) = (PyList::empty(py), PyList::empty(py));
        while src.state().map_err(to_py)? == stream::State::Ready {
            let (k, v) = src.entry();
            keys.append(PyBytes::new(py, k))?;
            versions.append(PyBytes::new(py, v))?;
            src.advance();
        }
        Ok((keys, versions))
    }
}

// -- digests ------------------------------------------------------------------------------

/// `enc(v)` of a Python value: the canonical bytes (docs/row-digest.md).
#[pyfunction]
fn encode<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let mut out = Vec::new();
    pyvalue::Walker::new(py)?.value(&value, &mut out, 0)?;
    Ok(PyBytes::new(py, &out))
}

/// `row(r)` of a row, without its `key` column.
#[pyfunction]
#[pyo3(signature = (row, key=None))]
fn row_digest<'py>(
    py: Python<'py>,
    row: Bound<'py, PyAny>,
    key: Option<&str>,
) -> PyResult<Bound<'py, PyBytes>> {
    let skip: Vec<&str> = key.into_iter().collect();
    Ok(PyBytes::new(
        py,
        &pyvalue::Walker::new(py)?.row(&row, &skip)?,
    ))
}

/// `row(r)` of every row, without its `key` column: 16 bytes each, concatenated.
#[pyfunction]
#[pyo3(signature = (rows, key=None))]
fn row_digests<'py>(
    py: Python<'py>,
    rows: Bound<'py, PyList>,
    key: Option<&str>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut w = pyvalue::Walker::new(py)?;
    let skip: Vec<&str> = key.into_iter().collect();
    let mut out = Vec::with_capacity(rows.len() * 16);
    for r in rows.iter() {
        out.extend_from_slice(&w.row(&r, &skip)?);
    }
    Ok(PyBytes::new(py, &out))
}

/// `group(rows)`: the version of a key whose rows these are.
#[pyfunction]
#[pyo3(signature = (rows, key=None))]
fn group_digest<'py>(
    py: Python<'py>,
    rows: Bound<'py, PyAny>,
    key: Option<&str>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut w = pyvalue::Walker::new(py)?;
    let skip: Vec<&str> = key.into_iter().collect();
    let mut digests: Vec<digest::Digest> = rows
        .try_iter()?
        .map(|r| w.row(&r?, &skip))
        .collect::<PyResult<_>>()?;
    Ok(PyBytes::new(py, &digest::group(&mut digests)))
}

/// `value(v)`: a `keyed=True` output's version.
#[pyfunction]
fn value_digest<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let mut out = Vec::new();
    pyvalue::Walker::new(py)?.value(&value, &mut out, 0)?;
    Ok(PyBytes::new(py, &digest::value(&out)))
}

/// A declared revision's text.
#[pyfunction]
fn revision_text<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let mut out = Vec::new();
    pyvalue::Walker::new(py)?.render(&value, &mut out)?;
    Ok(PyBytes::new(py, &out))
}

/// A chunk of a sorted stream: `(key, version)` pairs — a list, or Arrow
/// data whose first two columns they are — or with `records`, rows (a list
/// of mappings, or Arrow data) whose versions are computed here: each row's
/// digest without its key column, or the text of its revision column.
fn chunk(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    records: Option<&Records>,
) -> PyResult<(Arena, Arena)> {
    let (mut keys, mut versions) = (Arena::default(), Arena::default());
    if obj.hasattr("__arrow_c_stream__")? {
        let batches = arrow_batches(py, obj)?;
        let Some(first) = batches.first() else {
            return Ok((keys, versions));
        };
        let schema = first.schema();
        let (kn, mut v): (String, Box<dyn Versions>) = match records {
            Some(Records {
                key,
                revision: None,
                skip,
            }) => (
                key.clone(),
                Box::new(arrow::RowDigest::new(batches.clone(), skip).map_err(to_py)?),
            ),
            Some(Records {
                key,
                revision: Some(rev),
                ..
            }) => (
                key.clone(),
                Box::new(arrow::Revision::new(&batches, rev).map_err(to_py)?),
            ),
            None if schema.fields().len() >= 2 => (
                schema.field(0).name().clone(),
                Box::new(arrow::Revision::new(&batches, schema.field(1).name()).map_err(to_py)?),
            ),
            None => {
                return Err(PyValueError::new_err(
                    "a sorted chunk has key and version columns",
                ))
            }
        };
        let k = arrow::keys(&batches, &kn, records.is_none()).map_err(to_py)?;
        for i in 0..k.len() {
            keys.push(k.key(i));
        }
        let rows: Vec<u32> = (0..k.len() as u32).collect();
        v.fill(&rows, &mut versions).map_err(to_py)?;
        return Ok((keys, versions));
    }
    let mut w = pyvalue::Walker::new(py)?;
    let names = records.map(|r| r.names(py));
    for item in obj.try_iter()? {
        let item = item?;
        match records {
            None => {
                key_of(&item.get_item(0)?, &mut keys.data)?;
                key_of(&item.get_item(1)?, &mut versions.data)?;
            }
            Some(records) => records.read(
                &mut w,
                names.as_ref().unwrap(),
                &item,
                Some(&mut keys.data),
                &mut versions.data,
            )?,
        }
        keys.ends.push(keys.data.len());
        versions.ends.push(versions.data.len());
    }
    Ok((keys, versions))
}

// -- jobs -------------------------------------------------------------------------------

enum Kind {
    Replace(Box<Replace>),
    Patch(Box<Patch>),
    Compact(Box<Compact>),
    Count(Count),
}

/// A streaming job over an index's runs (see the module documentation).
/// `step()` returns `("run", r)` when run `r` needs `feed` or `end`,
/// `("rows", None)` when a streamed replacement needs `feed_rows` or
/// `end_rows`, `("file", data)` for each file written, `("garbage", data)`
/// for each garbage file a compaction writes, and `None` when done.
#[pyclass(module = "solera._native")]
struct Job {
    kind: Kind,
    records: Option<Records>,
}

impl Job {
    fn merge(&mut self) -> &mut stream::Merge {
        match &mut self.kind {
            Kind::Replace(j) => &mut j.merge,
            Kind::Patch(j) => &mut j.merge,
            Kind::Compact(j) => &mut j.merge,
            Kind::Count(j) => &mut j.merge,
        }
    }

    fn replacement(&mut self) -> PyResult<&mut Replace> {
        match &mut self.kind {
            Kind::Replace(j) => Ok(j),
            _ => Err(PyTypeError::new_err("not a replacement")),
        }
    }

    /// A replacement's or a patch's counts and collected keys.
    fn delta(&self) -> PyResult<(u64, u64, u64, &jobs::Collected)> {
        match &self.kind {
            Kind::Replace(j) => Ok((j.added, j.removed, j.changed, &j.collected)),
            Kind::Patch(j) => Ok((j.added, j.removed, j.changed, &j.collected)),
            _ => Err(PyTypeError::new_err("not a replacement or a patch")),
        }
    }
}

#[pymethods]
impl Job {
    /// The merge-join of the written content (`rows`, or with None a stream
    /// fed sorted chunks, `feed_rows`) against `runs` existing runs, newest
    /// first. A streamed chunk holds `(key, version)` pairs, or with `key`
    /// rows keyed by that column: their versions are their digests without
    /// the key and `exclude`d columns, folded into each key's group, or the
    /// `revision` a key's rows share. At most
    /// `collect` changed keys are kept for `collected`. Written entries carry
    /// `generation` as their locator.
    #[staticmethod]
    #[pyo3(signature = (rows, runs, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864, collect=0, key=None, revision=None, exclude=vec![], generation=0))]
    #[allow(clippy::too_many_arguments)]
    fn replace(
        rows: Option<PyRefMut<'_, Rows>>,
        runs: usize,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
        collect: usize,
        key: Option<String>,
        revision: Option<String>,
        exclude: Vec<String>,
        generation: u64,
    ) -> PyResult<Job> {
        let src = match rows {
            Some(mut r) => Source::Table(
                r.table
                    .take()
                    .ok_or_else(|| PyValueError::new_err("rows already used"))?,
            ),
            None => Source::Stream(Stream::new(key.is_some() && revision.is_none())),
        };
        let o = options(block_size, level, bits_per_item, k, codec);
        Ok(Job {
            kind: Kind::Replace(Box::new(Replace::new(
                src,
                runs,
                o,
                max_file_bytes,
                collect,
                generation,
            ))),
            records: key.map(|k| Records::new(&k, revision.as_deref(), exclude)),
        })
    }

    /// The merge-join of a patch — sorted `keys`, their `versions`, and a
    /// `deleted` flag per key for removes — against `runs` existing runs,
    /// newest first. At most `collect` changed keys are kept for `collected`.
    /// Written entries carry `generation` as their locator.
    #[staticmethod]
    #[pyo3(signature = (keys, versions, deleted, runs, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864, collect=0, generation=0))]
    #[allow(clippy::too_many_arguments)]
    fn patch(
        keys: Vec<PyBackedBytes>,
        versions: Vec<PyBackedBytes>,
        deleted: PyBackedBytes,
        runs: usize,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> PyResult<Job> {
        if versions.len() != keys.len() || deleted.len() != keys.len() {
            return Err(PyValueError::new_err(
                "keys, versions and deleted must have the same length",
            ));
        }
        let (mut ka, mut va) = (Arena::default(), Arena::default());
        for (i, (key, v)) in keys.iter().zip(&versions).enumerate() {
            if i > 0 && key.as_ref() <= keys[i - 1].as_ref() {
                return Err(PyValueError::new_err(
                    "patch keys must be sorted and unique",
                ));
            }
            ka.push(key);
            va.push(v);
        }
        let o = options(block_size, level, bits_per_item, k, codec);
        Ok(Job {
            records: None,
            kind: Kind::Patch(Box::new(Patch::new(
                ka,
                va,
                deleted.iter().map(|&d| d != 0).collect(),
                runs,
                o,
                max_file_bytes,
                collect,
                generation,
            ))),
        })
    }

    /// Merges `runs` (newest first) into new files; `drop_deleted` when
    /// nothing older lies below; with `garbage`, the entries it drops that
    /// name objects go to garbage files.
    #[staticmethod]
    #[pyo3(signature = (runs, *, drop_deleted, garbage=false, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn compact(
        runs: usize,
        drop_deleted: bool,
        garbage: bool,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
    ) -> Job {
        let o = options(block_size, level, bits_per_item, k, codec);
        Job {
            records: None,
            kind: Kind::Compact(Box::new(Compact::new(
                runs,
                drop_deleted,
                garbage,
                o,
                max_file_bytes,
            ))),
        }
    }

    /// Counts the live keys of `runs` (newest first).
    #[staticmethod]
    fn count(runs: usize) -> Job {
        Job {
            records: None,
            kind: Kind::Count(Count::new(runs)),
        }
    }

    /// The next consecutive blocks of run `r`: `data` holds them, `blocks`
    /// gives each one's offset in `data`, compressed size and CRC.
    fn feed(&mut self, r: usize, data: PyBackedBytes, blocks: Vec<(usize, usize, u32)>, codec: u8) {
        self.merge().runs[r].feed(Segment {
            data: Arc::new(data),
            blocks,
            codec,
        });
    }

    fn end(&mut self, r: usize) {
        self.merge().runs[r].end();
    }

    fn feed_rows(&mut self, py: Python<'_>, rows: Bound<'_, PyAny>) -> PyResult<()> {
        let (k, v) = chunk(py, &rows, self.records.as_ref())?;
        match &mut self.replacement()?.src {
            Source::Stream(s) => s.feed(k, v).map_err(to_py),
            Source::Table(_) => Err(PyTypeError::new_err("not a streamed replacement")),
        }
    }

    fn end_rows(&mut self) -> PyResult<()> {
        if let Source::Stream(s) = &mut self.replacement()?.src {
            s.end();
        }
        Ok(())
    }

    fn step<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(&'static str, Bound<'py, PyAny>)>> {
        let kind = &mut self.kind;
        let (step, file) = py
            .detach(|| {
                let step = match kind {
                    Kind::Replace(j) => j.step()?,
                    Kind::Patch(j) => j.step()?,
                    Kind::Compact(j) => j.step()?,
                    Kind::Count(j) => j.step()?,
                };
                let file = match (&step, kind) {
                    (Step::File, Kind::Replace(j)) => j.writer.files.pop_front(),
                    (Step::File, Kind::Patch(j)) => j.writer.files.pop_front(),
                    (Step::File, Kind::Compact(j)) => j.writer.files.pop_front(),
                    (Step::Garbage, Kind::Compact(j)) => {
                        j.garbage.as_mut().and_then(|g| g.files.pop_front())
                    }
                    _ => None,
                };
                Ok((step, file))
            })
            .map_err(to_py)?;
        Ok(match step {
            Step::Run(r) => Some(("run", r.into_pyobject(py)?.into_any())),
            Step::Rows => Some(("rows", py.None().into_bound(py))),
            Step::File => Some(("file", PyBytes::new(py, &file.unwrap()).into_any())),
            Step::Garbage => Some(("garbage", PyBytes::new(py, &file.unwrap()).into_any())),
            Step::Done => None,
        })
    }

    /// A replacement's or a patch's counts: new live keys, deleted keys,
    /// changed versions.
    #[getter]
    fn added(&self) -> PyResult<u64> {
        Ok(self.delta()?.0)
    }

    #[getter]
    fn removed(&self) -> PyResult<u64> {
        Ok(self.delta()?.1)
    }

    #[getter]
    fn changed(&self) -> PyResult<u64> {
        Ok(self.delta()?.2)
    }

    /// Entries a compaction wrote to garbage files.
    #[getter]
    fn garbage(&self) -> PyResult<u64> {
        match &self.kind {
            Kind::Compact(j) => Ok(j.garbage.as_ref().map_or(0, |g| g.total)),
            _ => Err(PyTypeError::new_err("not a compaction")),
        }
    }

    /// A count's live keys.
    #[getter]
    fn live(&self) -> PyResult<u64> {
        match &self.kind {
            Kind::Count(j) => Ok(j.live),
            _ => Err(PyTypeError::new_err("not a count")),
        }
    }

    /// A replacement's or a patch's written keys and deleted keys, or None
    /// past `collect`.
    fn collected<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<(Bound<'py, PyList>, Bound<'py, PyList>)>> {
        let c = self.delta()?.3;
        match (&c.upserts, &c.removes) {
            (Some(u), Some(r)) => Ok(Some((arena_list(py, u)?, arena_list(py, r)?))),
            _ => Ok(None),
        }
    }
}

// -- the engine cache's local files (docs/resolved-commits.md §5), and content digests ---------

/// A file's content digest: XXH3-128, as hex.
#[pyfunction]
fn content_digest(py: Python<'_>, data: PyBackedBytes) -> String {
    let h = py.detach(|| xxhash_rust::xxh3::xxh3_128(&data));
    format!("{h:032x}")
}

/// The local form of a `.kx` file (`source`, content digest `digest`).
#[pyfunction]
fn build_local<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
    source: String,
    digest: Vec<u8>,
) -> PyResult<Bound<'py, PyBytes>> {
    let out = py
        .detach(|| local::build(&data, &source, &digest))
        .map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// A local file, open, its directory verified and held in memory.
#[pyclass(module = "solera._native", frozen)]
struct LocalFile {
    inner: Arc<local::Local>,
}

#[pymethods]
impl LocalFile {
    #[new]
    fn open(py: Python<'_>, path: String) -> PyResult<LocalFile> {
        let inner = py
            .detach(|| local::Local::open(std::path::Path::new(&path)))
            .map_err(to_py)?;
        Ok(LocalFile {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn source(&self) -> String {
        self.inner.source.clone()
    }

    #[getter]
    fn source_size(&self) -> u64 {
        self.inner.source_size
    }

    #[getter]
    fn entries(&self) -> u64 {
        self.inner.entries
    }

    #[getter]
    fn size(&self) -> u64 {
        self.inner.size
    }

    #[getter]
    fn blocks(&self) -> usize {
        self.inner.blocks()
    }

    #[getter]
    fn digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.digest)
    }
}

type Resolved<'py> = (Vec<Bound<'py, PyBytes>>, u64, u64, u64);

/// An index as local files, newest run first, each run in key order.
#[pyclass(module = "solera._native")]
struct Snapshot {
    inner: local::Snapshot,
}

#[pymethods]
impl Snapshot {
    #[new]
    fn new(runs: Vec<Vec<PyRef<'_, LocalFile>>>) -> Snapshot {
        Snapshot {
            inner: local::Snapshot::new(
                runs.iter()
                    .map(|r| r.iter().map(|f| f.inner.clone()).collect())
                    .collect(),
            ),
        }
    }

    #[getter]
    fn entries(&self) -> u64 {
        self.inner.entries()
    }

    /// The delta of `run` (a `.kx` file) against the snapshot, as a patch or
    /// a `replace`ment, as `.kx` files with added, removed and changed.
    #[pyo3(signature = (run, *, replace, generation, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn resolve<'py>(
        &mut self,
        py: Python<'py>,
        run: PyBackedBytes,
        replace: bool,
        generation: u64,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
    ) -> PyResult<Resolved<'py>> {
        let o = options(block_size, level, bits_per_item, k, codec);
        let inner = &mut self.inner;
        let (files, counts) = py
            .detach(|| {
                let mut w = stream::Writer::new(o, max_file_bytes);
                let counts = inner.resolve(&run, replace, generation, &mut w)?;
                Ok::<_, Error>((w.files, counts))
            })
            .map_err(to_py)?;
        Ok((
            files.iter().map(|f| PyBytes::new(py, f)).collect(),
            counts.0,
            counts.1,
            counts.2,
        ))
    }

    /// The newest entry of each key — `(version, deleted, locator)` — or None.
    #[allow(clippy::type_complexity)]
    fn get<'py>(
        &mut self,
        py: Python<'py>,
        keys: Vec<PyBackedBytes>,
    ) -> PyResult<Vec<Option<(Bound<'py, PyBytes>, bool, u64)>>> {
        let inner = &mut self.inner;
        let hits = py
            .detach(|| {
                keys.iter()
                    .map(|k| inner.get(k))
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(to_py)?;
        Ok(hits
            .into_iter()
            .map(|h| h.map(|h| (PyBytes::new(py, &h.version), h.deleted, h.locator)))
            .collect())
    }
}

#[pymodule]
#[pyo3(name = "_native")]
fn solera_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CODEC_NONE", format::CODEC_NONE)?;
    m.add("CODEC_ZLIB", format::CODEC_ZLIB)?;
    m.add("FOOTER_SIZE", format::FOOTER_SIZE)?;
    m.add("FormatError", m.py().get_type::<FormatError>())?;
    m.add_function(wrap_pyfunction!(encode_file, m)?)?;
    m.add_function(wrap_pyfunction!(write_files, m)?)?;
    m.add_function(wrap_pyfunction!(decode_block, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_keys, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_tombstones, m)?)?;
    m.add_function(wrap_pyfunction!(sort_entries, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(merge_range, m)?)?;
    m.add_function(wrap_pyfunction!(filter_nbits, m)?)?;
    m.add_function(wrap_pyfunction!(parse_footer, m)?)?;
    m.add_function(wrap_pyfunction!(parse_index, m)?)?;
    m.add_function(wrap_pyfunction!(parse_tail, m)?)?;
    m.add_function(wrap_pyfunction!(check_block, m)?)?;
    m.add_function(wrap_pyfunction!(decode_garbage, m)?)?;
    m.add_function(wrap_pyfunction!(build_local, m)?)?;
    m.add_function(wrap_pyfunction!(content_digest, m)?)?;
    m.add_class::<LocalFile>()?;
    m.add_class::<Snapshot>()?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_function(wrap_pyfunction!(row_digest, m)?)?;
    m.add_function(wrap_pyfunction!(row_digests, m)?)?;
    m.add_function(wrap_pyfunction!(group_digest, m)?)?;
    m.add_function(wrap_pyfunction!(value_digest, m)?)?;
    m.add_function(wrap_pyfunction!(revision_text, m)?)?;
    m.add("DIGEST_VERSION", digest::VERSION)?;
    m.add_class::<Rows>()?;
    m.add_class::<Job>()?;
    Ok(())
}
