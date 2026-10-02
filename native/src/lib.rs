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
pub mod delta;
pub mod digest;
pub mod format;
pub mod garbage;
pub mod jobs;
pub mod local;
mod pyvalue;
pub mod rows;
pub mod run;
pub mod sort;
pub mod sparse;
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
use jobs::{Compact, Count, Join, Step};
use pyo3::types::PyTuple;
use rayon::prelude::*;
use rows::{Arena, Constant, Cursor, Overlay, Source, Stream, Table, Versions};
use stream::Segment;

create_exception!(
    _native,
    FormatError,
    PyValueError,
    "A key index file is malformed or fails a checksum."
);
create_exception!(
    _native,
    LocalError,
    FormatError,
    "An engine cache's local copy failed a check; `path` is its source's path."
);
create_exception!(
    _native,
    LimitError,
    PyValueError,
    "Well-formed input over a limit: more entries or bytes than the caller takes."
);

fn to_py(e: Error) -> PyErr {
    match e {
        Error::Format(m) => FormatError::new_err(m),
        Error::Value(m) => PyValueError::new_err(m),
        Error::Limit(m) => LimitError::new_err(m),
        Error::Local(path, what) => Python::attach(|py| {
            let e = LocalError::new_err(format!("local file {path}: {what}"));
            match e.value(py).setattr("path", path) {
                Ok(()) => e,
                Err(set) => set,
            }
        }),
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
    let out = py
        .detach(|| {
            let predecessors: Vec<format::Predecessor> = predecessors.iter().map(prev).collect();
            format::encode_file(
                &slices(&keys),
                &slices(&versions),
                &deleted,
                &locators,
                &predecessors,
                o,
            )
        })
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

/// The merged view's keys, versions, deleted flags and locators: `runs` are
/// each a file's consecutive blocks, `codecs` each run's file's codec.
#[pyfunction]
fn merge_range<'py>(
    py: Python<'py>,
    runs: Vec<Vec<PyBackedBytes>>,
    codecs: Vec<u8>,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    drop_deleted: bool,
) -> PyResult<Merged<'py>> {
    let runs: Vec<Vec<&[u8]>> = runs.iter().map(|r| slices(r)).collect();
    let (k, v, f, l) = format::merge_range(
        &runs,
        &codecs,
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
    fn fill(&self, rows: &[u32], out: &mut Arena) -> format::Result<()> {
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
    fn fill(&self, rows: &[u32], out: &mut Arena) -> format::Result<()> {
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

/// A pass over `Rows`, a page of keys and versions at a time (`Rows.pages`).
#[pyclass(module = "solera._native")]
struct Pages {
    cursor: Cursor,
    size: usize,
}

#[pymethods]
impl Pages {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[allow(clippy::type_complexity)]
    fn __next__<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(Bound<'py, PyList>, Bound<'py, PyList>)>> {
        let (keys, versions) = (PyList::empty(py), PyList::empty(py));
        while keys.len() < self.size && self.cursor.read().map_err(to_py)? {
            let (k, v) = self.cursor.entry();
            keys.append(PyBytes::new(py, k))?;
            versions.append(PyBytes::new(py, v))?;
        }
        Ok((!keys.is_empty()).then_some((keys, versions)))
    }
}

/// A keyed write's content, sorted by key (unless it arrived sorted) and
/// read a key at a time: every key is the group of rows that carry it, and
/// its version is computed as the reader reaches it (docs/row-digest.md).
#[pyclass(module = "solera._native", frozen)]
struct Rows {
    table: Arc<Table>,
}

impl Rows {
    fn new(
        py: Python<'_>,
        keys: Box<dyn sort::Keys + Send>,
        versions: Box<dyn Versions>,
    ) -> PyResult<Rows> {
        let table = py.detach(|| Table::new(keys, versions)).map_err(to_py)?;
        Ok(Rows {
            table: Arc::new(table),
        })
    }
}

/// Keys, packed.
fn packed_keys(keys: &[Bound<'_, PyAny>]) -> PyResult<Arena> {
    let mut out = Arena::default();
    for k in keys {
        row_key(k, &mut out.data)?;
        out.ends.push(out.data.len());
    }
    Ok(out)
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

    /// Rows read a column at a time — a DataFrame through pandas alone, no
    /// Arrow: `columns[i]` holds column `names[i]`'s value of every row, None
    /// where it is missing. A row digests as the mapping of its values not
    /// null would (`records`).
    #[staticmethod]
    #[pyo3(signature = (names, columns, key, revision=None, exclude=vec![]))]
    #[allow(clippy::too_many_arguments)]
    fn columns(
        py: Python<'_>,
        names: Vec<String>,
        columns: Vec<Bound<'_, PyList>>,
        key: &str,
        revision: Option<&str>,
        exclude: Vec<String>,
    ) -> PyResult<Rows> {
        if names.len() != columns.len() {
            return Err(PyValueError::new_err("a name for every column"));
        }
        let mut sorted: Vec<&String> = names.iter().collect();
        sorted.sort();
        if let Some(w) = sorted.windows(2).find(|w| w[0] == w[1]) {
            return Err(PyValueError::new_err(format!(
                "column {:?} appears twice",
                w[0]
            )));
        }
        let n = columns.first().map_or(0, |c| c.len());
        if columns.iter().any(|c| c.len() != n) {
            return Err(PyValueError::new_err("columns of different lengths"));
        }
        let at = |name: &str| names.iter().position(|c| c == name);
        let k = at(key).ok_or_else(|| PyKeyError::new_err(key.to_string()))?;
        let (mut keys, mut versions) = (Arena::default(), Arena::default());
        keys.ends.reserve(n);
        versions.ends.reserve(n);
        let mut w = pyvalue::Walker::new(py)?;
        let records = Records::new(key, revision, exclude);
        let mut order: Vec<usize> = (0..names.len())
            .filter(|&c| !records.skip.contains(&names[c]))
            .collect();
        order.sort_by(|&a, &b| names[a].as_bytes().cmp(names[b].as_bytes()));
        let cells: Vec<(Vec<u8>, Bound<'_, PyList>)> = order
            .iter()
            .map(|&c| {
                let mut head = Vec::new();
                digest::put_len(&mut head, names[c].as_bytes());
                (head, columns[c].clone())
            })
            .collect();
        let rev = match revision {
            Some(r) => Some(at(r).ok_or_else(|| {
                PyValueError::new_err(format!("a row lacks the declared revision field {r:?}"))
            })?),
            None => None,
        };
        for i in 0..n {
            row_key(&columns[k].get_item(i)?, &mut keys.data)?;
            keys.ends.push(keys.data.len());
            match rev {
                Some(r) => {
                    let v = columns[r].get_item(i)?;
                    if v.is_none() {
                        return Err(PyValueError::new_err(format!(
                            "a row lacks the declared revision field {:?}",
                            names[r]
                        )));
                    }
                    w.render(&v, &mut versions.data)?;
                }
                None => versions.data.extend_from_slice(&w.column_row(&cells, i)?),
            }
            versions.ends.push(versions.data.len());
            if i % 65536 == 65535 {
                py.detach(|| ()); // let other threads run: this loop holds the GIL
            }
        }
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
        self.table.len()
    }

    /// Whether the keys arrived sorted, so no sort ran.
    #[getter]
    fn presorted(&self) -> bool {
        self.table.presorted()
    }

    /// Every key and its version, in key order. Rows are read, never used
    /// up: every pass computes the versions it reaches.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>)> {
        let mut c = Cursor::new(self.table.clone());
        let (keys, versions) = (PyList::empty(py), PyList::empty(py));
        while c.read().map_err(to_py)? {
            let (k, v) = c.entry();
            keys.append(PyBytes::new(py, k))?;
            versions.append(PyBytes::new(py, v))?;
        }
        Ok((keys, versions))
    }

    /// Every key and its version, in key order, `size` at a time: `(keys,
    /// versions)` per page, computed as the pass reaches them.
    fn pages(&self, size: usize) -> PyResult<Pages> {
        if size == 0 {
            return Err(PyValueError::new_err("a page holds at least one key"));
        }
        Ok(Pages {
            cursor: Cursor::new(self.table.clone()),
            size,
        })
    }

    /// The digest of every `(key, version)`, in key order: the content's
    /// identity. Free once a pass has read every key, as a replacement does.
    fn digest<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let table = self.table.clone();
        let d = py.detach(|| table.content()).map_err(to_py)?;
        Ok(PyBytes::new(py, &d))
    }

    /// The rows of each of `keys`, as indices into what the rows were made
    /// from, each key's in their order there: `(rows, ends)`, key `i`'s rows
    /// at `rows[ends[i - 1]:ends[i]]`. A key the write does not hold is a
    /// `KeyError`.
    fn find<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>)> {
        let packed = packed_keys(&keys)?;
        let table = self.table.clone();
        let found: Vec<Option<Vec<u32>>> = py.detach(|| {
            (0..packed.len())
                .into_par_iter()
                .map(|i| table.find(packed.get(i)))
                .collect()
        });
        let (rows, ends) = (PyList::empty(py), PyList::empty(py));
        let mut n = 0usize;
        for (i, f) in found.into_iter().enumerate() {
            let Some(f) = f else {
                return Err(PyKeyError::new_err(keys[i].clone().unbind()));
            };
            n += f.len();
            for r in f {
                rows.append(r)?;
            }
            ends.append(n)?;
        }
        Ok((rows, ends))
    }

    /// The version of each of `keys`, from its rows alone; a key the write
    /// does not hold is a `KeyError`.
    fn versions<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let packed = packed_keys(&keys)?;
        let table = self.table.clone();
        let found: Vec<format::Result<Option<Vec<u8>>>> = py.detach(|| {
            (0..packed.len())
                .into_par_iter()
                .map(|i| table.version(packed.get(i)))
                .collect()
        });
        let out = PyList::empty(py);
        for (i, f) in found.into_iter().enumerate() {
            match f.map_err(to_py)? {
                Some(v) => out.append(PyBytes::new(py, &v))?,
                None => return Err(PyKeyError::new_err(keys[i].clone().unbind())),
            }
        }
        Ok(out)
    }

    /// Whether the write holds `key`.
    fn __contains__(&self, key: Bound<'_, PyAny>) -> PyResult<bool> {
        let mut k = Vec::new();
        row_key(&key, &mut k)?;
        Ok(self.table.find(&k).is_some())
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
        let (kn, v): (String, Box<dyn Versions>) = match records {
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

// -- sorted runs ------------------------------------------------------------------------

/// A write's entries in key order (`run.rs`): upserts at their versions, and
/// removes. Immutable once built, so readers share it.
#[pyclass(module = "solera._native", frozen)]
struct SortedRun {
    inner: Arc<run::SortedRun>,
}

fn sorted_run(r: format::Result<run::SortedRun>) -> PyResult<SortedRun> {
    Ok(SortedRun {
        inner: Arc::new(r.map_err(to_py)?),
    })
}

#[pymethods]
impl SortedRun {
    /// Upserts of `keys` at `versions` (any order, each key once), and the
    /// removes of `removes`; a key both written and removed is an error.
    #[staticmethod]
    #[pyo3(signature = (keys, versions, removes=vec![]))]
    fn of(
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        versions: Vec<PyBackedBytes>,
        removes: Vec<PyBackedBytes>,
    ) -> PyResult<SortedRun> {
        sorted_run(
            py.detach(|| run::SortedRun::of(&slices(&keys), &slices(&versions), &slices(&removes))),
        )
    }

    /// Every key of `rows` at its version, in key order, and the removes of
    /// `removes`. Reads the rows, which stay usable.
    #[staticmethod]
    #[pyo3(signature = (rows, removes=vec![]))]
    fn from_rows(
        py: Python<'_>,
        rows: PyRef<'_, Rows>,
        removes: Vec<PyBackedBytes>,
    ) -> PyResult<SortedRun> {
        let mut src = Source::Table(Box::new(Cursor::new(rows.table.clone())));
        sorted_run(py.detach(|| run::SortedRun::from_source(&mut src, &slices(&removes))))
    }

    /// A run from its transport form, a `.kx` file, every fact checked
    /// (`FormatError` when one fails), decoding at most `max_entries`
    /// entries and `max_bytes` bytes (`LimitError` past either).
    #[staticmethod]
    #[pyo3(signature = (data, *, max_entries=u64::MAX, max_bytes=u64::MAX))]
    fn decode(
        py: Python<'_>,
        data: PyBackedBytes,
        max_entries: u64,
        max_bytes: u64,
    ) -> PyResult<SortedRun> {
        sorted_run(py.detach(|| run::SortedRun::decode(&data, max_entries, max_bytes)))
    }

    /// The transport form: one `.kx` file.
    #[pyo3(signature = (*, block_size=65536, level=1, bits_per_item=14, k=10, codec=1))]
    fn encode<'py>(
        &self,
        py: Python<'py>,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let o = options(block_size, level, bits_per_item, k, codec);
        let out = py.detach(|| self.inner.encode(o)).map_err(to_py)?;
        Ok(PyBytes::new(py, &out))
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn upserts(&self) -> usize {
        self.inner.len() - self.inner.removes()
    }

    #[getter]
    fn removes(&self) -> usize {
        self.inner.removes()
    }

    /// Bytes held.
    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        arena_list(py, &self.inner.keys)
    }

    /// Keys, versions (empty for a remove), deleted flags, locators.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Merged<'py>> {
        let r = &self.inner;
        let flags: Vec<u8> = r.deleted.iter().map(|&d| d as u8).collect();
        Ok((
            arena_list(py, &r.keys)?,
            arena_list(py, &r.versions)?,
            PyBytes::new(py, &flags),
            r.locators.clone(),
        ))
    }

    /// Up to `limit` entries of the newest-wins merge of `runs` (newest
    /// first) past `after`: keys, versions, deleted flags, locators, and
    /// whether any key lies past them.
    #[staticmethod]
    #[pyo3(signature = (runs, after, limit))]
    fn merge<'py>(
        py: Python<'py>,
        runs: Vec<PyRef<'py, SortedRun>>,
        after: Option<PyBackedBytes>,
        limit: usize,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let inner: Vec<&run::SortedRun> = runs.iter().map(|r| r.inner.as_ref()).collect();
        let (picks, more) = run::SortedRun::merge(&inner, after.as_deref(), limit);
        let keys = PyList::new(
            py,
            picks
                .iter()
                .map(|&(r, i)| PyBytes::new(py, inner[r].key(i))),
        )?;
        let versions = PyList::new(
            py,
            picks
                .iter()
                .map(|&(r, i)| PyBytes::new(py, inner[r].versions.get(i))),
        )?;
        let deleted: Vec<u8> = picks
            .iter()
            .map(|&(r, i)| inner[r].deleted[i] as u8)
            .collect();
        let locators: Vec<u64> = picks.iter().map(|&(r, i)| inner[r].locators[i]).collect();
        (keys, versions, PyBytes::new(py, &deleted), locators, more).into_pyobject(py)
    }
}

/// The sparse reader's state over a sorted run (`sparse.rs`): Python
/// fetches what it asks for, entries are named by their position.
#[pyclass(module = "solera._native")]
struct Sparse {
    inner: sparse::Sparse,
}

fn filter_of(f: &(u64, u8, PyBackedBytes)) -> sparse::Filter<'_> {
    (f.0, f.1, f.2.as_ref())
}

#[pymethods]
impl Sparse {
    #[new]
    fn new(run: PyRef<'_, SortedRun>) -> Sparse {
        Sparse {
            inner: sparse::Sparse::new(run.inner.clone()),
        }
    }

    /// Entries still undecided.
    #[getter]
    fn unknown(&self) -> usize {
        self.inner.unknown()
    }

    /// Entries an exact read must decide.
    #[getter]
    fn maybe(&self) -> usize {
        self.inner.maybe()
    }

    /// Whether a count change was inferred from the filters.
    #[getter]
    fn inferred(&self) -> bool {
        self.inner.inferred
    }

    /// The positions `[lo, hi)` of the run's keys in `[min, max]`.
    fn span(&self, min: &[u8], max: &[u8]) -> (usize, usize) {
        self.inner.span(min, max)
    }

    /// The blocks a read of one file needs, by its blocks' first keys: with
    /// `file`, for the entries its filters left to it; else the undecided
    /// ones in `[lo, hi)`.
    #[pyo3(signature = (firsts, *, file=None, lo=0, hi=0))]
    fn blocks(
        &self,
        firsts: Vec<PyBackedBytes>,
        file: Option<usize>,
        lo: usize,
        hi: usize,
    ) -> Vec<usize> {
        self.inner.blocks(&slices(&firsts), file, lo, hi)
    }

    /// Reads those entries in one file's fetched `blocks`, `(index, bytes)`.
    #[pyo3(signature = (blocks, codec, firsts, *, file=None, lo=0, hi=0))]
    #[allow(clippy::too_many_arguments)]
    fn read(
        &mut self,
        py: Python<'_>,
        blocks: Vec<(usize, PyBackedBytes)>,
        codec: u8,
        firsts: Vec<PyBackedBytes>,
        file: Option<usize>,
        lo: usize,
        hi: usize,
    ) -> PyResult<()> {
        let inner = &mut self.inner;
        py.detach(|| {
            let blocks: Vec<(usize, &[u8])> =
                blocks.iter().map(|(i, b)| (*i, b.as_ref())).collect();
            inner.read(&blocks, codec, &slices(&firsts), file, lo, hi)
        })
        .map_err(to_py)
    }

    /// Runs one file's filters — `(nbits, k, bits)` each, as its tail holds
    /// them — over the undecided entries in `[lo, hi)`.
    fn filter(
        &mut self,
        file: usize,
        lo: usize,
        hi: usize,
        keys: (u64, u8, PyBackedBytes),
        tombs: (u64, u8, PyBackedBytes),
        pairs: (u64, u8, PyBackedBytes),
    ) {
        self.inner.filter(
            file,
            lo,
            hi,
            filter_of(&keys),
            filter_of(&tombs),
            filter_of(&pairs),
        );
    }

    /// Decides what the filters can (`exact`: no change from filters alone).
    fn classify(&mut self, exact: bool) {
        self.inner.classify(exact);
    }

    /// Each live entry read: `{key: (version, locator)}`.
    fn live<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        for (p, v, l) in self.inner.live() {
            out.set_item(
                PyBytes::new(py, self.inner.run.key(p)),
                (PyBytes::new(py, v), l),
            )?;
        }
        Ok(out)
    }

    /// The run's delta over what was read: `.kx` files with added, removed
    /// and changed, and up to `collect` changed keys (`Job.collected`).
    #[pyo3(signature = (*, generation, collect=0, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn delta<'py>(
        &self,
        py: Python<'py>,
        generation: u64,
        collect: usize,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
    ) -> PyResult<(Resolved<'py>, Option<Changed<'py>>)> {
        let o = options(block_size, level, bits_per_item, k, codec);
        let inner = &self.inner;
        let d = py
            .detach(|| inner.delta(o, max_file_bytes, collect, generation))
            .map_err(to_py)?;
        Ok((delta_files(py, &d), changed(py, &d.collected)?))
    }
}

// -- jobs -------------------------------------------------------------------------------

enum Kind {
    Join(Box<Join>),
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
    /// The index's runs as local files (`local`): fed from them, not by the caller.
    local: Option<(local::Snapshot, local::Feed)>,
}

fn merge_of(kind: &mut Kind) -> &mut stream::Merge {
    match kind {
        Kind::Join(j) => &mut j.merge,
        Kind::Compact(j) => &mut j.merge,
        Kind::Count(j) => &mut j.merge,
    }
}

impl Job {
    fn merge(&mut self) -> &mut stream::Merge {
        merge_of(&mut self.kind)
    }

    fn join(&mut self) -> PyResult<&mut Join> {
        match &mut self.kind {
            Kind::Join(j) => Ok(j),
            _ => Err(PyTypeError::new_err("not a replacement or a patch")),
        }
    }

    /// A replacement's or a patch's delta.
    fn delta(&self) -> PyResult<&delta::Delta> {
        match &self.kind {
            Kind::Join(j) => Ok(&j.delta),
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
    /// `generation` as their locator. A streamed replacement may take an
    /// `overlay`: a run whose upserts stand in for the stream's entries of
    /// their keys, and whose removes drop them.
    #[staticmethod]
    #[pyo3(signature = (rows, runs, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864, collect=0, key=None, revision=None, exclude=vec![], generation=0, overlay=None))]
    #[allow(clippy::too_many_arguments)]
    fn replace(
        rows: Option<PyRef<'_, Rows>>,
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
        overlay: Option<PyRef<'_, SortedRun>>,
    ) -> PyResult<Job> {
        let src = match (rows, overlay) {
            (Some(r), None) => Source::Table(Box::new(Cursor::new(r.table.clone()))),
            (None, None) => Source::Stream(Stream::new(key.is_some() && revision.is_none())),
            (None, Some(run)) => Source::Overlay(Box::new(Overlay::new(
                Stream::new(key.is_some() && revision.is_none()),
                run.inner.clone(),
            ))),
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "an overlay goes over a streamed replacement",
                ))
            }
        };
        let o = options(block_size, level, bits_per_item, k, codec);
        Ok(Job {
            kind: Kind::Join(Box::new(
                Join::new(src, true, runs, o, max_file_bytes, collect, generation)
                    .map_err(to_py)?,
            )),
            records: key.map(|k| Records::new(&k, revision.as_deref(), exclude)),
            local: None,
        })
    }

    /// The merge-join of a sorted run against `runs` existing runs, newest
    /// first: a patch, or with `replace` the whole new content. At most
    /// `collect` changed keys are kept for `collected`. Written entries carry
    /// `generation` as their locator.
    #[staticmethod]
    #[pyo3(signature = (run, runs, *, replace=false, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864, collect=0, generation=0))]
    #[allow(clippy::too_many_arguments)]
    fn patch(
        run: PyRef<'_, SortedRun>,
        runs: usize,
        replace: bool,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> PyResult<Job> {
        let o = options(block_size, level, bits_per_item, k, codec);
        let src = Source::Run(run.inner.clone(), 0);
        let job =
            Join::new(src, replace, runs, o, max_file_bytes, collect, generation).map_err(to_py)?;
        Ok(Job {
            records: None,
            local: None,
            kind: Kind::Join(Box::new(job)),
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
            local: None,
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
            local: None,
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

    /// Reads the existing runs from local files (`LocalFile`s, newest run
    /// first, each in key order) instead of asking for segments: `step`
    /// then never returns `("run", r)`.
    fn local(&mut self, runs: Vec<Vec<PyRef<'_, LocalFile>>>) -> PyResult<()> {
        if runs.len() != self.merge().runs.len() {
            return Err(PyValueError::new_err("a local run per run"));
        }
        let snap = local::Snapshot::new(
            runs.iter()
                .map(|r| r.iter().map(|f| f.inner.clone()).collect())
                .collect(),
        );
        self.local = Some((snap, local::Feed::new(runs.len())));
        Ok(())
    }

    fn feed_rows(&mut self, py: Python<'_>, rows: Bound<'_, PyAny>) -> PyResult<()> {
        let (k, v) = chunk(py, &rows, self.records.as_ref())?;
        match self.join()?.src.stream() {
            Some(s) => s.feed(k, v).map_err(to_py),
            None => Err(PyTypeError::new_err("not a streamed replacement")),
        }
    }

    fn end_rows(&mut self) -> PyResult<()> {
        if let Some(s) = self.join()?.src.stream() {
            s.end();
        }
        Ok(())
    }

    fn step<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(&'static str, Bound<'py, PyAny>)>> {
        let (kind, local) = (&mut self.kind, &mut self.local);
        let (step, file) = py
            .detach(|| {
                let step = loop {
                    let step = match kind {
                        Kind::Join(j) => j.step()?,
                        Kind::Compact(j) => j.step()?,
                        Kind::Count(j) => j.step()?,
                    };
                    match (step, local.as_mut()) {
                        (Step::Run(r), Some((snap, feed))) => feed.feed(snap, merge_of(kind), r)?,
                        (step, _) => break step,
                    }
                };
                let file = match (&step, kind) {
                    (Step::File, Kind::Join(j)) => j.delta.writer.files.pop_front(),
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
        Ok(self.delta()?.added)
    }

    #[getter]
    fn removed(&self) -> PyResult<u64> {
        Ok(self.delta()?.removed)
    }

    #[getter]
    fn changed(&self) -> PyResult<u64> {
        Ok(self.delta()?.changed)
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

    /// A replacement's or a patch's written keys, `{key: version}`, and its
    /// deleted keys, or None past `collect`.
    fn collected<'py>(&self, py: Python<'py>) -> PyResult<Option<Changed<'py>>> {
        changed(py, &self.delta()?.collected)
    }
}

// -- the engine cache's local files (docs/resolved-commits.md §5), and content digests ---------

/// A file's content digest: XXH3-128, as hex.
#[pyfunction]
fn content_digest(py: Python<'_>, data: PyBackedBytes) -> String {
    let h = py.detach(|| xxhash_rust::xxh3::xxh3_128(&data));
    format!("{h:032x}")
}

/// Writes the local form of a `.kx` file (`source`, content digest
/// `digest`) to `path`, as it is built: at most `max_bytes` (`LimitError`
/// past them, the file partial). Returns its size.
#[pyfunction]
fn build_local(
    py: Python<'_>,
    data: PyBackedBytes,
    source: String,
    digest: Vec<u8>,
    path: String,
    max_bytes: u64,
) -> PyResult<u64> {
    py.detach(|| {
        local::build(
            &data,
            &source,
            &digest,
            std::path::Path::new(&path),
            max_bytes,
        )
    })
    .map_err(to_py)
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

/// Written keys with their versions, and deleted keys.
type Changed<'py> = (Bound<'py, PyDict>, Bound<'py, PyList>);

fn changed<'py>(py: Python<'py>, c: &delta::Collected) -> PyResult<Option<Changed<'py>>> {
    match (&c.upserts, &c.removes) {
        (Some((k, v)), Some(r)) => {
            let written = PyDict::new(py);
            for i in 0..k.len() {
                written.set_item(PyBytes::new(py, k.get(i)), PyBytes::new(py, v.get(i)))?;
            }
            Ok(Some((written, arena_list(py, r)?)))
        }
        _ => Ok(None),
    }
}

fn delta_files<'py>(py: Python<'py>, d: &delta::Delta) -> Resolved<'py> {
    (
        d.writer.files.iter().map(|f| PyBytes::new(py, f)).collect(),
        d.added,
        d.removed,
        d.changed,
    )
}

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

    /// The delta of a `SortedRun` against the snapshot, as a patch or a
    /// `replace`ment: `.kx` files, with added, removed and changed.
    #[pyo3(signature = (run, *, replace, generation, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn resolve<'py>(
        &mut self,
        py: Python<'py>,
        run: PyRef<'_, SortedRun>,
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
        let (inner, run) = (&mut self.inner, run.inner.clone());
        let d = py
            .detach(|| inner.resolve(&run, replace, generation, o, max_file_bytes))
            .map_err(to_py)?;
        Ok(delta_files(py, &d))
    }

    /// Up to `limit` entries of the merged snapshot past `after` — deletions
    /// dropped with `drop_deleted` — as `KeyIndex`'s scans return them: keys,
    /// versions, deleted flags, locators, and the cursor (None at the end).
    #[pyo3(signature = (after, limit, *, drop_deleted))]
    #[allow(clippy::type_complexity)]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        after: Option<PyBackedBytes>,
        limit: usize,
        drop_deleted: bool,
    ) -> PyResult<(
        Bound<'py, PyList>,
        Bound<'py, PyList>,
        Bound<'py, PyBytes>,
        Vec<u64>,
        Option<Bound<'py, PyBytes>>,
    )> {
        let inner = &self.inner;
        let s = py
            .detach(|| inner.scan(after.as_deref(), limit, drop_deleted))
            .map_err(to_py)?;
        Ok((
            list_of_bytes(py, &s.keys)?,
            list_of_bytes(py, &s.versions)?,
            PyBytes::new(py, &s.deleted),
            s.locators,
            s.next.map(|n| PyBytes::new(py, &n)),
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
    m.add("LimitError", m.py().get_type::<LimitError>())?;
    m.add("LocalError", m.py().get_type::<LocalError>())?;
    m.add_class::<SortedRun>()?;
    m.add_class::<Sparse>()?;
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
    m.add_class::<Pages>()?;
    m.add_class::<Job>()?;
    Ok(())
}
